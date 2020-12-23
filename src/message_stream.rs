use crate::channel::ChannelOptions;
use crate::message::{
    AssignIncomingMessageHandler, MessageId, OutgoingMessage, OutgoingMessagePayload,
};
use crate::metrics::ChannelMetrics;
use crate::packet::{PacketHeaderDecoder, PacketizedMessage, MIN_PACKET_LEN};
use crate::{ErrorKind, Result};
use bytecodec::bytes::{BytesEncoder, RemainingBytesDecoder};
use bytecodec::combinator::{MaybeEos, Peekable, Slice};
use bytecodec::io::{IoDecodeExt, IoEncodeExt, ReadBuf, WriteBuf};
use bytecodec::{Decode, DecodeExt, Encode, EncodeExt, Eos};
use fibers::sync::mpsc;
use fibers::time::timer::{self, Timeout};
use fibers_tasque::DefaultCpuTaskQueue;
use futures::{Async, Future, Stream};
use futures03::Stream as Stream03;
use std::cmp;
use std::collections::{BinaryHeap, HashMap};
use std::fmt;
use std::pin::Pin;
use std::task::{Context, Poll as Poll03};
use tokio::io::{AsyncRead as AsyncRead03, AsyncWrite as AsyncWrite03};
use tokio::net::TcpStream;

pub struct MessageStream<A: AssignIncomingMessageHandler> {
    transport_stream: TcpStream,
    rbuf: ReadBuf<Vec<u8>>,
    wbuf: WriteBuf<Vec<u8>>,
    assigner: A,
    packet_header_decoder: Peekable<MaybeEos<PacketHeaderDecoder>>,
    receiving_messages: HashMap<MessageId, Slice<A::Handler>>,
    sending_messages: BinaryHeap<SendingMessage>,
    async_outgoing_tx: mpsc::Sender<Result<OutgoingMessage>>,
    async_outgoing_rx: mpsc::Receiver<Result<OutgoingMessage>>,
    async_incoming_tx: mpsc::Sender<Result<<A::Handler as Decode>::Item>>,
    async_incoming_rx: mpsc::Receiver<Result<<A::Handler as Decode>::Item>>,
    async_incomings: HashMap<MessageId, Slice<RemainingBytesDecoder>>,
    seqno: u64,
    options: ChannelOptions,
    metrics: ChannelMetrics,
    write_timeout: Option<Timeout>,
    is_written: bool,
}
impl<A: AssignIncomingMessageHandler> MessageStream<A>
where
    <A::Handler as Decode>::Item: Send + 'static,
{
    pub fn new(
        transport_stream: TcpStream,
        assigner: A,
        options: ChannelOptions,
        metrics: ChannelMetrics,
    ) -> Self {
        let _ = transport_stream.set_nodelay(true);
        let (async_outgoing_tx, async_outgoing_rx) = mpsc::channel();
        let (async_incoming_tx, async_incoming_rx) = mpsc::channel();
        MessageStream {
            transport_stream,
            rbuf: ReadBuf::new(vec![0; options.read_buffer_size]),
            wbuf: WriteBuf::new(vec![0; options.write_buffer_size]),
            sending_messages: BinaryHeap::new(),
            async_outgoing_tx,
            async_outgoing_rx,
            async_incoming_tx,
            async_incoming_rx,
            async_incomings: HashMap::new(),
            assigner,
            packet_header_decoder: PacketHeaderDecoder::default().maybe_eos().peekable(),
            receiving_messages: HashMap::new(),
            seqno: 0,
            options,
            metrics,
            write_timeout: None,
            is_written: false,
        }
    }

    pub fn options(&self) -> &ChannelOptions {
        &self.options
    }

    pub fn metrics(&self) -> &ChannelMetrics {
        &self.metrics
    }

    pub fn send_message(&mut self, mut message: OutgoingMessage) {
        if message.header.is_async {
            self.metrics.async_outgoing_messages.increment();
            let tx = self.async_outgoing_tx.clone();
            DefaultCpuTaskQueue.with(|tasque| {
                tasque.enqueue(move || {
                    let f = || {
                        let header = message.header;
                        let mut payload = Vec::new();
                        track!(message.payload.encode_all(&mut payload))?;
                        let payload =
                            OutgoingMessagePayload::new(BytesEncoder::new().last(payload));
                        Ok(OutgoingMessage { header, payload })
                    };
                    let _ = tx.send(f());
                })
            })
        } else {
            self.start_sending_message(message);
        }
    }

    pub fn assigner_mut(&mut self) -> &mut A {
        &mut self.assigner
    }

    fn start_sending_message(&mut self, message: OutgoingMessage) {
        let message = SendingMessage {
            seqno: self.seqno,
            message: PacketizedMessage::new(message),
        };
        self.seqno += 1;
        self.sending_messages.push(message);
        self.metrics.enqueued_outgoing_messages.increment();
    }

    fn handle_outgoing_messages(
        &mut self,
        mut ctx: &mut Context<'_>,
    ) -> Result<Option<MessageEvent<<A::Handler as Decode>::Item>>> {
        while !(self.sending_messages.is_empty() && self.wbuf.is_empty()) {
            if self.wbuf.room() >= MIN_PACKET_LEN {
                if let Some(mut sending) = self.sending_messages.pop() {
                    track!(sending.message.encode_to_write_buf(&mut self.wbuf))?;
                    if sending.message.is_idle() {
                        // Completed to write the message to the sending buffer
                        let event = MessageEvent::Sent;
                        self.metrics.dequeued_outgoing_messages.increment();
                        return Ok(Some(event));
                    } else {
                        // A part of the message was written to the sending buffer
                        sending.seqno = self.seqno;
                        self.seqno += 1;
                        self.sending_messages.push(sending);
                    }
                    continue;
                }
            }

            let old_len = self.wbuf.len();
            track!(self.wbuf.flush(WriteWrapper {
                inner: &mut self.transport_stream,
                ctx: &mut ctx,
            }))?;
            if self.wbuf.len() < old_len {
                self.is_written = true;
            }
            if !self.wbuf.stream_state().is_normal() {
                break;
            }
        }
        Ok(None)
    }

    #[allow(clippy::map_entry)]
    fn handle_incoming_messages(
        &mut self,
        mut ctx: &mut Context<'_>,
    ) -> Result<Option<MessageEvent<<A::Handler as Decode>::Item>>> {
        loop {
            eprintln!("handle_incoming_messages loop start");
            track!(self.rbuf.fill(ReadWrapper {
                inner: &mut self.transport_stream,
                ctx: &mut ctx,
            }))?;
            eprintln!("handle_incoming_messages loop mid 1");

            if !self.packet_header_decoder.is_idle() {
                eprintln!("handle_incoming_messages not idle");
                track!(self
                    .packet_header_decoder
                    .decode_from_read_buf(&mut self.rbuf))?;
                if let Some(header) = self.packet_header_decoder.peek().cloned() {
                    if header.is_async() {
                        let decoder = self
                            .async_incomings
                            .entry(header.message.id)
                            .or_insert_with(Default::default);
                        decoder.set_consumable_bytes(u64::from(header.payload_len));
                    } else {
                        if !self.receiving_messages.contains_key(&header.message.id) {
                            let handler = track!(self
                                .assigner
                                .assign_incoming_message_handler(&header.message))?;
                            self.receiving_messages
                                .insert(header.message.id, handler.slice());
                        }
                        let handler = self
                            .receiving_messages
                            .get_mut(&header.message.id)
                            .expect("Never fails");
                        handler.set_consumable_bytes(u64::from(header.payload_len));
                    }
                }
            }

            eprintln!("handle_incoming_messages loop mid 2");
            if let Some(header) = self.packet_header_decoder.peek().cloned() {
                if header.is_async() {
                    let mut decoder = self
                        .async_incomings
                        .remove(&header.message.id)
                        .expect("Never fails");

                    track!(decoder.decode_from_read_buf(&mut self.rbuf))?;
                    track_assert!(!decoder.is_idle(), ErrorKind::Other);

                    if decoder.is_suspended() {
                        let _ = track!(self.packet_header_decoder.finish_decoding())?;
                    }
                    if decoder.is_suspended() && header.is_end_of_message() {
                        track!(decoder.decode(&[][..], Eos::new(true)))?;
                        let buf = track!(decoder.finish_decoding())?;
                        self.metrics.async_incoming_messages.increment();

                        let tx = self.async_incoming_tx.clone();
                        let mut handler = track!(self
                            .assigner
                            .assign_incoming_message_handler(&header.message))?;
                        DefaultCpuTaskQueue.with(|tasque| {
                            tasque.enqueue(move || {
                                let mut f = || {
                                    let size = track!(handler.decode(&buf[..], Eos::new(true)))?;
                                    track_assert_eq!(size, buf.len(), ErrorKind::Other);
                                    let action = track!(handler.finish_decoding())?;
                                    Ok(action)
                                };
                                let _ = tx.send(f());
                            })
                        })
                    } else {
                        self.async_incomings.insert(header.message.id, decoder);
                    }
                } else {
                    let mut handler = self
                        .receiving_messages
                        .remove(&header.message.id)
                        .expect("Never fails");

                    track!(handler.decode_from_read_buf(&mut self.rbuf))?;
                    if !handler.is_idle() && handler.is_suspended() && header.is_end_of_message() {
                        track!(handler.decode(&[][..], Eos::new(true)); header)?;
                    }
                    if handler.is_suspended() {
                        let _ = track!(self.packet_header_decoder.finish_decoding())?;
                    }
                    if handler.is_idle() {
                        let next_action = track!(handler.finish_decoding(); header)?;
                        track_assert_eq!(handler.consumable_bytes(), 0, ErrorKind::Other; header);
                        track_assert!(header.is_end_of_message(), ErrorKind::Other; header);

                        let event = MessageEvent::Received { next_action };
                        return Ok(Some(event));
                    } else {
                        self.receiving_messages.insert(header.message.id, handler);
                    }
                }
            }
            eprintln!("handle_incoming_messages loop mid 3");

            if self.rbuf.is_empty() && !self.rbuf.stream_state().is_normal() {
                break;
            }
            eprintln!("handle_incoming_messages loop end");
        }
        Ok(None)
    }

    fn check_write_timeout(&mut self) -> Result<()> {
        loop {
            if self.write_timeout.is_none() && !self.wbuf.is_empty() {
                self.write_timeout = Some(timer::timeout(self.options.tcp_write_timeout));
                self.is_written = false;
            }
            if let Ok(Async::Ready(Some(()))) = self.write_timeout.poll() {
                self.write_timeout = None;
                track_assert!(
                    self.is_written,
                    ErrorKind::Timeout,
                    "TCP socket buffer (send) is full for {:?}",
                    self.options.tcp_write_timeout
                );
                continue;
            }
            break;
        }
        Ok(())
    }
}
impl<A: AssignIncomingMessageHandler> Stream03 for MessageStream<A>
where
    <A::Handler as Decode>::Item: Send + 'static,
{
    type Item = Result<MessageEvent<<A::Handler as Decode>::Item>>;

    fn poll_next(self: Pin<&mut Self>, ctx: &mut Context) -> Poll03<Option<Self::Item>> {
        match self.poll_next_inner(ctx) {
            Err(e) => Poll03::Ready(Some(Err(e))),
            Ok(Poll03::Ready(Some(x))) => Poll03::Ready(Some(Ok(x))),
            Ok(Poll03::Ready(None)) => Poll03::Ready(None),
            Ok(Poll03::Pending) => Poll03::Pending,
        }
    }
}

impl<A: AssignIncomingMessageHandler> MessageStream<A>
where
    <A::Handler as Decode>::Item: Send + 'static,
{
    fn poll_next_inner(
        self: Pin<&mut Self>,
        ctx: &mut Context<'_>,
    ) -> Result<Poll03<Option<MessageEvent<<A::Handler as Decode>::Item>>>> {
        // SAFETY: not verified (TODO)
        let unpinned_self = unsafe { self.get_unchecked_mut() };
        eprintln!("MessageStream::poll");
        track!(unpinned_self.check_write_timeout())?;

        while let Async::Ready(Some(message)) =
            unpinned_self.async_outgoing_rx.poll().expect("Never fails")
        {
            eprintln!("MessageStream::poll async_outgoing");
            unpinned_self.start_sending_message(track!(message)?);
        }
        if let Async::Ready(Some(next)) =
            unpinned_self.async_incoming_rx.poll().expect("Never fails")
        {
            eprintln!("MessageStream::poll async_incoming");
            let next_action = track!(next)?;
            let event = MessageEvent::Received { next_action };
            return Ok(Poll03::Ready(Some(event)));
        }

        if let Some(event) = track!(unpinned_self.handle_incoming_messages(ctx))? {
            eprintln!("MessageStream::poll incoming_messages");
            return Ok(Poll03::Ready(Some(event)));
        }
        if let Some(event) = track!(unpinned_self.handle_outgoing_messages(ctx))? {
            eprintln!("MessageStream::poll outgoing_messages");
            return Ok(Poll03::Ready(Some(event)));
        }

        track_assert!(
            unpinned_self.sending_messages.len() <= unpinned_self.options.max_transmit_queue_len,
            ErrorKind::Other,
            "This stream may be overloaded"
        );

        eprintln!("MessageStream::poll about to return");
        if unpinned_self.rbuf.stream_state().is_eos() {
            Ok(Poll03::Ready(None))
        } else {
            Ok(Poll03::Pending)
        }
    }
}

impl<A: AssignIncomingMessageHandler> fmt::Debug for MessageStream<A> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MessageStream {{ .. }}")
    }
}

#[derive(Debug)]
pub enum MessageEvent<T> {
    Sent,
    Received { next_action: T },
}

#[derive(Debug)]
struct SendingMessage {
    seqno: u64,
    message: PacketizedMessage,
}
impl PartialEq for SendingMessage {
    fn eq(&self, other: &Self) -> bool {
        self.message.header().id == other.message.header().id
    }
}
impl Eq for SendingMessage {}
impl PartialOrd for SendingMessage {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for SendingMessage {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        let ordering = (self.message.header().priority, self.seqno)
            .cmp(&(other.message.header().priority, other.seqno));
        ordering.reverse()
    }
}

// TODO: AsyncRead を Read でラッピングする。
// この関数は tokio の task 内部で実行されることを想定している。そうでなければ panic する。
struct ReadWrapper<'a, 'b, T> {
    inner: T,
    ctx: &'a mut Context<'b>,
}

impl<'a, 'b, T: AsyncRead03 + Unpin> std::io::Read for ReadWrapper<'a, 'b, T>
where
    ReadWrapper<'a, 'b, T>: Unpin,
{
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        use core::task::Poll;
        eprintln!("read: (buflen={})", buf.len());
        // https://docs.rs/crate/async-stdio/0.3.0-alpha.4/source/src/lib.rs
        let ctx = &mut self.ctx;
        let mut read_buf = tokio::io::ReadBuf::new(buf);

        match AsyncRead03::poll_read(Pin::new(&mut self.inner), ctx, &mut read_buf) {
            Poll::Ready(result) => {
                eprintln!("read done");
                let () = result?;
                Ok(read_buf.filled().len())
            }
            Poll::Pending => {
                eprintln!("read pending");
                Err(std::io::ErrorKind::WouldBlock.into())
            }
        }
    }
}

// TODO: AsyncWrite を Write でラッピングする。
// この関数は tokio の task 内部で実行されることを想定している。そうでなければ panic する。
struct WriteWrapper<'a, 'b, T> {
    inner: T,
    ctx: &'a mut Context<'b>,
}

impl<'a, 'b, T: AsyncWrite03 + Unpin + Send> std::io::Write for WriteWrapper<'a, 'b, T>
where
    WriteWrapper<'a, 'b, T>: Unpin,
{
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        use core::task::Poll;
        // https://docs.rs/crate/async-stdio/0.3.0-alpha.4/source/src/lib.rs
        let ctx = &mut self.ctx;

        match AsyncWrite03::poll_write(Pin::new(&mut self.inner), ctx, buf) {
            Poll::Ready(result) => {
                eprintln!("write done: {:?}", result);
                result
            }
            Poll::Pending => {
                eprintln!("write pending");
                Err(std::io::ErrorKind::WouldBlock.into())
            }
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        todo!()
    }
}
