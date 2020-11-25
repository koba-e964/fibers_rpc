use crate::channel::ChannelOptions;
use crate::client_side_handlers::{Assigner, BoxResponseHandler};
use crate::message::{MessageId, OutgoingMessage};
use crate::message_stream::MessageStream;
use crate::metrics::{ChannelMetrics, ClientMetrics};
use crate::{Error, ErrorKind, Result};
use fibers::time::timer::{self, Timeout, TimerExt};
use futures::{Async, Future, Poll, Stream};
use futures03::TryFutureExt;
use slog::Logger;
use std::fmt;
use std::net::SocketAddr;
use std::sync::atomic::{self, AtomicBool};
use std::sync::mpsc::RecvError;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use trackable::error::ErrorKindExt;

pub const DEFAULT_KEEP_ALIVE_TIMEOUT_SECS: u64 = 60 * 10;

#[derive(Debug)]
pub struct ClientSideChannel {
    logger: Logger,
    server: SocketAddr,
    is_server_down: Arc<AtomicBool>,
    keep_alive: KeepAlive,
    next_message_id: MessageId,
    message_stream: MessageStreamState,
    exponential_backoff: ExponentialBackoff,
    options: ChannelOptions,
    metrics: ClientMetrics,
}
impl ClientSideChannel {
    pub fn new(
        logger: Logger,
        server: SocketAddr,
        is_server_down: Arc<AtomicBool>,
        options: ChannelOptions,
        metrics: ClientMetrics,
    ) -> Self {
        ClientSideChannel {
            logger,
            server,
            is_server_down,
            keep_alive: KeepAlive::new(Duration::from_secs(DEFAULT_KEEP_ALIVE_TIMEOUT_SECS)),
            next_message_id: MessageId(0),
            message_stream: MessageStreamState::new(server, &options),
            exponential_backoff: ExponentialBackoff::new(),
            options,
            metrics,
        }
    }

    pub fn set_keep_alive_timeout(&mut self, duration: Duration) {
        self.keep_alive = KeepAlive::new(duration);
    }

    pub fn send_message(
        &mut self,
        mut message: OutgoingMessage,
        response_handler: Option<BoxResponseHandler>,
    ) {
        message.header.id = self.next_message_id.next();
        if !self.message_stream.send_message(message, response_handler) {
            self.metrics.discarded_outgoing_messages.increment();
        }
    }

    pub fn force_wakeup(&mut self) {
        if let MessageStreamState::Wait { .. } = self.message_stream {
            info!(self.logger, "Waked up");
            // We don't multiply `timeout` by 2 here because it would cause
            // arithmetic overflow on Duration/Instant computation
            // if this function is repeatedly called.
            let next = MessageStreamState::Connecting {
                buffer: Vec::new(),
                future: tcp_connect(self.server, &self.options),
            };
            self.update_message_stream_state(next);
        }
    }

    fn update_message_stream_state(&mut self, next: MessageStreamState) {
        if let MessageStreamState::Wait { .. } = next {
            self.is_server_down.store(true, atomic::Ordering::SeqCst);
        } else {
            self.is_server_down.store(false, atomic::Ordering::SeqCst);
        };
        self.message_stream = next;
    }

    fn wait_or_reconnect(
        server: SocketAddr,
        backoff: &mut ExponentialBackoff,
        metrics: &ClientMetrics,
        options: &ChannelOptions,
    ) -> MessageStreamState {
        metrics.channels().remove_channel_metrics(server);
        if let Some(timeout) = backoff.timeout() {
            MessageStreamState::Wait { timeout }
        } else {
            backoff.next();
            MessageStreamState::Connecting {
                buffer: Vec::new(),
                future: tcp_connect(server, options),
            }
        }
    }

    fn poll_message_stream(&mut self) -> Result<Async<Option<MessageStreamState>>> {
        match self.message_stream {
            MessageStreamState::Wait { ref mut timeout } => {
                let is_expired = track!(timeout.poll().map_err(from_timeout_error))?.is_ready();
                if is_expired {
                    info!(
                        self.logger,
                        "Reconnecting timeout expired; starts reconnecting"
                    );
                    self.exponential_backoff.next();
                    let next = MessageStreamState::Connecting {
                        buffer: Vec::new(),
                        future: tcp_connect(self.server, &self.options),
                    };
                    Ok(Async::Ready(Some(next)))
                } else {
                    Ok(Async::NotReady)
                }
            }
            MessageStreamState::Connecting {
                ref mut future,
                ref mut buffer,
            } => match track!(future.poll().map_err(Error::from)) {
                Err(e) => {
                    warn!(self.logger, "Failed to TCP connect: {}", e);
                    self.metrics
                        .discarded_outgoing_messages
                        .add_u64(buffer.len() as u64);
                    let next = Self::wait_or_reconnect(
                        self.server,
                        &mut self.exponential_backoff,
                        &self.metrics,
                        &self.options,
                    );
                    Ok(Async::Ready(Some(next)))
                }
                Ok(Async::NotReady) => Ok(Async::NotReady),
                Ok(Async::Ready(stream)) => {
                    info!(
                        self.logger,
                        "TCP connected: stream={:?}, buffered_messages={}",
                        stream,
                        buffer.len()
                    );
                    let stream = MessageStream::new(
                        stream,
                        Assigner::new(),
                        self.options.clone(),
                        self.metrics.channels().create_channel_metrics(self.server),
                    );
                    let mut connected = MessageStreamState::Connected { stream };
                    for m in buffer.drain(..) {
                        connected.send_message(m.message, m.handler);
                    }
                    Ok(Async::Ready(Some(connected)))
                }
            },
            MessageStreamState::Connected { ref mut stream } => match track!(stream.poll()) {
                Err(e) => {
                    error!(self.logger, "Message stream aborted: {}", e);
                    let next = Self::wait_or_reconnect(
                        self.server,
                        &mut self.exponential_backoff,
                        &self.metrics,
                        &self.options,
                    );
                    Ok(Async::Ready(Some(next)))
                }
                Ok(Async::NotReady) => Ok(Async::NotReady),
                Ok(Async::Ready(None)) => {
                    warn!(self.logger, "Message stream terminated");
                    let next = Self::wait_or_reconnect(
                        self.server,
                        &mut self.exponential_backoff,
                        &self.metrics,
                        &self.options,
                    );
                    Ok(Async::Ready(Some(next)))
                }
                Ok(Async::Ready(Some(_event))) => {
                    self.exponential_backoff.reset();
                    self.keep_alive.extend_period();
                    Ok(Async::Ready(None))
                }
            },
        }
    }
}
impl Future for ClientSideChannel {
    type Item = ();
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let is_timeout = track!(self.keep_alive.poll())?.is_ready();
        if is_timeout {
            return Ok(Async::Ready(()));
        }

        let mut count = 0;
        while let Async::Ready(next) = track!(self.poll_message_stream())? {
            if let Some(next) = next {
                self.update_message_stream_state(next);
            }

            count += 1;
            if count > self.options.yield_threshold {
                if let Some(m) = self.message_stream.metrics() {
                    m.fiber_yielded.increment();
                }
                return fibers::fiber::yield_poll();
            }
        }
        Ok(Async::NotReady)
    }
}
impl Drop for ClientSideChannel {
    fn drop(&mut self) {
        self.metrics.channels().remove_channel_metrics(self.server);
    }
}

#[allow(clippy::large_enum_variant)]
enum MessageStreamState {
    Wait {
        timeout: Timeout,
    },
    Connecting {
        buffer: Vec<BufferedMessage>,
        future: Box<dyn Future<Item = TcpStream, Error = Error> + Send + 'static>,
    },
    Connected {
        stream: MessageStream<Assigner>,
    },
}
impl MessageStreamState {
    fn new(addr: SocketAddr, options: &ChannelOptions) -> Self {
        MessageStreamState::Connecting {
            buffer: Vec::new(),
            future: tcp_connect(addr, options),
        }
    }

    fn metrics(&self) -> Option<&ChannelMetrics> {
        match *self {
            MessageStreamState::Wait { .. } | MessageStreamState::Connecting { .. } => None,
            MessageStreamState::Connected { ref stream } => Some(stream.metrics()),
        }
    }

    fn send_message(
        &mut self,
        message: OutgoingMessage,
        handler: Option<BoxResponseHandler>,
    ) -> bool {
        match *self {
            MessageStreamState::Wait { .. } => {
                if let Some(mut handler) = handler {
                    let e = ErrorKind::Unavailable
                        .cause("TCP stream disconnected (waiting for reconnecting)");
                    handler.handle_error(track!(e).into());
                }
                false
            }
            MessageStreamState::Connecting { ref mut buffer, .. } => {
                buffer.push(BufferedMessage { message, handler });
                true
            }
            MessageStreamState::Connected { ref mut stream } => {
                let message_id = message.header.id;
                stream.send_message(message);
                if let Some(handler) = handler {
                    stream
                        .assigner_mut()
                        .register_response_handler(message_id, handler);
                }
                true
            }
        }
    }
}
impl fmt::Debug for MessageStreamState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            MessageStreamState::Wait { timeout } => write!(f, "Wait {{ timeout: {:?} }}", timeout),
            MessageStreamState::Connecting { buffer, .. } => {
                write!(f, "Connecting {{ buffer: {:?}, .. }}", buffer)
            }
            MessageStreamState::Connected { stream } => {
                write!(f, "Connected {{ stream: {:?} }}", stream)
            }
        }
    }
}

struct BufferedMessage {
    message: OutgoingMessage,
    handler: Option<BoxResponseHandler>,
}
impl fmt::Debug for BufferedMessage {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "BufferedMessage {{ messag: {:?}, handler.is_some(): {} }}",
            self.message,
            self.handler.is_some()
        )
    }
}

#[derive(Debug)]
struct KeepAlive {
    future: Timeout,
    timeout: Duration,
    extend_period: bool,
}
impl KeepAlive {
    fn new(timeout: Duration) -> Self {
        KeepAlive {
            future: timer::timeout(timeout),
            timeout,
            extend_period: false,
        }
    }

    fn extend_period(&mut self) {
        self.extend_period = true;
    }

    fn poll_timeout(&mut self) -> Result<bool> {
        let result = track!(self.future.poll().map_err(from_timeout_error))?;
        Ok(result.is_ready())
    }
}
impl Future for KeepAlive {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        while track!(self.poll_timeout())? {
            if self.extend_period {
                self.future = timer::timeout(self.timeout);
                self.extend_period = false;
            } else {
                return Ok(Async::Ready(()));
            }
        }
        Ok(Async::NotReady)
    }
}

#[derive(Debug)]
struct ExponentialBackoff {
    retried_count: usize,
}
impl ExponentialBackoff {
    fn new() -> Self {
        ExponentialBackoff { retried_count: 0 }
    }
    fn next(&mut self) {
        self.retried_count += 1;
    }
    fn timeout(&self) -> Option<Timeout> {
        if self.retried_count == 0 {
            None
        } else {
            let duration = Duration::from_secs(2u64.pow(self.retried_count as u32 - 1));
            Some(timer::timeout(duration))
        }
    }
    fn reset(&mut self) {
        self.retried_count = 0;
    }
}

fn from_timeout_error(_: RecvError) -> Error {
    ErrorKind::Other.cause("Broken timer").into()
}

fn tcp_connect(
    server: SocketAddr,
    options: &ChannelOptions,
) -> Box<dyn Future<Item = TcpStream, Error = Error> + Send + 'static> {
    let future = Box::pin(TcpStream::connect(server))
        .compat()
        .timeout_after(options.tcp_connect_timeout)
        .map_err(|e| {
            e.map(Error::from)
                .unwrap_or_else(|| ErrorKind::Timeout.error().into())
        });
    Box::new(track_err!(future))
}
