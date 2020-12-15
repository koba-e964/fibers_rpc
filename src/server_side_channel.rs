use crate::channel::ChannelOptions;
use crate::message::OutgoingMessage;
use crate::message_stream::{MessageEvent, MessageStream};
use crate::metrics::ChannelMetrics;
use crate::server_side_handlers::{Action, Assigner};
use crate::Error;
use futures::{Async, Poll, Stream};
use futures03::compat::Compat;
use futures03::TryStreamExt;
use slog::Logger;
use tokio::net::TcpStream;

#[derive(Debug)]
pub struct ServerSideChannel {
    logger: Logger,
    message_stream: Compat<MessageStream<Assigner>>,
}
impl ServerSideChannel {
    pub fn new(
        logger: Logger,
        transport_stream: TcpStream,
        assigner: Assigner,
        options: ChannelOptions,
        metrics: ChannelMetrics,
    ) -> Self {
        let message_stream =
            MessageStream::new(transport_stream, assigner, options, metrics).compat();
        ServerSideChannel {
            logger,
            message_stream,
        }
    }

    pub fn reply(&mut self, message: OutgoingMessage) {
        self.message_stream.get_mut().send_message(message);
    }
}
impl Stream for ServerSideChannel {
    type Item = Action;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let mut count = 0;
        while let Async::Ready(item) = track!(self.message_stream.poll())? {
            if let Some(event) = item {
                match event {
                    MessageEvent::Sent => {
                        trace!(self.logger, "Completed to send a message");
                    }
                    MessageEvent::Received { next_action } => {
                        trace!(self.logger, "Completed to receive a message");
                        return Ok(Async::Ready(Some(next_action)));
                    }
                }

                count += 1;
                if count > self.message_stream.get_ref().options().yield_threshold {
                    self.message_stream
                        .get_ref()
                        .metrics()
                        .fiber_yielded
                        .increment();
                    return fibers::fiber::yield_poll();
                }
            } else {
                return Ok(Async::Ready(None));
            }
        }
        Ok(Async::NotReady)
    }
}
