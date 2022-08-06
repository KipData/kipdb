use core::panicking::panic;
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use futures::stream::{Next, SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use futures::sink::Feed;
use serde::de::Unexpected::Option;
use tokio::io::AsyncBufReadExt;
use tokio::net::TcpStream;
use tokio_util::codec::{Framed};
use crate::cmd::Command;
use crate::net::codec::CommandCodec;
use crate::net::ConnectionError;
use crate::Result;

type CommandFramedStream = SplitStream<Framed<TcpStream, CommandCodec>>;
type CommandFramedSink = SplitSink<Framed<TcpStream, CommandCodec>, Command>;

pub struct Connection {
    writer: CommandFramedSink,
    reader: CommandFramedStream
}

impl Connection {
    pub fn new(stream: TcpStream) -> Connection {
        let framed = Framed::new(stream, CommandCodec::new());
        let (writer, reader) = framed.split::<Command>();
        Connection{
            writer,
            reader
        }
    }

    pub async fn read(&mut self) -> Result<Option<Command>> {
        match self.reader.next().await {
            None => {
                Ok(None)
            }
            Some(Ok(cmd)) => {
                Ok(Some(cmd))
            }
            Some(Err(e)) => {
                panic!("{:?}", e)
            }
        }
    }

    pub async fn write(&mut self, cmd: Command) -> Result<()> {
        if self.writer.send(cmd).await.is_err() {
            Err(ConnectionError::WriteFailed)
        } else {
            Ok(())
        }
    }
}