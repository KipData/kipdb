use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use futures::stream::{SplitSink, SplitStream};
use futures::StreamExt;
use tokio::io::AsyncBufReadExt;
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, LinesCodec};
use crate::cmd::Command;
use crate::Result;

type CommandFramedStream = SplitStream<Framed<TcpStream, LinesCodec>>;
type CommandFramedSink = SplitSink<Framed<TcpStream, LinesCodec>, Command>;

pub struct Connection {
    writer: CommandFramedSink,
    reader: CommandFramedStream
}

impl Connection {
    pub fn new(stream: TcpStream) -> Result<Connection> {
        let framed = Framed::new(stream, LinesCodec::new());
        let (writer, reader) = framed.split::<Command>();
        Ok(Connection{
            writer,
            reader
        })
    }


}