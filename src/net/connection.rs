use futures::{SinkExt, StreamExt};
use futures::stream::{SplitSink, SplitStream};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use crate::error::ConnectionError;
use crate::net::codec::CommandCodec;
use crate::net::Result;
use crate::net::CommandOption;

type CommandFramedStream = SplitStream<Framed<TcpStream, CommandCodec>>;
type CommandFramedSink = SplitSink<Framed<TcpStream, CommandCodec>, CommandOption>;

pub struct Connection {
    writer: CommandFramedSink,
    reader: CommandFramedStream
}

impl Connection {
    /// 新建连接
    pub fn new(stream: TcpStream) -> Connection {
        let framed = Framed::new(stream, CommandCodec::new());
        let (writer, reader) = framed.split::<CommandOption>();
        Connection{
            writer,
            reader
        }
    }

    /// 读取CommandOption
    pub async fn read(&mut self) -> Result<CommandOption> {
        match self.reader.next().await {
            None => {
                Ok(CommandOption::None)
            }
            Some(Ok(option)) => {
                Ok(option)
            }
            Some(Err(e)) => {
                panic!("{:?}", e)
            }
        }
    }

    /// 写入CommandOption
    pub async fn write(&mut self, option: CommandOption) -> Result<()> {
        if self.writer.send(option).await.is_err() {
            Err(ConnectionError::WriteFailed)
        } else {
            Ok(())
        }
    }
}