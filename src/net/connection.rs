use futures::{SinkExt, StreamExt};
use futures::stream::{SplitSink, SplitStream};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use crate::error::ConnectionError;
use crate::kernel::options_none;
use crate::net::codec::NetCommandCodec;
use crate::net::Result;
use crate::proto::net_pb::CommandOption;

type CommandFramedStream = SplitStream<Framed<TcpStream, NetCommandCodec>>;
type CommandFramedSink = SplitSink<Framed<TcpStream, NetCommandCodec>, CommandOption>;

pub(crate) struct Connection {
    writer: CommandFramedSink,
    reader: CommandFramedStream
}

impl Connection {
    /// 新建连接
    pub(crate) fn new(stream: TcpStream) -> Connection {
        let framed = Framed::new(stream, NetCommandCodec::new());
        let (writer, reader) = framed.split::<CommandOption>();
        Connection{
            writer,
            reader
        }
    }

    /// 读取CommandOption
    pub(crate) async fn read(&mut self) -> Result<CommandOption> {
        match self.reader.next().await {
            None => {
                Ok(options_none())
            }
            Some(Ok(option)) => {
                Ok(option)
            }
            Some(Err(e)) => {
                panic!("{e:?}")
            }
        }
    }

    /// 写入CommandOption
    pub(crate) async fn write(&mut self, option: CommandOption) -> Result<()> {
        if self.writer.send(option).await.is_err() {
            Err(ConnectionError::WriteFailed)
        } else {
            Ok(())
        }
    }
}