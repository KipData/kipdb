# KipDB - Keep it Public DB

<p align="left">
  <a href="https://gitee.com/Kould/KipDB" target="_blank">
    <img src="https://gitee.com/Kould/KipDB/badge/star.svg?theme=white" alt="star"/>
    <img src="https://gitee.com/Kould/KipDB/badge/fork.svg" alt="fork"/>
  </a>
  <a href="https://github.com/KKould/KipDB" target="_blank">
    <img src="https://img.shields.io/github/stars/KKould/KipDB.svg?style=social" alt="github star"/>
    <img src="https://img.shields.io/github/forks/KKould/KipDB.svg?style=social" alt="github fork"/>
  </a>
  <a href="https://crates.io/crates/kip_db/" target="_blank">
    <img src="https://img.shields.io/crates/v/kip_db.svg" alt="Crates.io"/>
  </a>
</p>

### [Kiss](https://zh.m.wikipedia.org/zh/KISS%E5%8E%9F%E5%88%99) First Data Base
## 快速上手 🤞
### 直接调用
```rust
/// 指定文件夹以开启一个KvStore
let kip_db = LsmStore::open("/tmp/learning materials").await?;

// 插入数据
kip_db.set(&vec![b'k'], vec![b'v']).await?;
// 获取数据
kip_db.get(&vec![b'k']).await?;
// 已占有硬盘大小
kip_db.size_of_disk().await?
// 已有数据数量
kip_db.len().await?;
// 删除数据
kip_db.remove(&vec![b'k']).await?;

// 强制数据刷入硬盘
kip_db.flush().await?;
```
### 远程应用
#### 服务启动
```rust
/// 服务端启动！
let listener = TcpListener::bind("127.0.0.1:8080").await?;

kip_db::net::server::run(listener, tokio::signal::ctrl_c()).await;
```
#### 远程调用
```rust
/// 客户端调用！
let mut client = Client::connect("127.0.0.1:8080").await?;

// 插入数据
client.set(&vec![b'k'], vec![b'v']).await?
// 获取数据
client.get(&vec![b'k']).await?
// 已占有硬盘大小
client.size_of_disk().await?
// 存入指令数
client.len().await?
// 数据刷入硬盘
client.flush().await?
// 删除数据
client.remove(&vec![b'k']).await?;
// 批量指令执行(可选 并行/同步 执行)
let vec_batch_cmd = vec![CommandData::get(b"k1".to_vec()), CommandData::get(b"k2".to_vec())];
client.batch(vec_batch_cmd, true).await?
```

## 内置多种持久化内核👍
- LsmStore: 基于Lsm，使用Leveled Compaction策略(主要内核)
- HashStore: 基于哈希
- SledStore: 基于Sled数据库

## 操作示例⌨️
### 服务端
``` shell
PS D:\Workspace\kould\KipDB\target\release> ./server -h
KipDB-Server 0.1.0
Kould <2435992353@qq.com>
A KV-Store server

USAGE:
server.exe [OPTIONS]

OPTIONS:
-h, --help           Print help information
--ip <IP>
--port <PORT>
-V, --version        Print version information

PS D:\Workspace\kould\KipDB\target\release> ./server   
2022-10-13T06:50:06.528875Z  INFO kip_db::kernel::lsm::ss_table: [SsTable: 6985961041465315323][restore_from_file][TableMetaInfo]: MetaInfo { level: 0, version: 0, data_len: 118, index_len: 97, part_size: 64, crc_code: 43553795 }, Size of Disk: 263
2022-10-13T06:50:06.529614Z  INFO kip_db::net::server: [Listener][Inbound Connections]
2022-10-13T06:50:13.437586Z  INFO kip_db::net::server: [Listener][Shutting Down]

```
### 客户端
``` shell
PS D:\Workspace\kould\KipDB\target\release> ./cli --help
KipDB-Cli 0.1.0
Kould <2435992353@qq.com>
Issue KipDB Commands

USAGE:
    cli.exe [OPTIONS] <SUBCOMMAND>

OPTIONS:
    -h, --help                   Print help information
        --hostname <hostname>    [default: 127.0.0.1]
        --port <PORT>            [default: 6333]
    -V, --version                Print version information

SUBCOMMANDS:
    batch-get
    batch-get-parallel
    batch-remove
    batch-remove-parallel
    batch-set
    batch-set-parallel
    flush
    get
    help                     Print this message or the help of the given subcommand(s)
    len
    remove
    set
    size-of-disk
    
PS D:\Workspace\kould\KipDB\target\release> ./cli batch-set kould kipdb welcome !
2022-09-27T09:50:11.768931Z  INFO cli: ["Done!", "Done!"]

PS D:\Workspace\kould\KipDB\target\release> ./cli batch-get kould kipdb          
2022-09-27T09:50:32.753919Z  INFO cli: ["welcome", "!"]
```

## Features🌠
- Marjor Compation 
  - 多级递增循环压缩 ✅
  - SSTable锁
    - 避免并行压缩时数据范围重复 ✅
- KVStore
  - 参考Sled增加api
    - size_of_disk ✅
    - clear
    - contains_key
    - len ✅
    - ...
- SSTable
  - 校验和 ✅
    - 用于校验数据是否正常
  - 布隆过滤器 ✅
    - 加快获取键值的速度
  - MetaBlock区
    - 用于存储统计数据布隆过滤器的存放
  - 数据压缩
- Read Cache ✅
  - 加快数据读取，避免冗余硬盘读取IO
- Manifest
  - 多版本
  - 持久化
- 分布式
  - TAS(Test And Set)与Master调度主机
  - 服务端作为Worker支持单机与集群
  - 使用Raft复制协议保持状态一致
## Perf火焰图监测
- 为了方便性能调优等监测，提供了两个Dockerfile作为支持
  - Dockerfile: KipDB的Server与Cli
  - Dockerfile-perf: 外部Perf监测

### 使用步骤
1. 打包KipDB本体镜像``docker build -t kould/kip-db:v1 .``
2. 打包Perf监测镜像``docker build -f Dockerfile-perf -t kould/perf:v1 .``
3. 以任意形式执行kould/kip
   - 例: ``docker run kould/kip-db:v1``
4. 执行``attach-win.sh <kip-db容器ID>``
   - 例: ``./attach-win.sh 263ad21cc56169ebec79bbf614c6986a78ec89a6e0bdad5e364571d28bee2bfc``
5. 在该bash内输入. ``record.sh <kip-db的server进程pid>``
   - 若不清楚进程id是多少可以直接输入ps，通常为1
   - 注意!： 不要关闭bash，否则会监听失败！
6. **随后去对KipDB进行对应需要监测的操作**
7. 操作完毕后回到**步骤5**的bash内，以ctrl + c终止监听，得到perf.data
8. 继续在该bash内输入``. plot.sh <图片名.svg>``, 即可生成火焰图
    - 导出图片一般可使用 ``docker cp`` 和 ``docker exec`` 或挂载 volume，为方便预览和复制文件，容器内置了轻量网页服务，执行 ``thttpd -p <端口号>`` 即可。由于脚本中没有设置端口转发，需要 ``docker inspect <目标容器ID> | grep IPAdress`` 查看目标容器的 IP，然后在浏览器中访问即可。若需要更灵活的操作，可不用以上脚本手动添加参数运行容器。

参考自：https://chinggg.github.io/post/docker-perf/