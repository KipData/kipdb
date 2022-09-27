# KipDB - Keep it Public DB

[Kiss](https://zh.m.wikipedia.org/zh/KISS%E5%8E%9F%E5%88%99) First Data Base
## 快速上手 🤞
```rust
// 指定文件夹以开启一个KvStore
let kip_db = LsmStore::open("/tmp/learning materials").await?;

// 插入数据
kip_db.set(&vec![b'k'], vec![b'v']).await?;
// 获取数据
kip_db.get(&vec![b'k']).await?;
// 删除数据
kip_db.remove(&vec![b'k']).await?;

// 强制数据刷入硬盘
kip_db.flush().await?;

// 关闭内核(关闭，但没完全关闭 仅结束前处理)
kip_db.shut_down().await?;
```

## 内置多种持久化内核👍
- LsmStore: 基于Lsm，使用Leveled Compaction策略(主要内核)
- HashStore: 基于哈希
- SledStore: 基于Sled数据库

### 操作示例⌨️
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
SUBCOMMANDS:rsion                Print version information
    batch-get                
    batch-get-parallel       
    batch-remove             
    batch-remove-parallel    
    batch-set                
    batch-set-parallel
    get
    help                     Print this message or the help of the given subcommmand(s)
    remove
    set

PS D:\Workspace\kould\KipDB\target\release> ./cli batch-set kould kipdb welcome !
2022-09-27T09:50:11.768931Z  INFO cli: ["Done!", "Done!"]

PS D:\Workspace\kould\KipDB\target\release> ./cli batch-get kould kipdb          
2022-09-27T09:50:32.753919Z  INFO cli: ["welcome", "!"]
```

## Features🌠
- Marjor Compation 
  - 多级递增循环压缩
  - SSTable锁
    - 避免并行压缩时数据范围重复
- KVStore
  - 参考Sled增加api
    - size_of_disk
    - clear
    - contains_key
    - len
    - ...
- SSTable
  - 校验和
    - 用于校验数据是否正常
  - 布隆过滤器
    - 加快获取键值的速度
  - MetaBlock区
    - 用于存储统计数据布隆过滤器的存放
  - 数据压缩
- Cache
  - 加快数据读取，避免冗余硬盘读取IO
- Manifest
  - 多版本
  - 持久化