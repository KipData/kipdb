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