# 分布式 Compact
### [English Version](README.md)

## 概述
在 [ToplingDB](https://github.com/topling/toplingdb) 中，dcompact(Distributed Compaction) 在形式上是作为一个 [SidePlugin](https://github.com/topling/rockside/wiki) 实现的，也就是说，要使用分布式 Compact，用户代码不需要任何修改，只需要改 json/yaml 配置文件。

在分布式 Compact 的实现中：

1. 运行 ToplingDB/RocksDB 的一方称为 Hoster，在 Server/Client 模型中，是 Client
2. 运行 dcompact\_worker 的一方称为 Worker，在 Server/Client 模型中，是 Server
3. 同一个 worker 可以同时为多个 hoster 服务，同一个 hoster 也可以把自己的 compact 任务发给多个 worker 执行
4. 编译 ToplingDB 时，本模块(topling-dcompact)由 ToplingDB 的 Makefile 中从 github 自动 clone 下来

用户自定义的插件，例如 MergeOperator, CompactionFilter 等需要编译成动态库，运行 dcompact\_worker.exe 时通过 LD\_PRELOAD 加载。使用这种方式，相同进程可以加载多个不同的动态库，从而为多个不同的 DB 提供 compact 服务，例如 [MyTopling(MySQL)](https://github.com/topling/mytopling) 和 [Todis(Redis)](https://github.com/topling/todis) 就使用这种方式共享相同的 dcompact\_worker 进程。

> 分布式 Compact 的实现类名是 DcompactEtcd，这是因为 ToplingDB 分布式 Compact 最初是通过 etcd-cpp-api 使用 etcd 用作 Hoster 与 Worker 的交互，后来因为 etcd-cpp-api 的 [bug #78](https://github.com/etcd-cpp-apiv3/etcd-cpp-apiv3/issues/78) 而不得不弃用 etcd。目前 etcd 相关代码已经无用，仅有 DcompactEtcd 作为类名保留下来。


## 1. 配置

### 1.1. json 配置示例

分布式 Compact 在 json 配置文件中进行设置：
```json
  "CompactionExecutorFactory": {
    "dcompact": {
      "class": "DcompactEtcd",
      "params": {
        "allow_fallback_to_local": true,
        "hoster_root": "/nvmepool/shared/db1",
        "instance_name": "db1",
        "nfs_mnt_src": ":/nvmepool/shared",
        "nfs_mnt_opt": "nolock,addr=192.168.31.16",
        "http_max_retry": 3,
        "http_timeout": 1,
        "http_workers": [
          "http://active-host:8080",
          { "url": "http://worker.for.submit.compact.job",
            "base_url": "http://worker.for.probe.and.shutdown.compact.job",
            "web_url": "https://web.page.proxy"
          },
          "//http://commented-host:8080",
          "//end_http_workers"
        ],
        "dcompact_min_level": 2
      }
    }
  }
```

### 1.2. 配置参数说明

在 1.1 中，`CompactionExecutorFactory` 是 C++ 的接口，在 json 中是一个 namespace，在这个 namespace 中，定义了一个 varname 为 `dcompact`，类名为 `DcompactEtcd` 的（实现了 `CompactionExecutorFactory` 接口）的对象，该对象使用 `params` 进行构造。params 解释说明如下：

属性名  | 类型 | 默认值 | 解释说明
-------|:----:|-------|--------
allow_fallback_to_local| bool |false | 如果分布式 Compact 失败，是否允许回退到本地 Compact
hoster_root        | string | 空 | 该 db 的根目录，一般设置为与 DB::Open 中的 `path` 变量相同。
instance_name      | string | 空 | 该 db 实例名，在多租户场景下，CompactWorker 结点使用 instance\_name 区分不同的 db 实例
nfs_type           | string | 空 | NFS 类型，空表示 `nfs`, 也可以是 `glusterfs`, `smbfs` 等等
nfs_mnt_src        | string | 空 | NFS 挂载源
nfs_mnt_opt        | string | 空 | NFS 挂载选项
http_max_retry     | int    |  3 | 最大重试次数
retry_sleep_time   | int    |  1 | 单位秒，如果重试原因是服务器繁忙，则睡眠这么长时间再进行重试，避免重试太过频繁
overall_timeout    | int    |  5 | 单位秒，单个分布式 Compact 任务的单次执行尝试(attempt)从头至尾耗时的超时时间
http_timeout       | int    |  3 | 单位秒，http 连接的超时时间，一般情况下，超时即意味着出错
timeout_multiplier | int    | 10 | dcompact 执行时间超过预估时间的这么多倍时，会认为超时失败并重试
estimate_speed     | size   | 10M | 预估的单个 compact 吞吐率，该值会随着时间自动调整，用来计算预估的 compact 执行时间及超时时间
load_balance       | enum   | kRoundRobin | 可选值有 {kRoundRobin, kWeight}，表示多个 http_workers 的选择方式
max_book_dbcf      | int    | 20 | 如果进程运行中对临时 DB 进行 dcompact，max_book_dbcf 表示最多显示多少个最近活动的 DB
http_workers       | string<br/>或<br/>object| 空 | 多个（至少一个）http url, 以 `//` 开头的会被跳过，相当于是被注释掉了<br/> 末尾的 `//end_http_workers` 是为了 `start_workers.sh` 脚本服务的，不能删除<br/>**注意**: 必须至少定义一个 worker, 否则 DB 启动时会主动 abort
dcompact_min_level | int    |  2 | 只有在 Compact Output Level 大于等于该值时，才使用分布式 compact，小于该值时使用本地 compact
alert_email        | string | 空 | 重试次数到达 http_max_retry 但分布式 Compact 仍然失败时，发送 email 报警
alert_http         | string | 空 | 重试次数到达 http_max_retry 但分布式 Compact 仍然失败时，向此 http 发送 POST 报警
alert_interval     | int    | 60 | 如果频繁失败，且每次都报警，报警信息会洪水泛滥，所以每隔这么长时间（单位：**秒**），才报警一次
web_domain         | string | 空 | 在浏览器中，多个 compact_worker 的状态以 iframe 方式内嵌在页面中，web_domain 用来实现自动调整 iframe 高度（JavaScript 需要跨域权限）

以上 int, bool, enum, size 类型的参数可以通过 http 在线修改。

<!--
属性名 | 解释说明（ETCD 相关配置，已经过时无用）
-------|----------
etcd   | etcd 的连接选项，默认无认证，需要认证的话，有两种方式：<br/>  1. 使用 username + password 认证<br/>  2. 使用 ca + cert + key 认证，其中 cert 和 key 可以为空，ca 不能为空<br>认证时首选 ca，如果 ca 非空，就用 ca 认证，ca 为空但 username 非空时，使用 username + password 认证
`etcd_root` | 该 `dcompact` 的所有数据都保存在 `etcd_root` 之下
-->

### 1.3. http_workers
在内网环境下，每个 worker 可以简单地配置为一个字符串，表示 worker 的 http url。

在公网（例如云计算）环境下，worker 隐藏在反向代理/负载均衡后面，为此增加了几个配置字段：

字段名| 类型 | 默认值 | 说明
------|------|------|-----
url | string | 无 | 提交 compact job
base_url | string | 无 | probe 查询 compact job 或 shutdown 指定 compact job
web_url | string | 无 | 在浏览器中通过 stat 查看状态，以及查看 log 文件。如果未定义，使用 base_url
weight  | string | 无 | http_workers 包含多个服务器时，配置每个服务器的权重，派发 compact 时按权重等比例选择，权重仅在 http_workers 包含多个服务器时有用，表示不同服务器之间的相对值。例如权重 100 比权重 50 多一倍的入选机会，从而承担的计算量也多一倍

#### 最佳实践
应该只定义 url = `http://some.host`，不定义 base_url，此时：
* 生效的 base_url = `http://some.host`
* 生效的 url = `http://some.host/dcompact`

在公网环境中，web_url 可定义为不同于生效的 base_url，用于服务浏览器，而生效的 base_url 用于服务 dcompact 命令。

#### 历史原因导致的复杂性
用户可以不用关心该复杂性，仅遵循最佳实践即可
1. url 与 base_url 必须至少定义其中之一
   * 生效的 url 一定形如 `http://some.host/dcompact`
   * 生效的 base_url 一定形如 `http://some.host`
1. 如果定义 base_url，则必须形如 `http://some.host` (或 https)
1. 如果定义 url，则必须形如 `http://some.host/dcompact` 或 `http://some.host` (或 https)
1. 如果已定义 base_url，但未定义 url，则生效的 url = "${base_url}/dcompact"
1. 如果未定义 base_url，但已定义 url
   * 如果 url 形如 `http://some.host/dcompact` 则 url 保持不变
   * 如果 url 形如 `http://some.host` 则生效的 url 为 `http://some.host/dcompact`
   * 生效的 base_url 从生效的 url 推导为 `http://some.host`

### 1.4. 注意事项
CFOptions 的 `level_compaction_dynamic_level_bytes` 务必显示指定为 `false`，为 `true` 时，很可能会跳过 L1，直接 compact 到 L**n**，产生很大的单个 compact 并且无法利用 `max_level1_subcompactions` 配置的并发，导致长时间的卡顿。

## 2. dcompact worker

### 2.1. dcompact 作为服务
```bash
export MAX_PARALLEL_COMPACTIONS=16
export NFS_DYNAMIC_MOUNT=0
export NFS_MOUNT_ROOT=/nvme-shared
export WORKER_DB_ROOT=/tmp
rm -rf $WORKER_DB_ROOT/db-instance-name-1

# Only for ToplingZipTable
export ZIP_SERVER_OPTIONS="listening_ports=8090:num_threads=32"

# ToplingZipTable_XXX can be override here by env, which will
# override the values defined in db Hoster side's json config.
# Hoster side's json config will pass to compact worker through
# rpc, then it may be override by env defined here!
export ToplingZipTable_nltBuildThreads=5
export ToplingZipTable_localTempDir=/dev/shm
export ToplingZipTable_warmupLevel=kValue
export DictZipBlobStore_zipThreads=16
rm -f /tmp/Terark-* # clean garbage files in previous run

ulimit -n 100000
./dcompact_worker.exe -D listening_ports=8080
```

以该脚本为例，该脚本在当前节点上启动 dcompact\_worker，其中几个环境变量说明如下：

环境变量名 | 解释说明
-----------|---------
TERMINATION\_CHECK\_URL|如果机器是竞价实例（Spot Instance），云厂商会提供 HTTP 接口来检查机器是否将要释放，正常状态下该 HTTP 会返回 404，在机器将要释放时，返回其它值（一般是 200）
WORKER\_DB\_ROOT | worker 为每个 hoster 发过来的每个 compact job 的每个 attempt 都会创建一个目录，<br/>该目录中会保存 db 的 pseudo metadata 及运行日志，compact 成功之后该目录中的数据就没有用处了
DEL\_WORKER\_TEMP\_DB| 参考 `WORKER_DB_ROOT`，当相应的 compact 执行完后，是否删除相应的目录，<br/>因为 compact 执行完之后，相应目录中的数据就没有用处了，可以删除，但是为了事后追踪，仍可保留
NFS\_DYNAMIC\_MOUNT | 0 表示系统中已 mount nfs<br/>1 表示系统中未 mount nfs，由 dcompact\_worker 进程动态 mount http 请求中指定的 nfs
NFS\_MOUNT\_ROOT | 此目录下包含多个 Hoster 的 sst 目录，目录名为每个 Hoster 的 `instance_name`。<br/>hoster 发给 worker 的目录都是在 json 中指定的，worker 会将 hoster 发过来的目录前缀中同于<br/> `DcompactEtcd` json 定义中的 `hoster_root` 的部分替换为 `${NFS_MOUNT_ROOT}/instance_name`
MAX\_PARALLEL\_COMPACTIONS | 该 dcompact\_worker 进程可以同时执行多少个 compact 任务，必须大于 0，但不宜设得太大
ADVERTISE\_ADDR | 该参数会通过 dcompact 请求的 response 返回给 Hoster, 从而在 Hoster 的 dcompact worker web view 中展示在链接 url 中，这是为了适配反向代理，使得我们可以在公网环境下查看内网的 dcompact\_worker 的 web view
WEB\_DOMAIN | 用于 dcompact worker web view iframe 的自适应高度
MULTI\_PROCESS | 如果使用 ToplingDB 的程序及其 CompactionFilter/EventHandler 等插件使用了全局变量，就无法在同一个进程中执行来自多个 DB 实例的 Compact 任务，此时，将 MULTI\_PROCESS 设为 true 可以通过多进程的方式运行 Compact
ZIP\_SERVER\_OPTIONS | ToplingZipTable 环境变量，当 MULTI\_PROCESS 为 true 时，设置 ZipServer 的 http 参数，例如：<br/>`export ZIP_SERVER_OPTIONS=listening_ports=8090:num_threads=32`

注意：对于同一个 hoster 实例，该 hoster 上的 db 数据的路径和 worker 上访问该 hoster 的 db 数据的路径一般是不同的。因为两者的 mount path 很难达成一致，所以，worker 上的环境变量 NFS\_MOUNT\_ROOT 和 hoster 上的 json 变量 `hoster_root` 及 `instance_name` 共同协作，完成这个路径的映射关系。

<!--
ETCD 环境变量 | 默认值 | 是否可以为空 | 解释说明（ETCD 相关配置，已经过时无用）
-------------|--------|-------------|--------
ETCD\_LOAD\_BALANCER | round_robin | NO |
ETCD\_URL | 空 | NO |意如其名，不可为空
ETCD\_USERNAME | 空 | YES | 为空时，ETCD\_PASSWORD 被忽略
ETCD\_PASSWORD | 空 | YES |
ETCD\_CA       | 空 | YES | 指定 ca 文件路径，使用 {ca, cert, key} 链接 ETCD
ETCD\_CERT     | 空 | YES | 指定 cert 文件路径，与 ca 一起使用时，也可以为空
ETCD\_KEY      | 空 | YES | 指定 key 文件路径，与 ca 一起使用时，也可以为空
-->

## 3. Serverless 分布式 Compact
分布式 Compact 可以部署在一个弹性伸缩组（可弹性伸缩的集群）中，挂在一个反向代理后面，该反向代理对外暴露 HTTP 服务，从而该 HTTP 服务本质上就是一个 Serverless 服务。

在公有云上，[MyTopling](https://topling.cn/products/mytopling) 和 [Todis](https://topling.cn/products/todis-enterprise) 的分布式 Compact 就是通过这样的 Serverless 服务实现的。

在这种配置中，需要使用 dcompact_worker 的自动 mount 能力，DB 的 instance_name, nfs_mnt_src, nfs_mnt_opt 就是为了实现这个需求，DB 通过 HTTP 请求将包含这些信息的 compact 请求发送给反向代理，反向代理再根据权重等代理策略选择一个后端 dcompact_worker 结点，将请求转发过去，dcompact_worker 根据需要 mount 相应的 NFS，然后执行 compact 任务：从 NFS 上读取输入，将输出写回 NFS。

以这样的方式，如果 DB 的结点或流量有增减，后端 dcompact_worker 的机器负载就会相应增减，进而弹性伸缩组就自动增减结点，将机器负载保持在一个合理范围内，既提供足够的算力，又不浪费资源。

在公有云上，不同账号之间的内网是互相隔离的，要通过内网访问 NFS，就需要将不同账号的内网打通，各个公有云对这个需求都有相应的支持。
