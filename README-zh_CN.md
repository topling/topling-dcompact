# 分布式 Compact

在 ToplingDB 中，dcompact(Distributed Compaction) 在形式上是作为一个 SidePlugin 实现的，也就是说，要使用分布式 Compact，用户代码不需要任何修改，只需要改 json/yaml 配置文件。

在分布式 Compact 的实现中：

1. 运行 ToplingDB/RocksDB 的一方称为 Hoster，在 Server/Client 模型中，是 Client
2. 运行 dcompact\_worker 的一方称为 Worker，在 Server/Client 模型中，是 Server
3. 同一个 worker 可以同时为多个 hoster 服务，同一个 hoster 也可以把自己的 compact 任务发给多个 worker 执行

除了 ToplingDB 的动态库之外，dcompact\_worker.exe 还依赖 libcurl 及 libetcd-cpp-api，在此之外，对于用户自定义的插件，例如 MergeOperator, CompactionFilter 等，运行 dcompact\_worker 时需要通过 LD\_PRELOAD 加载相应的动态库。


## 1. json 配置

分布式 Compact 在 json 配置文件中进行设置：
```json
  "CompactionExecutorFactory": {
    "dcompact": {
      "class": "DcompactEtcd",
      "params": {
        "etcd": {
          "url": "etcd-host:2379",
          "//ca": "root CA file for SSL/TLS, can not be empty, if ca is not present, try username/password",
          "//cert": "cert chain file for SSL/TLS, can be empty",
          "//key": "private key file for SSL/TLS, can be empty",
          "//username": "leipeng",
          "//password": "topling@123",
          "load_balancer": "round_robin"
        },
        "etcd_root": "/dcompact-worker-cluster-1",

        "hoster_root": "/nvmepool/shared/db1",
        "instance_name": "db1",
        "nfs_mnt_flags": "noatime,rdma",
        "nfs_mnt_src": ":/nvmepool/shared",
        "nfs_mnt_opt": "nolock,addr=192.168.31.16",

        "http_max_retry": 999,
        "http_timeout": 30,
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

在这里，`CompactionExecutorFactory` 是 C++ 的接口，在 json 中是一个 namespace，在这个 namespace 中，定义了一个 varname 为 `dcompact`，类名为 `DcompactEtcd` 的（实现了 `CompactionExecutorFactory` 接口）的对象，该对象使用 `params` 进行构造。params 解释说明如下：

属性名 | 解释说明
-------|----------
etcd   | etcd 的连接选项，默认无认证，需要认证的话，有两种方式：<br/>  1. 使用 username + password 认证<br/>  2. 使用 ca + cert + key 认证，其中 cert 和 key 可以为空，ca 不能为空<br>认证时首选 ca，如果 ca 非空，就用 ca 认证，ca 为空但 username 非空时，使用 username + password 认证
`etcd_root` | 该 `dcompact` 的所有数据都保存在 `etcd_root` 之下
`hoster_root` | 该 db 的根目录，一般设置为与 DB::Open 中的 `path` 变量相同。
`instance_name` | 该 db 实例名，在多租户场景下，CompactWorker 结点使用 instance\_name 区分不同的 db 实例
`nfs_mnt_flags` | 保留
`nfs_mnt_src`   | 保留
`nfs_mnt_opt`   | 保留
`http_max_retry` | 最大重试次数
`http_timeout` | http 连接的超时时间，一般情况下，超时即意味着出错
`http_workers` | 多个 http url, 以 `//` 开头的会被跳过，相当于是被注释掉了<br/> 末尾的 `//end_http_workers` 是为了 `start_workers.sh` 脚本服务的，不能删除
`dcompact_min_level` | 只有在 Compact Output Level 大于等于该值时，才使用分布式 compact，小于该值时使用本地 compact

### 1.1. http_workers
在内网环境下，每个 worker 可以简单地配置为一个字符串，表示 worker 的 http url。

在公网（例如云计算）环境下，worker 隐藏在反向代理/负载均衡后面，为此增加了几个配置字段：

字段名| 说明
------|------
url | 提交 compact job
base_url | probe 查询 compact job 或 shutdown 指定 compact job
web_url | 在浏览器中通过 stat 查看状态，以及查看 log 文件


## 2. dcompact worker

### 2.1 dcompact 作为服务

当 MAX\_PARALLEL\_COMPACTIONS &gt; 0 时，在此模式下运行。

```bash
. inject-env.sh
export  WORKER_DB_ROOT=/tmp
export  WORKER_DB_ROOT=/dev/shm
rm -rf $WORKER_DB_ROOT/db1/* # db1 is test db instance_name
rm -rf /dev/shm/Terark-*
ulimit -n 100000
nodeset="0 1"

for i in $nodeset; do
    # TerarkZipTable_XXX can be override here by env, which will
    # override the values defined in db Hoster side's json config.
    #
    # Hoster side's json config will pass to compact worker through
    # rpc, then it may be override by env defined here!
    #
    env ETCD_URL=192.168.100.100:2379 \
        NFS_DYNAMIC_MOUNT=0 \
        NFS_MOUNT_ROOT=/nvme-shared \
        MAX_PARALLEL_COMPACTIONS=16 \
        DictZipBlobStore_zipThreads=16 \
        TerarkZipTable_nltBuildThreads=16 \
        TerarkZipTable_localTempDir=/dev/shm \
        TerarkZipTable_warmupLevel=kValue \
        numactl --cpunodebind=$i -- $dbg ./dcompact_worker.exe \
	-D listening_ports=$((8080+i)) &
done
wait
```

以该脚本为例，该脚本在当前节点上启动了两个 dcompact\_worker，每个 numa node 一个，其中几个环境变量说明如下：

环境变量名 | 解释说明
-----------|---------
WORKER\_DB\_ROOT | worker 为每个 hoster 发过来的每个 compact job 的每个 attempt 都会创建一个目录，<br/>该目录中会保存 db 的 pseudo metadata 及运行日志，compact 成功之后该目录中的数据就没有用处了
DEL\_WORKER\_TEMP\_DB| 参考 `WORKER_DB_ROOT`，当相应的 compact 执行完后，是否删除相应的目录，<br/>因为 compact 执行完之后，相应目录中的数据就没有用处了，可以删除，但是为了事后追踪，仍可保留
NFS\_DYNAMIC\_MOUNT | 0 表示系统中已 mount nfs<br/>1 表示系统中未 mount nfs，由 dcompact\_worker 进程动态 mount http 请求中指定的 nfs
NFS\_MOUNT\_ROOT | 此目录下包含多个 Hoster 的 sst 目录，目录名为每个 Hoster 的 `instance_name`。<br/>hoster 发给 worker 的目录都是在 json 中指定的，worker 会将 hoster 发过来的目录前缀中同于<br/> `DcompactEtcd` json 定义中的 `hoster_root` 的部分替换为 `${NFS_MOUNT_ROOT}/instance_name`
MAX\_PARALLEL\_COMPACTIONS | 该 dcompact\_worker 进程可以同时执行多少个 compact 任务，不宜设得太大
ADVERTISE\_ADDR | 该参数会通过 dcompact 请求的 response 返回给 Hoster, 从而在 Hoster 的 dcompact worker web view 中展示在链接 url 中，这是为了适配反向代理，使得我们可以在公网环境下查看内网的 dcompact\_worker 的 web view
WEB\_DOMAIN | 用于 dcompact worker web view iframe 的自适应高度
MULTI\_PROCESS | 如果使用 ToplingDB 的程序及其 CompactionFilter/EventHandler 等插件使用了全局变量，就无法在同一个进程中执行来自多个 DB 实例的 Compact 任务，此时，将 MULTI\_PROCESS 设为 true 可以通过多进程的方式运行 Compact
ZIP\_SERVER\_OPTIONS | 当 MULTI\_PROCESS 为 true 时，设置 ZipServer 的 http 参数，例如：<br/>`export ZIP_SERVER_OPTIONS=listening_ports=8090:num_threads=32`

注意：对于同一个 hoster 实例，该 hoster 上的 db 数据的路径和 worker 上访问该 hoster 的 db 数据的路径一般是不同的。因为两者的 mount path 很难达成一致，所以，worker 上的环境变量 NFS\_MOUNT\_ROOT 和 hoster 上的 json 变量 `hoster_root` 及 `instance_name` 共同协作，完成这个路径的映射关系。

ETCD 环境变量 | 默认值 | 是否可以为空 | 解释说明
-------------|--------|-------------|--------
ETCD\_LOAD\_BALANCER | round_robin | NO |
ETCD\_URL | 空 | NO |意如其名，不可为空
ETCD\_USERNAME | 空 | YES | 为空时，ETCD\_PASSWORD 被忽略
ETCD\_PASSWORD | 空 | YES | 
ETCD\_CA       | 空 | YES | 指定 ca 文件路径，使用 {ca, cert, key} 链接 ETCD
ETCD\_CERT     | 空 | YES | 指定 cert 文件路径，与 ca 一起使用时，也可以为空
ETCD\_KEY      | 空 | YES | 指定 key 文件路径，与 ca 一起使用时，也可以为空


