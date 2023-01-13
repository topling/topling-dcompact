# Distributed Compact

In [ToplingDB](https://github.com/topling/toplingdb), dcompact (Distributed Compaction) is formally implemented as a [SidePlugin](https://github.com/topling/rockside/wiki). That is to say, to use distributed compact, the user code does not need any modification, only the json/yaml configuration file needs to be changed.

In the implementation of Distributed Compact:

1. Who runs ToplingDB/RocksDB is called Hoster, as Client in the Server/Client model.
2. Who runs dcompact\_worker is called Worker, as Server in the Server/Client model.
3. One worker can serve multiple hosters at the same time, and one hoster can also send its compact tasks to multiple workers for executing.
4. When compiling ToplingDB, this module (topling-dcompact) is automatically cloned from github by ToplingDB's Makefile.

In addition to the dynamic library of ToplingDB, dcompact\_worker.exe also depends on libcurl. In addition, for user-defined plugins, such as MergeOperator, CompactionFilter, etc., the corresponding dynamic library needs to be loaded through LD\_PRELOAD when running dcompact\_worker. In this way, the same process can load multiple different dynamic libraries to provide compact services for multiple different DBs. For example, [MyTopling](https://github.com/topling/mytopling) and [Todis](https://github.com/topling/todis) use this method to share the same dcompact\_worker process.

> The implementation class name of Distributed Compact is DcompactEtcd, because ToplingDB Distributed Compact originally used etcd through etcd-cpp-api for the interaction between Hoster and Worker. But etcd had to be abandoned because of [bug #78](https://github.com/etcd-cpp-apiv3/etcd-cpp-apiv3/issues/78) of etcd-cpp-api. Currently etcd-related code is useless, only DcompactEtcd is reserved as the class name.

## 1. json configuration

Distributed Compact is configured in the json configuration file:
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

Here, `CompactionExecutorFactory` is a C++ interface, which is a namespace in json. In this namespace, an object whose varname is `dcompact` and class name is `DcompactEtcd` (implements the `CompactionExecutorFactory` interface) is defined, and the object is constructed with `params`. The params are explained as follows:

property name | default value | explanation
-------|------|---
`allow_fallback_to_local`| false | Whether to allow fallback to local compact if distributed compact fails
`hoster_root` | null | The root directory of the db, generally set the same as the `path` variable in DB::Open.
`instance_name` | null | The db instance name, in a multi-tenant scenario, the CompactWorker node uses instance\_name to distinguish different db instances
`nfs_mnt_src`   | null | NFS mount source
`nfs_mnt_opt`   | null | NFS mount options
`http_max_retry` | 3 | Maximum number of retries
`overall_timeout` | 5 | In seconds, the time-out time for a single execution attempt of a single distributed Compact task from start to finish
`http_timeout` | 3 | In seconds, the timeout time of the http connection. Under normal circumstances, timeout means an error
`http_workers` | null | Multiple (at least one) http urls. Those starting with `//` will be skipped, equivalent to being commented out<br/>`//end_http_workers` at the end is for the `start_workers.sh` script and cannot be deleted
`dcompact_min_level` | 2 | Distributed compact is used only when the Compact Output Level is greater than or equal to this value, and local compact is used when it is smaller than this value

### 1.1 http_workers

In the intranet environment, each worker can be simply configured as a string representing the http url of the worker.

In a public network (such as cloud computing) environment, workers are hidden behind reverse proxy or load balancing, and several configuration fields are added for this:

field name | explanation
------|------
url | Submit a compact job
base_url | probe to query compact job or shutdown to specify compact job
web_url | View the status through stat and view the log file in the browser

## 2. dcompact worker

### 2.1 dcompact as a service

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
rm /tmp/Terark-* # clean garbage files in previous run

ulimit -n 100000
./dcompact_worker.exe -D listening_ports=8080
```

Taking this script as an example, the script starts dcompact\_worker on the current node, and several environment variables are described as follows:

环境变量名 | 解释说明
-----------|---------
WORKER\_DB\_ROOT | The worker will create a directory for each attempt of each compact job sent by each hoster,<br/>The pseudo metadata and operation logs of the db will be saved in this directory. After the compact is successful, the data in this directory will be useless
DEL\_WORKER\_TEMP\_DB| Referring to `WORKER_DB_ROOT`, when the corresponding compact is executed, whether to delete the corresponding directory.<br/>While after the execution of compact, the data in the corresponding directory is useless and can be deleted, it can still be retained for subsequent tracking
NFS\_DYNAMIC\_MOUNT | 0 means that the system has mounted nfs<br/>1 means the system has not mounted nfs, and the dcompact\_worker process dynamically mounts the nfs specified in the http request
NFS\_MOUNT\_ROOT | This directory contains multiple Hosters' SST directory, and the directory name is the Instance\_name of each host.<br/>The directory sent by hostter to worker is specified in JSON. The part of the `hoster_root` in the DCOMPACTETCD JSON definition is replaced by `${nfs_mount_root}/instance_name`
MAX\_PARALLEL\_COMPACTIONS | The number of compact tasks that the dcompact\_worker process can execute at the same time. It must be greater than 0, but should not be set too large
ADVERTISE\_ADDR | This parameter will be returned to the Hoster through the response of the dcompact request, so that it will be displayed in the link url in the dcompact worker web view of the Hoster, to adapt to the reverse proxy, so that we can view the dcompact\_worker of the intranet in the public network environment web view
WEB\_DOMAIN | Adaptive height for dcompact worker web view iframe
MULTI\_PROCESS | If the program using ToplingDB and its plug-ins such as CompactionFilter/EventHandler use global variables, it is impossible to execute Compact tasks from multiple DB instances in the same process. At this time, set MULTI_PROCESS to true to run Compact through multiple processes.
ZIP\_SERVER\_OPTIONS | ToplingZipTable environment variable. when MULTI_PROCESS is true, it set the http parameters of ZipServer. For example:<br/>`export ZIP_SERVER_OPTIONS=listening_ports=8090:num_threads=32`

Note: For the same hoster instance, the path of the db data on the hoster and the path of accessing the db data of the hoster on the worker are generally different. Because it is difficult to agree on the mount path between the two, the environment variable NFS_MOUNT_ROOT on the worker and the json variables `hoster_root` and `instance_name` on the hoster work together to complete the mapping relationship of this path.

## 3. Serverless Distributed Compact

Distributed Compact can be deployed in an auto-scaling group (an auto-scaling cluster) and hung behind a reverse proxy that exposes the HTTP service to the outside, so that the HTTP service is essentially a serverless service.

On the public cloud, the distributed compact of [MyTopling](https://topling.cn/products/mytopling) and [Todis](https://topling.cn/products/todis-enterprise) is realized through such serverless services.

In this configuration, the automatic mount capability of dcompact\_worker needs to be used. The instance\_name, nfs\_mnt\_src, and nfs\_mnt\_opt of DB are used to realize this requirement. DB sends the compact request containing these information to the reverse proxy through HTTP request, and the reverse proxy then uses the weight Wait for the proxy strategy to select a back-end dcompact\_worker node, and forward the request. dcompact\_worker mounts the corresponding NFS as needed, and then executes the compact task: reading input from NFS, and writing output back to NFS.

In this way, if the number of DB nodes or traffic increases or decreases, the machine load of the backend dcompact\_worker will increase or decrease accordingly, and then the auto-scaling group will automatically increase or decrease nodes to keep the machine load within a reasonable range. Provide enough computing power without wasting resources.

On the public cloud, the intranets of different accounts are isolated from each other. To access NFS through the intranet, it is necessary to connect the intranets of different accounts. Each public cloud has corresponding support for this requirement.