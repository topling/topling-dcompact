//
// Created by leipeng on 2021/1/21.
//
#include <topling/side_plugin_factory.h>
#include <topling/side_plugin_internal.h>
#include <topling/web/CivetServer.h>
#include <topling/web/json_civetweb.h>
#include <dcompact/dcompact_executor.h>
#include <db/error_handler.h>
#include <logging/logging.h>
#include <options/options_helper.h> // for BuildDBOptions
#include <rocksdb/merge_operator.h>
#include <terark/io/FileStream.hpp>
#include <terark/lru_map.hpp>
#include <terark/num_to_str.hpp>
#include <terark/util/linebuf.hpp>
#include <terark/util/process.hpp>
#include <terark/util/profiling.hpp>
#include <terark/util/refcount.hpp>
#include <terark/util/concurrent_queue.hpp>
#include <terark/circular_queue.hpp>
#include <boost/intrusive_ptr.hpp>
using boost::intrusive_ptr;

#include <getopt.h>
#include <fcntl.h>
#include <sys/mount.h> // for umount()
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/prctl.h>

#include <filesystem>

#ifdef TOPLING_DCOMPACT_USE_ETCD
#undef __declspec // defined in port_posix.h and etcd/Client.h's include
#if defined(__clang__) || defined(__GNUC__) || defined(__GNUG__)
  #pragma GCC diagnostic push
  #pragma GCC diagnostic ignored "-Wnon-virtual-dtor"
  #include <etcd/Client.hpp>
  #pragma GCC diagnostic pop
#else
  #include <etcd/Client.hpp>
#endif
#endif // TOPLING_DCOMPACT_USE_ETCD

static const long LOG_LEVEL = terark::getEnvLong("LOG_LEVEL", 2);
#define PrintLog(level, fmt, ...) \
  do { \
    if (LOG_LEVEL >= level) \
      fprintf(stderr, "%s: %s:%d: " fmt "\n", StrDateTimeNow(), \
              RocksLogShorterFileName(__FILE__), \
              TERARK_PP_SmartForPrintf(__LINE__, ## __VA_ARGS__)); \
    if (info_log) \
      Log(info_log->GetInfoLogLevel(), info_log, "%s:%d: " fmt, \
          RocksLogShorterFileName(__FILE__), \
          TERARK_PP_SmartForPrintf(__LINE__, ##__VA_ARGS__)); \
  } while (0)
#define TRAC(...) PrintLog(4, "TRAC: " __VA_ARGS__)
#define DEBG(...) PrintLog(3, "DEBG: " __VA_ARGS__)
#define INFO(...) PrintLog(2, "INFO: " __VA_ARGS__)
#define WARN(...) PrintLog(1, "WARN: " __VA_ARGS__)
#define ERROR(...) PrintLog(0, "ERROR: " __VA_ARGS__)

#define HttpErr(code, fmt, ...) do { \
  const char* strNow = StrDateTimeNow(); \
mg_printf(conn, "HTTP/1.1 %d\r\nContent-type: text\r\n\r\n%s: " fmt, code, \
          TERARK_PP_SmartForPrintf(strNow, ##__VA_ARGS__)); \
  fprintf(stderr, "ERROR: %s: " fmt "\n", \
          TERARK_PP_SmartForPrintf(strNow, ##__VA_ARGS__)); \
  if (info_log) \
    Error(info_log, "%s" fmt, TERARK_PP_SmartForPrintf("", ##__VA_ARGS__)); \
} while (0)

#define AddFmt(str, ...) do { \
  char buf[128];                 \
  auto len = snprintf(buf, sizeof buf, __VA_ARGS__); \
  str.append(buf, len);          \
} while (0)

#define VERIFY_S_2(expr, job, fmt, ...) \
do { \
  if (terark_unlikely(!(expr))) { \
    if (info_log) \
      ROCKS_LOG_FATAL(info_log, "verify(%s) failed: " fmt, \
                      TERARK_PP_SmartForPrintf(#expr, ##__VA_ARGS__)); \
    std::string errmsg(32*1024, '\0'); \
    errmsg.resize(snprintf(&errmsg[0], errmsg.size(), \
            "verify(%s) failed: " fmt, \
            TERARK_PP_SmartForPrintf(#expr, ##__VA_ARGS__))); \
    job->results->status = Status::Corruption(errmsg); \
    SerDeWrite(out, results); fclose(out); fclose(in); \
    info_log->Close(); \
    job->NotifyEtcd(); \
    const char* strNow = StrDateTimeNow(); \
    TERARK_DIE("%s: verify(%s) failed: " fmt, strNow, \
               TERARK_PP_SmartForPrintf(#expr, ##__VA_ARGS__)); \
  } \
} while (0)

#define VERIFY_S(expr, ...) VERIFY_S_2(expr, this, __VA_ARGS__)

#define VERIFY_EQ(x, y) VERIFY_S(x == y, "%lld %lld", llong(x), llong(y))
#define VERIFY_S_EQ(x, y) VERIFY_S(x == y, "%s %s", x, y)

extern const char* rocksdb_build_git_sha;

namespace ROCKSDB_NAMESPACE {

using namespace std;
using namespace terark;

profiling pf;
extern json from_query_string(const char* qry);
extern void mg_print_cur_time(mg_connection *conn);
extern std::string cur_time_stat();
std::string ReadPostData(mg_connection* conn);
__attribute__((weak)) json JS_TopZipTable_Global_Stat(bool html);
__attribute__((weak)) json JS_TopZipTable_Global_Env();
extern const char* StrDateTimeNow(); // in builtin_table_factory.cc

static SidePluginRepo repo; // empty repo

template<class Ptr>
void CompactionParams_SetPtr(const CompactionParams& params,
                             const ObjectRpcParam& rpc, const Ptr& p) {
  // do nothing
}
void CompactionParams_SetPtr(const CompactionParams& params,
                             const ObjectRpcParam& rpc,
                             const std::shared_ptr<AnyPlugin>& p) {
  // now just html_user_key_coder
  TERARK_VERIFY_EQ(&rpc, &params.html_user_key_coder);
  if (p) {
    params.p_html_user_key_coder = dynamic_cast<UserKeyCoder*>(p.get());
    TERARK_VERIFY(nullptr != params.p_html_user_key_coder);
  }
}

///@param fp, rpc are passed as ObjectRpcParam::serde(fp, rpc)
///@param ptr, params are bound to lambda
template<class Ptr>
void CreatePluginTpl(FILE* fp, const ObjectRpcParam& rpc,
                     Ptr& ptr, const CompactionParams& params) {
  TERARK_VERIFY(GetRawPtr(ptr) == nullptr);
  if (rpc.clazz.empty()) {
    return;  // not defined
  }
  json spec;
  if (!rpc.params.empty()) {
#if defined(NDEBUG)
    Logger* info_log = params.info_log;
    try {
      spec = json::parse(rpc.params);
    }
    catch (const std::exception& ex) {
      ERROR("%s: exception: %s", ROCKSDB_FUNC, ex);
      THROW_Corruption(ex.what());
    }
#else
    spec = json::parse(rpc.params);
#endif
  }
  ptr = PluginFactory<Ptr>::AcquirePlugin(rpc.clazz, spec, repo);
  TERARK_VERIFY(GetRawPtr(ptr) != nullptr);
  CompactionParams_SetPtr(params, rpc, ptr);
  auto sdjs = JS_CompactionParamsEncodePtr(&params);
  auto serde = SerDeFac(&*ptr)->NullablePlugin(rpc.clazz, sdjs, repo);
  if (serde)
    serde->DeSerialize(fp, dest_ccast(&*ptr));
}

template<class Ptr>
ObjectRpcParam::serde_fn_t
Bind_CreatePluginTpl(Ptr& ptr, const CompactionParams& params) {
  return [&](FILE* fp, const ObjectRpcParam& rpc) {
     CreatePluginTpl(fp, rpc, ptr, params);
  };
}

static std::atomic_long g_jobsAccepting{0};
static std::atomic_long g_jobsRunning{0};
static std::atomic_long g_jobsFinished{0};
static std::atomic_long g_jobsRejected{0};
static std::atomic_long g_jobsPreFailed{0};
#ifdef TOPLING_DCOMPACT_USE_ETCD
static std::atomic_long g_etcd_err{0};
#endif

// CHECK_CODE_REVISION - 0: do not check
//                       1: check rocksdb release version
//                       2: check git commit hash
static const long CHECK_CODE_REVISION = getEnvLong("CHECK_CODE_REVISION", 1);

static const bool NFS_DYNAMIC_MOUNT = getEnvBool("NFS_DYNAMIC_MOUNT", false);
static const long MAX_PARALLEL_COMPACTIONS = getEnvLong("MAX_PARALLEL_COMPACTIONS", 0);
static const long MAX_WAITING_COMPACTIONS = getEnvLong("MAX_WAITING_COMPACTIONS",
                                     std::thread::hardware_concurrency()*2);
static const string WORKER_DB_ROOT = GetDirFromEnv("WORKER_DB_ROOT", "/tmp"); // NOLINT
static const string NFS_MOUNT_ROOT = GetDirFromEnv("NFS_MOUNT_ROOT", "/mnt/nfs");
static const string ADVERTISE_ADDR = getEnvStr("ADVERTISE_ADDR", "self");
static const char* WEB_DOMAIN = getenv("WEB_DOMAIN");
static const bool MULTI_PROCESS = getEnvBool("MULTI_PROCESS", false);

int mount_nfs(const DcompactMeta& meta, mg_connection* conn, Logger* info_log) {
  const string& source = meta.nfs_mnt_src;
  const string  target = MakePath(NFS_MOUNT_ROOT, meta.instance_name);
  if (mkdir(target.c_str(), 0777) < 0) {
    int err = errno;
    if (EEXIST != err) {
      HttpErr(412, "job-%05d/att-%02d: mount prepare mkdir(%s, 0777) = %s",
              meta.job_id, meta.attempt, target, strerror(err));
      return err;
    }
  }
  std::string nfs_type = meta.nfs_type.empty() ? "nfs" : meta.nfs_type;
  string_appender<> cmd;
  cmd|"mount -t "|nfs_type|" -o "|meta.nfs_mnt_opt|" "|source|" "|target;
  ProcPipeStream proc(cmd, "r2");
  LineBuf cmd_output(proc);
  int err = proc.xclose();
  if (err) {
    HttpErr(412, "job-%05d/att-%02d: cmd = (%s) failed = %d(%#X) : %s",
            meta.job_id, meta.attempt, cmd, err, err, cmd_output);
    return err;
  }
  return 0;
}

struct MountEntry {
  std::string start_time;
  int job_id = -1;
  int attempt = -1;
};

using LruMountMapBase = lru_hash_strmap<MountEntry>;
class LruMountMap : public LruMountMapBase {
public:
  using LruMountMapBase::LruMountMapBase;
  std::pair<size_t, int>
  ensure_mnt(const DcompactMeta& meta, mg_connection* conn, Logger* info_log) {
    int err = 0;
    auto solve = [&](fstring /*instance_name*/, MountEntry* me) {
      me->start_time = meta.start_time;
      me->job_id = meta.job_id;
      me->attempt = meta.attempt;
      err = mount_nfs(meta, conn, info_log);
      return 0 == err;
    };
    auto ib = this->lru_add(meta.instance_name, solve);
    return std::make_pair(ib.first, err);
  }
 protected:
  void lru_evict(const fstring& key, MountEntry* me) override {
    Slice instance_name(key.p, key.n);
    string mnt_dir = MakePath(NFS_MOUNT_ROOT, instance_name);
    if (umount(mnt_dir.c_str()) < 0) {
      Logger* info_log = nullptr;
      ERROR("umount(%s) = %m", mnt_dir);
    }
  }
};
static LruMountMap g_mnt_map(32); // NOLINT

#ifdef TOPLING_DCOMPACT_USE_ETCD
static etcd::Client* g_etcd = nullptr;
#endif

static volatile bool           g_stop = false;
static std::mutex              g_stop_mtx;
static std::condition_variable g_stop_cond;

using terark::util::concurrent_queue;
struct QueueItem {
  std::function<void()> func;
  uint32_t n_subcompacts;
};
static concurrent_queue<circular_queue<QueueItem> > g_workQueue;
static std::atomic<size_t> g_jobsWaiting{0};

static string_appender<> BuildMetaKey(const DcompactMeta& meta) {
  string_appender<> str;
  str.reserve(meta.instance_name.size() + meta.start_time.size() +
              meta.dbname.size() + 10 + 7 + 8);
  str|meta.instance_name|"/"|meta.start_time|"/"|meta.dbname;
  str^"/job-%05d"^meta.job_id^"/att-%02d"^meta.attempt;
  return str;
}

class Job;
class AcceptedJobsMap {
  hash_strmap<Job*> map;
  mutable std::mutex mtx;
public:
  hash_strmap<Job*>& get_map() { return map; }
  std::mutex& get_mtx() { return mtx; }
  AcceptedJobsMap() { map.enable_freelist(4096); }
  std::pair<size_t, bool> add(Job*) noexcept;
  intrusive_ptr<Job> find(const DcompactMeta& key) const noexcept;
  void del(Job*) noexcept;
  size_t peekSize() const noexcept { return map.size(); }
};
static AcceptedJobsMap g_acceptedJobs;
static void work_thread_func() {
  while (!g_stop || g_acceptedJobs.peekSize() > 0) {
    auto task = g_workQueue.pop_front_if([]{
      auto running = g_jobsRunning.load(std::memory_order_relaxed);
      if (0 == running) {
        // if there is no running jobs, always return true, because
        // n_subcompacts maybe larger than MAX_PARALLEL_COMPACTIONS,
        // we allow over load on this scenario.
        return true;
      } else {
        auto& front = g_workQueue.queue().front();
        return running + front.n_subcompacts <= MAX_PARALLEL_COMPACTIONS;
      }
    });
    task.func();
  }
}

#if defined(_MSC_VER)
static std::string html_user_key_decode(const CompactionParams&, Slice uk) {
  return uk.ToString(true);
}
#else
std::string __attribute__((weak))
CompactionParams_html_user_key_decode(const CompactionParams&, Slice);
static std::string html_user_key_decode(const CompactionParams& cp, Slice uk) {
  if (CompactionParams_html_user_key_decode)
    return CompactionParams_html_user_key_decode(cp, uk);
  else
    return uk.ToString(true);
}
#endif

class Job : public RefCounter {
std::atomic<bool> shutting_down{false};
mutable bool m_shutdown_files_cleaned = false;
public:
void ShutDown() {
  shutting_down.store(true, std::memory_order_release);
  if (MULTI_PROCESS) {
    size_t n_retry = 0;
    while (child_pid < 0) {
      auto info_log = m_log.get();
      if (++n_retry >= 100) {
        WARN("%s: ShutDown: wait child forking child process timeout", attempt_dbname);
        return;
      }
      INFO("%s: ShutDown: wait for forking child process", attempt_dbname);
      usleep(100000); // 100ms
    }
    if (!process_mem_write(child_pid, &shutting_down, sizeof(shutting_down))) {
      auto info_log = m_log.get();
      WARN("%s: ShutDown: process_mem_write = %m", attempt_dbname);
    }
    usleep(1000000); // 1 second
    ::kill(child_pid, SIGKILL);
  }
}

// used for mapping hoster node dir to worker node dir
const string g_worker_root;
const DcompactMeta m_meta;

std::shared_ptr<Logger> m_log;
Env* env = Env::Default();
CompactionResults* results;
std::atomic<pid_t> child_pid{-1};
long long accept_time = 0;
long long start_run_time = 0;
mutable size_t inputBytes[2] = {0,0};

// NOT for SST file, but for MANIFEST, info log, ...
string job_dbname[5];
const string& attempt_dbname = job_dbname[4];
public:
~Job() override {
  if (shutting_down.load(std::memory_order_relaxed)) {
    ShutDownCleanFiles();
  }
  Status s = m_log->Close();
  TERARK_VERIFY_S(s.ok(), "%s: m_log->Close() = %s", attempt_dbname, s.ToString());
  delete results;
}

explicit Job(const DcompactMeta& meta)
 : g_worker_root(MakePath(NFS_MOUNT_ROOT, meta.instance_name))
 , m_meta(meta)
{
  results = new CompactionResults();
  job_dbname[0] = MakePath(WORKER_DB_ROOT, m_meta.instance_name);
  job_dbname[1] = MakePath(job_dbname[0], m_meta.start_time);
  job_dbname[2] = MakePath(job_dbname[1], m_meta.dbname);
  job_dbname[3] = CatJobID(job_dbname[2], m_meta.job_id);
  job_dbname[4] = CatAttempt(job_dbname[3], m_meta.attempt);
  for (const string& subdir : job_dbname) {
    Status s = env->CreateDirIfMissing(subdir);
    TERARK_VERIFY_S(s.ok(), "CreateDirIfMissing(%s) = %s", subdir, s.ToString());
  }
  CreateLogger("LOG");
}

void CreateLogger(const char* basename) {
  std::string log_fname = MakePath(attempt_dbname, basename);
  Status s = env->NewLogger(log_fname, &m_log);
  TERARK_VERIFY_S(s.ok(), "NewLogger(%s) = %s", log_fname, s.ToString());
  m_log->SetInfoLogLevel(INFO_LEVEL);
}

string GetWorkerNodePath(const string& hostNodePath) const {
  string res;
  if (!ReplacePrefix(m_meta.hoster_root, g_worker_root, hostNodePath, &res)) {
    auto info_log = m_log.get();
    INFO("hostNodePath = '%s' does not start with hoster_root='%s'",
          hostNodePath.c_str(), m_meta.hoster_root.c_str());
    res = "?/" + hostNodePath;
  }
  return res;
}
string GetHosterNodePath(const string& workerNodePath) const {
  string res;
  if (ReplacePrefix(g_worker_root, m_meta.hoster_root, workerNodePath, &res)) {
    return res;
  }
  TERARK_DIE("workerNodePath = '%s' does not start with WORKER_ROOT='%s'",
            workerNodePath.c_str(), g_worker_root.c_str());
}

public:
void ShowCompactionParams(const CompactionParams& p, Version* const v,
                          ColumnFamilyData* const cfd, const std::string* t0,
                          const std::string* t1 = nullptr,
                          const double dur = -1.0) const {
  const string& attempt_dbname = job_dbname[4];
  std::string summary_fname = attempt_dbname + "/summary.html";
  TableProperties agg;
  std::string inputs = Json_DB_CF_SST_HtmlTable(v, cfd, &agg);
  json time;
  if (t1 != nullptr) {
    summary_fname = attempt_dbname + "/summary-done.html";
    time["link"] = "<a href='summary.html'>on compact start</a>";
    time["time"]["start"] = *t0;
    time["time"]["end"] = *t1;
    time["time"]["duration"] = std::to_string(dur) + " sec";
    time["time"]["speed"] = SizeToString((agg.raw_key_size + agg.raw_value_size)/dur) + " / sec";
  } else {
    time["link"] = "<a href='summary-done.html'>on compact done</a>";
    time["time"]["start"] = *t0;
    time["time"]["end"] = "unfinished";
    time["time"]["duration"] = "unfinished";
  }

  json js;
  js["overview"]["job_id"] = p.job_id;
  js["overview"]["num_levels"] = p.num_levels;
  js["overview"]["output_level"] = p.output_level;
  js["overview"]["target_file_size"] = SizeToString(p.target_file_size);
  js["overview"]["hoster_root"] = p.hoster_root;
  js["overview"]["instance_name"] = p.instance_name;
//js["overview"]["compression_type"] = enum_stdstr(p.compression);

  js["db"]["dbname"] = p.dbname;
  js["db"]["db_id"] = p.db_id;
  js["db"]["db_session_id"] = p.db_session_id;

  js["cf"]["cf_id"] = p.cf_id;
  js["cf"]["cf_name"] = p.cf_name;
  if (p.cf_paths.empty()) {
    js["cf"]["cf_paths"] = "";
  } else {
    for (size_t i = 0; i < p.cf_paths.size(); ++i) {
      json tmp;
      tmp["path"] = p.cf_paths[i].path;
      tmp["target_size"] = SizeToString(p.cf_paths[i].target_size);
      js["cf"]["cf_paths"].push_back(tmp);
    }
  }
  js["inputs"] = std::move(inputs);

  if (p.grandparents == nullptr || p.grandparents->empty()) {
    js["grand<br/>parents"] = "";
  } else {
    terark::string_appender<> gphtml;
    gphtml|R"(<style>
  .right {
    white-space:nowrap;
    text-align:right;
    font-family:monospace;
  }
  .left {
    white-space: nowrap;
    text-align: left;
    font-family: monospace;
  }
  .center {
    white-space: nowrap;
    text-align: center;
    font-family: monospace;
  }
  </style>)";
    gphtml|"<table border=1>";
    gphtml|"<tr>";
    gphtml|"<th rowspan=2>Name</th>";
    gphtml|"<th rowspan=2>Smallest<br/>SeqNum</th>";
    gphtml|"<th rowspan=2>Largest<br/>SeqNum</th>";
    // gphtml|"<th rowspan=2>PathId</th>";
    gphtml|"<th rowspan=2>FileSize<br/>(GB)</th>";
    gphtml|"<th rowspan=2>Compensated<br/>FileSize(GB)</th>";
    gphtml|"<th colspan=3>Smallest</th>";
    gphtml|"<th colspan=3>Largest</th>";
    gphtml|"<th colspan=2>Entries</th>";
    gphtml|"<th colspan=2>Raw(GB)</th>";
//  gphtml|"<th rowspan=2>being_compacted</th>";
//  gphtml|"<th rowspan=2>init_stats_from_file</th>";
//  gphtml|"<th rowspan=2>marked_compaction</th>";
//  gphtml|"<th rowspan=2>oldest_blob_file_num</th>";
//  gphtml|"<th rowspan=2>oldest_ancester_time</th>";
    gphtml|"<th rowspan=2>Creation<br/>time</th>";
//  gphtml|"<th rowspan=2>NumReads<br/>Sampled</th>";
    gphtml|"</tr>";

    gphtml|"<tr>";

    gphtml|"<th>UserKey</th>";
    gphtml|"<th>Seq</th>";
    gphtml|"<th>Type</th>";

    gphtml|"<th>UserKey</th>";
    gphtml|"<th>Seq</th>";
    gphtml|"<th>Type</th>";

    gphtml|"<th>ALL</th>";
    gphtml|"<th>D</th>";

    gphtml|"<th>K</th>";
    gphtml|"<th>V</th>";

    gphtml|"</tr>";

    for (size_t i = 0; i < p.grandparents->size(); ++i) {
      FileMetaData* fmd = p.grandparents->at(i);

      gphtml|"<tr>";
      gphtml^"<td class='right'>%06d"^fmd->fd.GetNumber()^"</td>";
      gphtml|"<td class='right'>"|fmd->fd.smallest_seqno|"</td>";
      gphtml|"<td class='right'>"|fmd->fd.largest_seqno|"</td>";
   // gphtml|"<td class='right'>"|fmd->fd.GetPathId()|"</td>";
      gphtml^"<td class='right'>%.6f"^fmd->fd.GetFileSize()/1e9^"</td>";
      gphtml^"<td class='right'>%.6f"^fmd->compensated_file_size/1e9^"</td>";

      ParsedInternalKey smallest, largest;
      ParseInternalKey(*fmd->smallest.rep(), &smallest, false);
      ParseInternalKey(*fmd->largest.rep(), &largest, false);
      gphtml|"<td class='left'>"|html_user_key_decode(p, smallest.user_key)|"</td>";
      gphtml|"<td class='right'>"|smallest.sequence|"</td>";
      gphtml|"<td class='center'>"|smallest.type|"</td>";
      gphtml|"<td class='left'>"|html_user_key_decode(p, largest.user_key)|"</td>";
      gphtml|"<td class='right'>"|largest.sequence|"</td>";
      gphtml|"<td class='center'>"|largest.type|"</td>";

      gphtml|"<td class='right'>"|fmd->num_entries  |"</td>";
      gphtml|"<td class='right'>"|fmd->num_deletions|"</td>";
      gphtml^"<td class='right'>%.6f"^fmd->raw_key_size/1e9^"</td>";
      gphtml^"<td class='right'>%.6f"^fmd->raw_value_size/1e9^"</td>";
  //  gphtml|"<td class='right'>"|fmd->being_compacted        |"</td>";
  //  gphtml|"<td class='right'>"|fmd->init_stats_from_file   |"</td>";
  //  gphtml|"<td class='right'>"|fmd->marked_for_compaction  |"</td>";
  //  gphtml|"<td class='right'>"|fmd->oldest_blob_file_number|"</td>";
  //  gphtml|"<td class='right'>"|fmd->oldest_ancester_time   |"</td>";
      if (fmd->file_creation_time != 0) {
        time_t rawtime = (time_t)fmd->file_creation_time;
        struct tm* timeinfo;
        timeinfo = localtime(&rawtime);
        char buffer[32] = {0};
        auto len = strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", timeinfo);
        gphtml|"<td class='left'>"|fstring(buffer, len)|"</td>";
      } else {
        gphtml|"<td class='center'>0</td>";
      }
  //  gphtml|"<td class='right'>"|fmd->stats.num_reads_sampled.load()|"</td>";
      gphtml|"</tr>";
    }
    gphtml|"</table>";
    js["grand<br/>parents"] = std::move(gphtml);
  }

  js["compaction"]["compaction_reason"] = enum_stdstr(p.compaction_reason);
  js["compaction"]["compaction_log_level"] = enum_stdstr(p.compaction_log_level);
  js["compaction"]["max_compaction_bytes"] = p.max_compaction_bytes;
  js["compaction"]["max_subcompactions"] = p.max_subcompactions;
  js["compaction"]["manual_compaction"] = p.manual_compaction;
  js["compaction"]["deletion_compaction"] = p.deletion_compaction;
//js["compaction"]["compression_opts"]["kDefaultCompressionLevel"] =
//    p.compression_opts.kDefaultCompressionLevel;
  js["compaction"]["compression_opts"]["window_bits"] =
      p.compression_opts.window_bits;
  js["compaction"]["compression_opts"]["level"] = p.compression_opts.level;
  js["compaction"]["compression_opts"]["strategy"] =
      p.compression_opts.strategy;
  js["compaction"]["compression_opts"]["max_dict_bytes"] =
      p.compression_opts.max_dict_bytes;
  js["compaction"]["compression_opts"]["zstd_max_train_bytes"] =
      p.compression_opts.zstd_max_train_bytes;
  js["compaction"]["compression_opts"]["parallel_threads"] =
      p.compression_opts.parallel_threads;
  js["compaction"]["compression_opts"]["enabled"] = p.compression_opts.enabled;
  js["compaction"]["compression_opts"]["max_dict_buffer_bytes"] =
      p.compression_opts.max_dict_buffer_bytes;

  size_t jcp_row = 1 + 7 +
                   (p.table_properties_collector_factories.empty()
                        ? 1
                        : p.table_properties_collector_factories.size());
  json* jcp = new json[jcp_row];
  TERARK_SCOPE_EXIT(delete[] jcp);
  jcp[1]["name"] = "compaction_filter_factory";
  jcp[1]["clazz"] = p.compaction_filter_factory.clazz;
  jcp[1]["params"] = p.compaction_filter_factory.params;
  jcp[2]["name"] = "merge_operator";
  jcp[2]["clazz"] = p.merge_operator.clazz;
  jcp[2]["params"] = p.merge_operator.params;
  jcp[3]["name"] = "user_comparator";
  jcp[3]["clazz"] = p.user_comparator.clazz;
  jcp[3]["params"] = p.user_comparator.params;
  jcp[4]["name"] = "table_factory";
  jcp[4]["clazz"] = p.table_factory.clazz;
  jcp[4]["params"] = p.table_factory.params;
  jcp[5]["name"] = "prefix_extractor";
  jcp[5]["clazz"] = p.prefix_extractor.clazz;
  jcp[5]["params"] = p.prefix_extractor.params;
  jcp[6]["name"] = "sst_partitioner_factory";
  jcp[6]["clazz"] = p.sst_partitioner_factory.clazz;
  jcp[6]["params"] = p.sst_partitioner_factory.params;
  jcp[7]["name"] = "html_user_key_coder";
  jcp[7]["clazz"] = p.html_user_key_coder.clazz;
  jcp[7]["params"] = p.html_user_key_coder.params;
  if (p.table_properties_collector_factories.empty()) {
    jcp[jcp_row - 1]["name"] = "table_properties_collector_factories";
    jcp[jcp_row - 1]["clazz"] = "";
    jcp[jcp_row - 1]["params"] = "";
  } else {
    for (size_t i = 0; i + 8 < jcp_row; ++i) {
      jcp[i + 8]["name"] =
          "table_properties_collector_factories[" + std::to_string(i) + "]";
      jcp[i + 8]["clazz"] = p.table_properties_collector_factories[i].clazz;
      jcp[i + 8]["params"] = p.table_properties_collector_factories[i].params;
    }
  }
  for (size_t i = 1; i < jcp_row; ++i) {
    jcp[0].push_back(std::move(jcp[i]));
  }
  json& jtabcols = jcp[0][0]["<htmltab:col>"];
  jtabcols.push_back("name");
  jtabcols.push_back("clazz");
  jtabcols.push_back("params");
  js["Object<br/>RpcParam"] = std::move(jcp[0]);

  if (p.info_log == nullptr) {
    js["info_log"] = "";
  } else {
    js["info_log"]["LogFileSize"] = SizeToString(p.info_log->GetLogFileSize());
    js["info_log"]["InfoLogLevel"] = enum_stdstr(p.info_log->GetInfoLogLevel());
  }

  js["version<br/>set"]["last_sequence"] = p.version_set.last_sequence;
  js["version<br/>set"]["last_allocated_sequence"] =
      p.version_set.last_allocated_sequence;
  js["version<br/>set"]["last_published_sequence"] =
      p.version_set.last_published_sequence;
  js["version<br/>set"]["next_file_number"] = p.version_set.next_file_number;
 #if ROCKSDB_MAJOR < 7
  js["version<br/>set"]["min_log_number_to_keep_2pc"] =
      p.version_set.min_log_number_to_keep_2pc;
 #else
  js["version<br/>set"]["min_log_number_to_keep"] =
      p.version_set.min_log_number_to_keep;
 #endif
  js["version<br/>set"]["manifest_file_number"] =
      p.version_set.manifest_file_number;
  js["version<br/>set"]["options_file_number"] = p.version_set.options_file_number;
  js["version<br/>set"]["prev_log_number"] = p.version_set.prev_log_number;
  js["version<br/>set"]["current_version_number"] = p.version_set.current_version_number;

  js["Others"]["allow_ingest_behind"] = p.allow_ingest_behind;
  js["Others"]["preserve_deletes"] = p.preserve_deletes;
  js["Others"]["bottommost_level"] = p.bottommost_level;
  js["Others"]["is_deserialized"] = p.is_deserialized;
  js["Others"]["score"] = p.score;
  js["Others"]["preserve_deletes_seqnum"] = p.preserve_deletes_seqnum;
  js["Others"]["smallest_seqno"] = p.smallest_seqno;
  js["Others"]["earliest_write_conflict_snapshot"] =
      p.earliest_write_conflict_snapshot;
  js["Others"]["paranoid_file_checks"] = p.paranoid_file_checks;
  js["Others"]["full_history_ts_low"] = p.full_history_ts_low;
  js["Others"]["smallest_user_key"] = "<span style='font-family:monospace;white-space:nowrap;'>" + html_user_key_decode(p, p.smallest_user_key) + "</span>";
  js["Others"]["largest_user_key"] = "<span style='font-family:monospace;white-space:nowrap;'>" + html_user_key_decode(p, p.largest_user_key) + "</span>";
  if (p.existing_snapshots->empty()) {
    js["Others"]["existing_snapshots"] = "";
  } else {
    for (size_t i = 0; i < p.existing_snapshots->size(); ++i) {
      js["Others"]["existing_snapshots"].push_back(p.existing_snapshots->at(i));
    }
  }
  if (p.extra_serde_files.empty()) {
    js["Others"]["extra_serde_files"] = "";
  } else {
    for (size_t i = 0; i < p.extra_serde_files.size(); ++i) {
      js["Others"]["extra_serde_files"].push_back(p.extra_serde_files[i]);
    }
  }
  if (p.shutting_down) {
    js["Others"]["shutting_down"] = p.shutting_down->load();
  } else {
    js["Others"]["shutting_down"] = "";
  }

  json dump_options;
  dump_options["html"] = "1";
  std::string html =
      JsonToString(time, dump_options) + JsonToString(js, dump_options);

  FILE* summary_file = fopen(summary_fname.c_str(), "w");
  fwrite(html.c_str(), 1, html.size(), summary_file);
  fclose(summary_file);
}
int RunCompact(FILE* in, FILE* out) const {
  const string worker_dir = GetWorkerNodePath(m_meta.output_root);
  const string output_dir = CatJobID(worker_dir, m_meta.job_id);
  const string attempt_dir = CatAttempt(output_dir, m_meta);
  FSDirectory* null_dbdir = nullptr;
  InstrumentedMutex mutex;
  bool mutex_locked = false; // to be exception-safe
  TERARK_SCOPE_EXIT(if (mutex_locked) mutex.Unlock());
#define MutexLock()   mutex.  Lock(), mutex_locked = true
#define MutexUnlock() mutex.Unlock(), mutex_locked = false
  MutexLock();
  auto t0 = pf.now();
  Logger* info_log = m_log.get();
  CompactionParams  params;
  params.info_log = info_log;
  EnvOptions env_options;
  // env_options.use_mmap_reads = true; // not needed any more
  env_options.allow_fdatasync = false;
  env_options.allow_fallocate = false;
  FileOptions file_options(env_options);
  shared_ptr<FileSystem> fs = env->GetFileSystem();
  shared_ptr<Cache> table_cache = NewLRUCache(50000, 16);
  WriteController write_controller;
  WriteBufferManager write_buffer_manager(8<<20); // 8M
  ImmutableDBOptions imm_dbo;
  MutableDBOptions   mut_dbo;
  ColumnFamilyOptions cfo;
  cfo.comparator = nullptr; // will set from rpc
  cfo.table_factory = nullptr;

// MyCreatePlugin really bind deserialize function
#define MyCreatePlugin2(obj, field1, field2) \
  params.field2.serde = Bind_CreatePluginTpl(obj.field1, params)
#define MyCreatePlugin1(obj, field) MyCreatePlugin2(obj, field, field)
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  MyCreatePlugin1(cfo, compaction_filter_factory);
  MyCreatePlugin2(cfo, comparator, user_comparator);
  MyCreatePlugin1(cfo, merge_operator);
  MyCreatePlugin1(cfo, table_factory);
  MyCreatePlugin1(cfo, prefix_extractor);
  MyCreatePlugin1(cfo, sst_partitioner_factory);
  MyCreatePlugin1(cfo, html_user_key_coder);
//const size_t n_listeners = params.listeners.size();
  const size_t n_listeners = m_meta.n_listeners;
  params.listeners.resize(n_listeners);
  imm_dbo.listeners.resize(n_listeners);
  for (size_t i = 0; i < n_listeners; i++) {
    MyCreatePlugin1(imm_dbo, listeners[i]);
  }
//size_t n_tbl_prop_coll = params.table_properties_collector_factories.size();
  size_t n_tbl_prop_coll = m_meta.n_prop_coll_factory;
  cfo.table_properties_collector_factories.resize(n_tbl_prop_coll);
  params.table_properties_collector_factories.resize(n_tbl_prop_coll);
  for (size_t i = 0; i < n_tbl_prop_coll; i++) {
    MyCreatePlugin1(cfo, table_properties_collector_factories[i]);
  }
  DEBG("Beg SerDeRead: %s", attempt_dir);
  SerDeRead(in, &params);
  DEBG("End SerDeRead: %s", attempt_dir);
  if (!params.full_history_ts_low.empty()) {
    VERIFY_EQ(cfo.comparator->timestamp_size(),
                     params.full_history_ts_low.size());
  }
  VERIFY_S_EQ(params.cf_paths.back().path, m_meta.output_root);
  params.InputBytes(inputBytes);
  if (MULTI_PROCESS) {
    process_mem_write(getppid(), inputBytes, sizeof(inputBytes));
  }
  imm_dbo.listeners.clear(); // ignore event listener on worker
  imm_dbo.advise_random_on_open = false;
  imm_dbo.allow_fdatasync = false;
  imm_dbo.allow_fallocate = false;
  imm_dbo.statistics = CreateDBStatistics();
  imm_dbo.env = env;
  imm_dbo.fs = fs;
  imm_dbo.db_log_dir = attempt_dbname;
  imm_dbo.db_paths.clear();
  imm_dbo.db_paths.reserve(params.cf_paths.size() + 1);
  size_t  output_path_id = params.cf_paths.size();
  for (auto& x : params.cf_paths) {
    imm_dbo.db_paths.emplace_back(GetWorkerNodePath(x.path), x.target_size);
  }
  imm_dbo.db_paths.emplace_back(attempt_dir, UINT64_MAX);
  cfo.num_levels = params.num_levels;
  cfo.cf_paths = imm_dbo.db_paths;
  cfo.compaction_style = params.compaction_style;
  cfo.compaction_pri   = params.compaction_pri;
  TERARK_VERIFY(kRoundRobin != params.compaction_pri);
  {
    imm_dbo.info_log = m_log;
    imm_dbo.info_log_level = params.compaction_log_level;
    auto var = getenv("INFO_LOG_LEVEL"); // NOLINT
    if (var) {
      if (!enum_value(var, &imm_dbo.info_log_level)) {
        WARN("bad INFO_LOG_LEVEL=%s, ignored", var);
      }
    }
    TRAC("INFO_LOG_LEVEL: rpc = %s, env_var = %s",
         enum_cstr(params.compaction_log_level), var?var:"undefined");
    imm_dbo.info_log->SetInfoLogLevel(imm_dbo.info_log_level);
  }
  {
    auto dbo = BuildDBOptions(imm_dbo, mut_dbo);
    auto s = cfo.table_factory->ValidateOptions(dbo, cfo);
    TERARK_VERIFY_S(s.ok(), "TableFactory.ValidateOptions() = %s", s.ToString());
  }
  BlockCacheTracer* block_cache_tracer = nullptr;
  const std::shared_ptr<IOTracer> io_tracer(nullptr);
  unique_ptr<VersionSet> versions(
      new VersionSet(attempt_dbname, &imm_dbo, env_options, table_cache.get(),
                     &write_buffer_manager, &write_controller,
                     block_cache_tracer, io_tracer,
                  #if (ROCKSDB_MAJOR * 10000 + ROCKSDB_MINOR * 10 + ROCKSDB_PATCH) >= 70060
                     params.db_id,
                  #endif
                     params.db_session_id));
  params.version_set.To(versions.get());

  uint64_t log_number = 0;
  auto manifest_fnum = params.version_set.manifest_file_number;
#define VERIFY_STATUS_OK(s) \
    if (shutting_down.load(std::memory_order_acquire)) \
    { ShutDownCleanFiles(); return 0; } \
    else \
      VERIFY_S(s.ok(), "%s : %s : %s", \
     s.ToString(), attempt_dbname, attempt_dir)
  {
    VersionEdit new_db;
    new_db.SetLogNumber(log_number);
    new_db.SetNextFile(params.version_set.next_file_number);
    new_db.SetLastSequence(params.version_set.last_sequence);
    const string manifest = DescriptorFileName(attempt_dbname, manifest_fnum);
    std::unique_ptr<WritableFileWriter> file_writer;
    Status s = WritableFileWriter::Create(fs, manifest,
        fs->OptimizeForManifestWrite(env_options), &file_writer, nullptr);
    VERIFY_STATUS_OK(s);
    log::Writer log(std::move(file_writer), log_number, false);
    string record;
    new_db.EncodeTo(&record);
    auto s2 = log.AddRecord(record);
    VERIFY_STATUS_OK(s2);
  }
  {
    auto s3 = SetCurrentFile(fs.get(), attempt_dbname, manifest_fnum, nullptr);
    VERIFY_STATUS_OK(s3);
    std::vector<ColumnFamilyDescriptor> column_families;
    if ("default" != params.cf_name) {
      column_families.emplace_back("default", cfo);
    }
    column_families.emplace_back(params.cf_name, cfo);
    auto s4 = versions->Recover(column_families, false);
    VERIFY_STATUS_OK(s4);
  }
  if ("default" != params.cf_name) { // workaround rocksdb pitfall
    VersionEdit edit;
    edit.SetLogNumber(log_number);
    edit.SetNextFile(params.version_set.next_file_number);
    edit.SetLastSequence(params.version_set.last_sequence);
    edit.SetComparatorName(params.user_comparator.clazz);
    edit.SetColumnFamily(params.cf_id);
    edit.AddColumnFamily(params.cf_name);
    // LogAndApply will both write the creation in MANIFEST and create
    // ColumnFamilyData object
    ColumnFamilyData* null_cfd = nullptr;
    bool new_descriptor_log = false;
    auto s = versions->LogAndApply(null_cfd, MutableCFOptions(cfo),
                  &edit, &mutex, null_dbdir, new_descriptor_log, &cfo);
    VERIFY_STATUS_OK(s);
  }
  auto cfd = versions->GetColumnFamilySet()->GetColumnFamily(params.cf_id);
  VERIFY_S(nullptr != cfd, "cf: id = %d name = %s", params.cf_id, params.cf_name);
  VERIFY_S_EQ(params.cf_name, cfd->GetName());
  VERIFY_EQ(params.cf_id, cfd->GetID());
  { // version didn't propagate info_log to cfd->ioptions()
    auto icfo = const_cast<ImmutableOptions*>(cfd->ioptions());
    icfo->info_log = imm_dbo.info_log;
    icfo->info_log_level = imm_dbo.info_log_level;
    icfo->statistics = imm_dbo.statistics;
    //icfo->allow_mmap_reads = true; // not need any more
  }
  {
    VersionEdit edit;
    edit.SetLogNumber(log_number);
    edit.SetNextFile(params.version_set.next_file_number);
    edit.SetLastSequence(params.version_set.last_sequence);
    for (auto& onelevel : *params.inputs) {
      for (auto& file_meta : onelevel.files)
        edit.AddFile(onelevel.level, *file_meta); // file_meta will be copied
    }
    // install files into ColumnFamily cfd
    bool new_descriptor_log = false;
    auto s = versions->LogAndApply(cfd, *cfd->GetLatestMutableCFOptions(),
                  &edit, &mutex, null_dbdir, new_descriptor_log, &cfo);
    VERIFY_STATUS_OK(s);
  }
  VersionStorageInfo* storage_info = cfd->current()->storage_info();
  vector<CompactionInputFiles> inputs = *params.inputs;
  for (auto& onelevel : inputs) {
    auto populated_files = storage_info->LevelFiles(onelevel.level);
    VERIFY_S(onelevel.files.size() == populated_files.size(),
        "%zd %zd : level = %d : %s >>>>\n%s\n<<<<",
        onelevel.files.size(), populated_files.size(),
        onelevel.level, attempt_dbname, params.DebugString());
    onelevel.files = std::move(populated_files);
  }
  std::string trim_ts = "";
  Compaction compaction(storage_info,
      *cfd->ioptions(), *cfd->GetLatestMutableCFOptions(), mut_dbo, inputs,
      params.output_level, params.target_file_size, params.max_compaction_bytes,
      uint32_t(output_path_id), params.compression, params.compression_opts,
      Temperature::kWarm, // rocksdb-6.24
      params.max_subcompactions, *params.grandparents, params.manual_compaction,
    #if ROCKSDB_MAJOR >= 7
      "", // trim_ts
    #endif
      params.score, params.deletion_compaction,
    #if (ROCKSDB_MAJOR * 10000 + ROCKSDB_MINOR * 10 + ROCKSDB_PATCH) >= 70060
      true, // l0_files_might_overlap
    #endif
      params.compaction_reason);
  DEBG("%s: bottommost_level: fake = %d, rpc = %d", attempt_dir,
       compaction.bottommost_level(), params.bottommost_level);
  compaction.set_bottommost_level(params.bottommost_level);
  compaction.SetInputVersion(cfd->current());
//----------------------------------------------------------------------------
  LogBuffer log_buffer(imm_dbo.info_log_level, imm_dbo.info_log.get());
  DBImpl* null_db = nullptr;
  ErrorHandler error_handler(null_db, imm_dbo, &mutex);
  EventLogger event_logger(imm_dbo.info_log.get());
  SnapshotChecker* snapshot_checker = nullptr;
  FSDirectory* db_directory = nullptr;
  FSDirectory* output_directory = nullptr;
  FSDirectory* blob_output_directory = nullptr;
  const bool measure_io_stats = true;
#if (ROCKSDB_MAJOR * 10000 + ROCKSDB_MINOR * 10 + ROCKSDB_PATCH) < 70040
  const std::atomic<int>* manual_compaction_paused = nullptr;
  const std::atomic<bool>* manual_compaction_canceled = nullptr;
#else
  JobContext* job_context = nullptr;
  std::atomic<bool> manual_compaction_canceled{false};
#endif
  BlobFileCompletionCallback* blob_callback = nullptr;
  CompactionJob compaction_job(
      params.job_id, &compaction, imm_dbo, mut_dbo, file_options,
      versions.get(),
      &shutting_down,
   #if (ROCKSDB_MAJOR * 10000 + ROCKSDB_MINOR * 10 + ROCKSDB_PATCH) < 70040
      params.preserve_deletes_seqnum,
   #endif
      &log_buffer,
      db_directory, output_directory, blob_output_directory,
      imm_dbo.statistics.get(),
      &mutex, &error_handler, *params.existing_snapshots,
      params.earliest_write_conflict_snapshot, snapshot_checker,
   #if (ROCKSDB_MAJOR * 10000 + ROCKSDB_MINOR * 10 + ROCKSDB_PATCH) >= 70040
      job_context,
   #endif
      table_cache, &event_logger, params.paranoid_file_checks,
      measure_io_stats,
      attempt_dbname, &results->job_stats, Env::Priority::USER,
      std::make_shared<IOTracer>(),
    #if (ROCKSDB_MAJOR * 10000 + ROCKSDB_MINOR * 10 + ROCKSDB_PATCH) < 70040
      manual_compaction_paused,
    #endif
      manual_compaction_canceled,
      params.db_id, params.db_session_id, params.full_history_ts_low,
    #if ROCKSDB_MAJOR >= 7
      "", // trim_ts
    #endif
      blob_callback);

  compaction_job.Prepare();
  MutexUnlock();
  VERIFY_S_EQ(compaction.GetSmallestUserKey(), params.smallest_user_key);
  VERIFY_S_EQ(compaction.GetLargestUserKey() , params.largest_user_key);
  const std::string start_time = StrDateTimeNow();
  ShowCompactionParams(params, cfd->current(), cfd, &start_time);
  {
    Status s1 = compaction_job.Run();
    IOStatus s2 = compaction_job.io_status();
    //VERIFY_STATUS_OK(s1);
    //VERIFY_STATUS_OK(s2);
    if (!s1.ok()) {
      ERROR("compaction_job.Run(%s) = %s : io_status() = %s",
            attempt_dir, s1.ToString(), s2.ToString());
      results->status = s1;
    }
    else if (!s2.ok()) {
      ERROR("compaction_job.io_status(%s) = %s", attempt_dir, s2.ToString());
      results->status = Status(s2);
    }
  }
auto writeObjResult = [&]{
  json js = JS_CompactionParamsEncodePtr(&params);
#define SetResultSerDe2(obj, field1, field2) \
  SerDe_SerializeOpt(out, params.field2.clazz, obj.field1, js)
#define SetResultSerDe1(obj, field) SetResultSerDe2(obj, field, field)
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  SetResultSerDe1(cfo, compaction_filter_factory);
  SetResultSerDe2(cfo, comparator, user_comparator);
  SetResultSerDe1(cfo, merge_operator);
  SetResultSerDe1(cfo, table_factory);
  SetResultSerDe1(cfo, prefix_extractor);
  SetResultSerDe1(cfo, sst_partitioner_factory);
//SetResultSerDe1(cfo, html_user_key_coder); // not needed
  NonOwnerFileStream(out).write_var_uint64(n_listeners);
  for (size_t i = 0; i < n_listeners; i++) {
    SetResultSerDe1(imm_dbo, listeners[i]);
  }
  NonOwnerFileStream(out).write_var_uint64(n_tbl_prop_coll);
  for (size_t i = 0; i < n_tbl_prop_coll; i++) {
    SetResultSerDe1(cfo, table_properties_collector_factories[i]);
  }
};
  vector<vector<const FileMetaData*> > output_files;
  compaction_job.GetSubCompactOutputs(&output_files);
  results->output_files.resize(output_files.size());
  for (size_t i = 0; i < output_files.size(); ++i) {
    auto& src_vec = output_files[i];
    auto& dst_vec = results->output_files[i];
    dst_vec.resize(src_vec.size());
    for (size_t j = 0; j < dst_vec.size(); ++j) {
      const FileMetaData& src = *src_vec[j];
      CompactionResults::FileMinMeta& dst = dst_vec[j];
      dst.file_number = src.fd.GetNumber();
      dst.file_size = src.fd.GetFileSize();
      dst.smallest_seqno = src.fd.smallest_seqno;
      dst.largest_seqno = src.fd.largest_seqno;
      dst.smallest_ikey = src.smallest;
      dst.largest_ikey = src.largest;
    }
  }
  results->output_dir = GetHosterNodePath(attempt_dir);
 #if (ROCKSDB_MAJOR * 10000 + ROCKSDB_MINOR * 10 + ROCKSDB_PATCH) < 70060
  results->compaction_stats = compaction_job.GetCompactionStats();
 #else
  results->compaction_stats = compaction_job.GetCompactionStats().stats;
 #endif
  results->job_stats = *compaction_job.GetCompactionJobStats();
  imm_dbo.statistics->GetAggregated(results->statistics.tickers,
                                    results->statistics.histograms);
  auto t1 = pf.now();
  results->work_time_usec = pf.us(t0, t1);
  try {
    SerDeWrite(out, results);
    writeObjResult();
  }
  catch (const std::exception& ex) {
    ERROR("SerDeWrite(%s) fail = %s", attempt_dir, ex);
    fclose(in); fclose(out); // ignore close error
    return 0;
  }
  //fflush(out); // flush crt buf to OS buf
  //fsync(fileno(out)); // must sync before close for NFS
  long out_fsize = ftell(out); //FileStream::fpsize(out);
  TERARK_VERIFY_GT(out_fsize, 1);
  fclose(in); //INFO("after fclose(in)");
  fclose(out); // must close before write compact_done_file
  //INFO("after fclose(out)");
  auto t2 = pf.now();
  {
    MutexLock();
    Status s = compaction_job.Install(*cfd->GetLatestMutableCFOptions());
    MutexUnlock();
    if (!s.ok()) {
      ERROR("compaction_job.Install(%s) = %s", attempt_dir, s.ToString());
      return 0;
    }
  }
  log_buffer.FlushBufferToLog();
  // compact end time
  auto t3 = pf.now();
  const std::string end_time = StrDateTimeNow();
  ShowCompactionParams(params, cfd->current(), cfd, &start_time, &end_time, pf.sf(t0, t3));
  if (terark::getEnvBool("DEL_WORKER_TEMP_DB", false)) {
    std::error_code ec;
    std::filesystem::remove_all(attempt_dbname, ec);
    // if cur attempt is last attempt, DeleteDir will success
    env->DeleteDir(job_dbname[3]); // dir job_id
    auto t4 = pf.now();
    INFO("%s: time work = %.6f, result = %.6f, install = %.6f, deldir = %.6f, input{raw = %s zip = %s}",
        attempt_dbname, pf.sf(t0,t1), pf.sf(t1,t2), pf.sf(t2,t3), pf.sf(t3,t4),
        SizeToString(inputBytes[0]), SizeToString(inputBytes[1]));
  }
  else {
    INFO("%s: time work = %.6f, result = %.6f, install = %.6f, input{raw = %s zip = %s}",
        attempt_dbname, pf.sf(t0,t1), pf.sf(t1,t2), pf.sf(t2,t3),
        SizeToString(inputBytes[0]), SizeToString(inputBytes[1]));
  }
  if (!shutting_down.load(std::memory_order_acquire)) {
    std::string compact_done_file = attempt_dir + "/compact.done";
    int fd = ::creat(compact_done_file.c_str(), 0644);
    if (fd < 0) {
      std::string errmsg = strerror(errno);
      ERROR("creat(%s) = %s", compact_done_file, errmsg);
      return 0;
    }
    //INFO("after creat(compact.done)");
    if (dprintf(fd, "%ld", out_fsize) <= 0) {
      std::string errmsg = strerror(errno);
      ERROR("%s : %s", compact_done_file, errmsg);
      ::close(fd); // ignore close error
      return 0;
    }
    //INFO("after dprintf(compact.done)");
    //::fsync(fd);
    if (::close(fd) < 0) {
      std::string errmsg = strerror(errno);
      ERROR("%s : %s", compact_done_file, errmsg);
    }
    //INFO("after close(compact.done)");
  }

  return 0;
}

void ShutDownCleanFiles() const {
  if (!m_shutdown_files_cleaned) {
    const string worker_dir = GetWorkerNodePath(m_meta.output_root);
    const string output_dir = CatJobID(worker_dir, m_meta.job_id);
    const string attempt_dir = CatAttempt(output_dir, m_meta);
    Logger* info_log = m_log.get();
    std::error_code ec;
    //std::filesystem::remove_all(output_dir, ec); // ignore error
    //INFO("ShutDownCleanFiles: '%s' and parent", attempt_dir);
    std::filesystem::remove_all(attempt_dir, ec); // ignore error
    INFO("ShutDownCleanFiles: dir = %s", attempt_dir);
    m_shutdown_files_cleaned = true;
  }
}

static void write_html_header(struct mg_connection* conn, const json& query, const char* name) {
  bool from_db_node = JsonSmartBool(query, "from_db_node", false);
  mg_printf(conn, "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n");
  int refresh = JsonSmartInt(query, "refresh", 0);
  if (refresh > 0) {
    mg_printf(conn,
      "<html><title>%s</title>\n"
      "<meta http-equiv='refresh' content='%d'>\n"
      "<body>\n", name, refresh);
  }
  else {
    mg_printf(conn, "<html><title>%s</title><body>\n", name);
  }
  if (from_db_node) {
    mg_printf(conn, "<p>%s</p>\n", cur_time_stat().c_str());
  } else {
    mg_printf(conn, R"(
<script>
function SetParam(name, value) {
  const url = new URL(location.href);
  var params = new URLSearchParams(url.search);
  params.set(name, value);
  url.search = params.toString();
  location.href = url.href;
}
</script>
    )");
    mg_print_cur_time(conn);
  }
  if (WEB_DOMAIN)
    mg_printf(conn, "<script>document.domain = '%s';</script>\n", WEB_DOMAIN);
}

class BasePostHttpHandler : public CivetHandler {
  virtual void doIt(const DcompactMeta&, struct mg_connection*) = 0;
 public:
#if CIVETWEB_VERSION_MAJOR * 100000 + CIVETWEB_VERSION_MINOR * 100 >= 1*100000 + 15*100
  using CivetHandler::handlePost;
#endif
  bool handlePost(CivetServer* server, struct mg_connection* conn) override {
    std::string data = ReadPostData(conn);
    DcompactMeta meta;
    Logger* info_log = nullptr;
#if defined(NDEBUG)
    try {
#endif
      meta.FromJsonStr(data);
      if (CHECK_CODE_REVISION >= 1 && meta.code_version != ROCKSDB_VERSION) {
        HttpErr(412, "ROCKSDB_VERSION Error: my = %d, req = %d", ROCKSDB_VERSION, meta.code_version);
        return true;
      }
      auto githash = strchr(rocksdb_build_git_sha, ':');
      ROCKSDB_VERIFY(nullptr != githash);
      githash++; // skip the ':'
      if (CHECK_CODE_REVISION >= 2 && meta.code_githash != githash) {
        HttpErr(412, "rocksdb_githash Error: my = %s, req = %s", githash, meta.code_githash);
        return true;
      }
      doIt(meta, conn); // will not throw
#if defined(NDEBUG)
    }
    catch (const std::exception& ex) {
      HttpErr(412, "JsonParseError: %s", ex);
    }
    catch (const Status& s) {
      HttpErr(412, "JsonParseError: Status: %s", s.ToString());
    }
#endif
    return true;
  }
};
class DcompactHttpHandler : public BasePostHttpHandler {
 public:
  void doIt(const DcompactMeta& meta, struct mg_connection* conn) override {
      RunOneJob(meta, conn); // will not throw
  }
};

class ShutdownCompactHandler : public BasePostHttpHandler {
 public:
  void doIt(const DcompactMeta& meta, struct mg_connection* conn) override {
    Logger* info_log = nullptr;
    auto p = g_acceptedJobs.find(meta);
    if (p) {
      info_log = p->m_log.get();
      p->ShutDown();
      mg_printf(conn,
        "HTTP/1.1 200 OK\r\nContent-Type: text/json\r\n\r\n"
        R"({"status": "ok", "addr": "%s"})", ADVERTISE_ADDR.c_str()
      );
      INFO("shutdown success: %s", meta.ToJsonStr());
      info_log->Flush();
    }
    else {
      mg_printf(conn,
        "HTTP/1.1 200 OK\r\nContent-Type: text/json\r\n\r\n"
        R"({"status": "NotFound", "addr": "%s"})", ADVERTISE_ADDR.c_str()
      );
      WARN("shutdown NotFound: %s", meta.ToJsonStr());
    }
  }
};

class ProbeCompactHandler : public BasePostHttpHandler {
 public:
  void doIt(const DcompactMeta& meta, struct mg_connection* conn) override {
    Logger* info_log = nullptr;
    auto p = g_acceptedJobs.find(meta);
    if (p) {
      info_log = p->m_log.get();
      mg_printf(conn,
        "HTTP/1.1 200 OK\r\nContent-Type: text/json\r\n\r\n"
        R"({"status": "ok", "addr": "%s"})", ADVERTISE_ADDR.c_str()
      );
      ROCKS_LOG_INFO(info_log, "got http probe req");
    }
    else {
      mg_printf(conn,
        "HTTP/1.1 200 OK\r\nContent-Type: text/json\r\n\r\n"
        R"({"status": "NotFound", "addr": "%s"})", ADVERTISE_ADDR.c_str()
      );
      DEBG("probe NotFound: %s", meta.ToJsonStr());
    }
  }
};

static const char* StrDateTime(long long now_miros) {
  static thread_local char buf[64];
  time_t rawtime = now_miros / 1000000;
  struct tm* timeinfo = localtime(&rawtime);
  auto len = strftime(buf, sizeof(buf), "%F %T",timeinfo);
  sprintf(buf + len, ".%03lld", now_miros % 1000000 / 1000);
  return buf;
}

class ListHttpHandler : public CivetHandler {
 public:
#if CIVETWEB_VERSION_MAJOR * 100000 + CIVETWEB_VERSION_MINOR * 100 >= 1*100000 + 15*100
  using CivetHandler::handlePost;
#endif
  bool handleGet(CivetServer* server, struct mg_connection* conn) override {
#if defined(NDEBUG)
    try {
#endif
      const mg_request_info* req = mg_get_request_info(conn);
      json query = from_query_string(req->query_string);
      bool html = JsonSmartBool(query, "html", true);
      terark::string_appender<> oss;
      oss.reserve(64*1024);
      oss|R"(
<link rel='stylesheet' type='text/css' href='/style.css'>
<style>
td {
  text-align: right;
}
</style>
<table border=1><tbody>
<tr>
  <th>job worker dir</th>
  <th>sub</th>
  <th>input raw</th>
  <th>input zip</th>
  <th>accept time</th>
  <th>start time</th>
  <th>elapsed rt</th>
  <th>kill</th>
</tr>
)";
  long long now_micros = Env::Default()->NowMicros();
  g_acceptedJobs.get_mtx().lock();
      for (size_t i = 0, n = g_acceptedJobs.get_map().end_i(); i < n; i++) {
        if (g_acceptedJobs.get_map().is_deleted(i)) {
          continue;
        }
        fstring key = g_acceptedJobs.get_map().key(i);
        Job*    job = g_acceptedJobs.get_map().val(i);
        oss|"<tr>";
        oss|"<td align='left'><a href='/"|key|"'>"|key|"</a></td>\n";
        oss|"<th>"|job->m_meta.n_subcompacts|"</th>";
        oss|"<td>"|SizeToString(job->inputBytes[0])|"</td>";
        oss|"<td>"|SizeToString(job->inputBytes[1])|"</td>";
        oss|"<td>"|StrDateTime(job->accept_time)|"</td>";
        oss|"<td>"|StrDateTime(job->start_run_time)|"</td>";
        oss^"<td>%.3f"^(now_micros - job->start_run_time)/1e6^"</td>";
        oss|"\n<script>\nvar g_killed_"|i|" = false;\n";
        oss|"async function kill_"|i|"() {\n";
        oss|"  if (g_killed_"|i|") { alert('already killed'); return;}\n";
        oss|"  var meta_js = `"|job->m_meta.ToJsonStr()|"`;";
        oss^R"EOS(
  const response = await fetch('/shutdown' + document.location.search, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json'
    },
    body: meta_js
  });
  const text = await response.text();
  document.getElementById('kill-result').innerHTML = "kill_%zd() = " + text;
)EOS"^i;
        oss|"  g_killed_"|i|" = true;\n";
        oss|"}\n</script>\n";
        oss|"<td><a href='javascript:kill_"|i|"()'>kill</td>";
        oss|"</tr>\n";
      }
  g_acceptedJobs.get_mtx().unlock();
      oss|"</tbody></table>\n";
      oss|"<p></p>\n";
      oss|"<pre id='kill-result'></pre>\n";
      if (html) {
        write_html_header(conn, query, "list");
        mg_write(conn, oss.str());
        mg_printf(conn, "</body></html>\n");
      }
      else {
        mg_printf(conn, "HTTP/1.1 200 OK\r\nContent-Type: text/json\r\n\r\n");
        mg_write(conn, oss.str());
      }
#if defined(NDEBUG)
    }
    catch (std::exception& ex) {
      mg_printf(conn, "Caught: %s\n", ex.what());
    }
    catch (const Status& s) {
      mg_printf(conn, "Status: %s\n", s.ToString().c_str());
    }
    catch (...) {
      mg_printf(conn, "Unknown Error\n");
    }
#endif
    return true;
  }
};

class StatHttpHandler : public CivetHandler {
 public:
#if CIVETWEB_VERSION_MAJOR * 100000 + CIVETWEB_VERSION_MINOR * 100 >= 1*100000 + 15*100
  using CivetHandler::handlePost;
#endif
  bool handleGet(CivetServer* server, struct mg_connection* conn) override {
#if defined(NDEBUG)
    try {
#endif
      const mg_request_info* req = mg_get_request_info(conn);
      json query = from_query_string(req->query_string);
      json js;
      bool html = JsonSmartBool(query, "html", true);
      int verbose = JsonSmartInt(query, "verbose", 1);
      if (JS_TopZipTable_Global_Env) {
        const char* tzkey = html ? "Top<br/>ling<br/>Zip" : "ToplingZip";
        json& tz = js[tzkey] = JS_TopZipTable_Global_Stat(html);
        if (verbose < 1) { // more important than waitQueueSize
          tz.erase("sumUserKeyLen");
          tz.erase("sumUserKeyNum");
        }
        if (verbose < 2) { // more important than sumWaitingMem/sumWorkingMem
          tz.erase("waitQueueSize");
        }
        if (verbose < 3) {
          tz.erase("sumWaitingMem");
          tz.erase("sumWorkingMem");
        }
        if (verbose >= 3) {
          js["Env"] = JS_TopZipTable_Global_Env();
        }
      }
      json& vars = js["Vars"];
      if (verbose >= 3) {
        ROCKSDB_JSON_SET_PROP(vars, NFS_MOUNT_ROOT);
        ROCKSDB_JSON_SET_PROP(vars, NFS_DYNAMIC_MOUNT);
      }
      vars["MAX_PARALLEL"] = MAX_PARALLEL_COMPACTIONS;
      if (verbose >= 3) {
        vars["Compactions"]["accepting"] = g_jobsAccepting.load(std::memory_order_relaxed);
      }
      vars["Compactions"][html ? "<a href='/list'>running</a>" : "running"] =
                          g_jobsRunning.load(std::memory_order_relaxed);
      vars["Compactions"]["waiting"] = g_jobsWaiting.load(std::memory_order_relaxed);
      vars["Compactions"]["queuing"] = g_workQueue.peekSize();
    //vars["Compactions"]["accepted"] = g_acceptedJobs.peekSize();
      vars["Compactions"]["finished"] = g_jobsFinished.load(std::memory_order_relaxed);
    #define ShowNonZero(name, atom_var) \
      do { auto name = atom_var.load(std::memory_order_relaxed); \
        if (name || verbose >= 3) vars["Compactions"][#name] = name; \
      } while (0)
      ShowNonZero(rejected, g_jobsRejected);
      ShowNonZero(prefailed, g_jobsPreFailed);
#ifdef TOPLING_DCOMPACT_USE_ETCD
      ShowNonZero(etcd_err, g_etcd_err);
#endif
      if (verbose >= 4) {
        std::string buf(8192, '\0');
        const mg_context* ctx = server->getContext();
        int len = mg_get_context_info(ctx, buf.data(), buf.size());
        buf.resize(len);
        vars["Server"] = json::parse(buf);
      }
      if (JsonSmartBool(query, "version")) {
        JS_ModuleGitInfo_Add(js, html);
      }
      if (html) {
        write_html_header(conn, query, "stat");
        mg_write(conn, JsonToString(js, query));
        mg_printf(conn, "</body></html>\n");
      }
      else {
        mg_printf(conn, "HTTP/1.1 200 OK\r\nContent-Type: text/json\r\n\r\n");
        mg_write(conn, JsonToString(js, query));
      }
#if defined(NDEBUG)
    }
    catch (std::exception& ex) {
      mg_printf(conn, "Caught: %s\n", ex.what());
    }
    catch (const Status& s) {
      mg_printf(conn, "Status: %s\n", s.ToString().c_str());
    }
    catch (...) {
      mg_printf(conn, "Unknown Error\n");
    }
#endif
    return true;
  }
};

class StopHttpHandler : public CivetHandler {
 public:
#if CIVETWEB_VERSION_MAJOR * 100000 + CIVETWEB_VERSION_MINOR * 100 >= 1*100000 + 15*100
  using CivetHandler::handlePost;
#endif
  bool handleGet(CivetServer*, struct mg_connection* conn) override {
    g_stop = true;
    g_stop_cond.notify_one();
    return true;
  }
};

static int main(int argc, char* argv[]) {
  SetAsCompactionWorker();
  Logger* info_log = nullptr;
  INFO("In main function, global objects was init ok before main function");
  if (MAX_PARALLEL_COMPACTIONS <= 0) {
    ERROR("bad MAX_PARALLEL_COMPACTIONS = %ld", MAX_PARALLEL_COMPACTIONS);
    return 1;
  }
  if (MAX_WAITING_COMPACTIONS <= 1) {
    ERROR("bad MAX_WAITING_COMPACTIONS = %ld", MAX_WAITING_COMPACTIONS);
    return 1;
  }
  g_workQueue.queue().init(MAX_WAITING_COMPACTIONS + 1);

  // reuse the compaction process: read compaction spec from http://me/dcompact
  // by post method, each http request defines a compaction job.
  // thus run multi jobs in one process, and such jobs can be executed in
  // parallel, thus utilize the CPU and other system resources:
  //  1. Multi compaction jobs can read input SST in parallel
  //     - during sst read(first pass scan), there is only one thread is active
  //       for each compaction job, if we just run one compaction job, the CPU
  //       will not be utilized.
  //  2. With large machine(such as 16 CPU cores) and DictZipBlobStore pipeline,
  //     L3 cache will be utilized by ToplingZipTableOptions::optimizeCpuL3Cache.
  //     L3 cache will not be utilized by divide one large machine into many
  //     small virtual machines(or containers).
#if 0
  if (NFS_DYNAMIC_MOUNT) {
    if (unshare(CLONE_NEWNS) < 0) {
      ERROR("FATAL: unshare(CLONE_NEWNS) = %m");
      return 1;
    }
  }
#endif
  ConnectEtcd();
  vector<string> mg_options;
  const char* doc_root = nullptr;
  for (int opt = 0; (opt = getopt(argc, argv, "D:")) != -1; ) {
    switch (opt) {
      default:
        break;
      case '?':
        fprintf(stderr, "usage: %s { -D name=value }\n", argv[0]);
        return 2;
      case 'D':
        if (auto eq = strchr(optarg, '=')) { // NOLINT
          if (fstring(optarg, eq) == "document_root") {
            doc_root = eq+1;
          }
          mg_options.emplace_back(optarg, eq);
          mg_options.emplace_back(eq+1);
        } else {
          mg_options.emplace_back(optarg);
          mg_options.push_back(""); // has no value
        }
        break;
    }
  }
  if (doc_root) {
    if (doc_root != WORKER_DB_ROOT) {
      fprintf(stderr, "ERROR: when arg -D document_root is given, it must be same with env WORKER_DB_ROOT\n");
      return 1;
    }
  } else {
    mg_options.push_back("document_root");
    mg_options.push_back(WORKER_DB_ROOT);
  }
  mg_init_library(0);
  CivetServer civet(mg_options);
  DcompactHttpHandler handle_dcompact;
  ShutdownCompactHandler handle_shutdown;
  ProbeCompactHandler handle_probe;
  ListHttpHandler handle_list;
  StatHttpHandler handle_stat;
  StopHttpHandler handle_stop;
  civet.addHandler("/dcompact", handle_dcompact);
  civet.addHandler("/shutdown", handle_shutdown);
  civet.addHandler("/probe", handle_probe);
  civet.addHandler("/list", handle_list);
  civet.addHandler("/stat", handle_stat);
  civet.addHandler("/stop", handle_stop); // stop process
  INFO("CivetServer setup ok, start work threads");
  valvec<std::thread> work_threads(MAX_PARALLEL_COMPACTIONS-1, valvec_reserve());
  for (size_t i = 0; i < work_threads.capacity(); ++i) {
    work_threads.unchecked_emplace_back(&work_thread_func);
  }
  work_thread_func(); // use main thread as a work thread
  for (auto& t : work_threads) t.join();

  {
    std::unique_lock<std::mutex> lock(g_stop_mtx);
    g_stop_cond.wait(lock, []() { return g_stop; });
    TERARK_VERIFY(g_stop);
  }
  mg_exit_library();
#ifdef TOPLING_DCOMPACT_USE_ETCD
  delete g_etcd;
#endif
  return 0;
} // main

static void ConnectEtcd() {
#ifdef TOPLING_DCOMPACT_USE_ETCD
  string etcd_url = getEnvStr("ETCD_URL", "");
  string username = getEnvStr("ETCD_USERNAME", "");
  string password = getEnvStr("ETCD_PASSWORD", "");
  string load_balancer = getEnvStr("ETCD_LOAD_BALANCER", "round_robin");
  string ca   = getEnvStr("ETCD_CA"  , ""); // connect by {ca, cert, key}
  string cert = getEnvStr("ETCD_CERT", ""); // can be empty when using ca
  string key  = getEnvStr("ETCD_KEY" , ""); // can be empty when using ca
  if (etcd_url.empty()) {
    Logger* info_log = nullptr;
    WARN("ETCD_URL is not defined, we will not use etcd");
    return; // do not use etcd
  }
  if (!ca.empty()) // first try ca
    g_etcd = new etcd::Client(etcd_url, ca, cert, key, load_balancer);
  else if (!username.empty()) // second try user password
    g_etcd = new etcd::Client(etcd_url, username, password, load_balancer);
  else // last try uncertified
    g_etcd = new etcd::Client(etcd_url, load_balancer);
#endif
}

#ifdef TOPLING_DCOMPACT_USE_ETCD
  int done_stat = 0;
  std::mutex cond_mtx;
  std::condition_variable cond_var;
void NotifyEtcd() const {
  if (nullptr == g_etcd) {
    return;
  }
  std::string key;
  key.append(m_meta.etcd_root);
  key.append("/dcompact-res/");
  key.append(m_meta.instance_name);
  key.append("/");
  key.append(m_meta.start_time);
  key.append("/");
  key.append(m_meta.dbname);
  AddFmt(key, "/job-%05d/att-%02d", m_meta.job_id, m_meta.attempt);
  // notify hoster to fetch the compaction result from nfs, fail is also done
  int ttl = 60; // 60 sec, type of ttl must be int
  auto t0 = env->NowMicros();
  Logger* info_log = m_log.get();
  TRAC("NotifyEtcd: calling Etcd.set(%s, done)", key);
  intrusive_ptr<Job> self = this; // must ref this
  g_etcd->set(key, "done", ttl).then([=](pplx::task<etcd::Response> async) {
    TERARK_VERIFY_EQ(self.get(), this); // must use self
    TERARK_VERIFY_EQ(info_log, this->m_log.get());
#ifdef NDEBUG
try {
#endif
  etcd::Response resp = async.get();
  if (!resp.is_ok()) {
    g_etcd_err.fetch_add(1, std::memory_order_relaxed);
    ERROR("Etcd.set(%s, done) = %s", key, resp.error_message());
    info_log->Flush();
    done_stat = 2; // error
    cond_var.notify_all();
  }
  else {
    auto t1 = env->NowMicros();
    DEBG("Etcd.set(%s, done) success with %8.6f sec, to call cond_var.notify_all()", key, (t1-t0)/1e6);
    info_log->Flush();
    done_stat = 1;
    cond_var.notify_all();
  }
#ifdef NDEBUG
} catch (const std::exception& ex) {
  ERROR("Etcd.set(%s, done) throws exception = %s", key, ex);
  info_log->Flush();
  done_stat = 2; // error
  g_etcd_err.fetch_add(1, std::memory_order_relaxed);
  cond_var.notify_all();
}
#endif
});
  if (0 == done_stat) {
    for (int retry = 0; retry < 1000; ++retry) {
      using namespace std::chrono;
      std::unique_lock<std::mutex> lock(cond_mtx);
      auto pred = [&]{ return 0 != done_stat; }; // to avoid spuriously awake
      if (cond_var.wait_for(lock, milliseconds(200), pred)) {
        TERARK_VERIFY(0 != done_stat); // can not use VERIFY_XXX
        break;
      } else {
        auto t1 = env->NowMicros();
        ERROR("Etcd.set(%s, done) wait for %8.6f sec, retry = %d", key, (t1-t0)/1e6, retry);
        info_log->Flush();
      }
    }
  }
  if (1 == done_stat) {
    auto t1 = env->NowMicros();
    INFO("Etcd.set(%s, done) success with %8.6f sec", key, (t1-t0)/1e6);
  }
  else if (0 == done_stat) { // timeout
    auto t1 = env->NowMicros();
    ERROR("Etcd.set(%s, done) timeout with %8.6f sec", key, (t1-t0)/1e6);
    long num = g_etcd_err.fetch_add(1, std::memory_order_relaxed);
    if (num > 1000) {
      TERARK_DIE("Etcd.set(): too many error = %ld !", num);
    }
  }
  TRAC("Etcd.set(%s, done): refcount = %ld", key, self->get_refcount());
}
#else
void NotifyEtcd() const {}
#endif

static void RunOneJob(const DcompactMeta& meta, mg_connection* conn) noexcept {
  if (!Slice(meta.output_root).starts_with(meta.hoster_root)) {
    Logger* info_log = nullptr;
    HttpErr(412,
      "Bad Request: hoster_root = (%s) is not a prefix of output_root = (%s)",
      meta.hoster_root, meta.output_root);
  }
  if (g_stop) {
    Logger* info_log = nullptr;
    HttpErr(412, "Compact Worker is stopping");
    return;
  }
  intrusive_ptr<Job> j = new Job(meta);
  const string old_prefix = meta.hoster_root;
  const string new_prefix = MakePath(NFS_MOUNT_ROOT, meta.instance_name);
  const string worker_dir = ReplacePrefix(old_prefix, new_prefix, meta.output_root);
  const string output_dir = CatJobID(worker_dir, meta.job_id);
  const string attempt_dir = CatAttempt(output_dir, meta);
  Logger* info_log = j->m_log.get();
  ROCKS_LOG_INFO(info_log, "ADVERTISE_ADDR: %s : %s",
                 ADVERTISE_ADDR.c_str(), cur_time_stat().c_str());
  DEBG("meta: %s", meta.ToJsonStr());
  auto n_subcompacts = meta.n_subcompacts;
  auto running = g_jobsRunning.load(std::memory_order_relaxed);
  auto waiting = g_jobsWaiting.fetch_add(n_subcompacts, std::memory_order_relaxed);
  size_t limit = (MAX_PARALLEL_COMPACTIONS + MAX_WAITING_COMPACTIONS) * 3/4;
  if (running + waiting >= limit) {
    g_jobsWaiting.fetch_sub(n_subcompacts, std::memory_order_relaxed);
    g_jobsRejected.fetch_add(1, std::memory_order_relaxed);
    HttpErr(503, "%s : server busy, running jobs = %ld, waiting = %zd", attempt_dir, running, waiting);
    return;
  }
  g_jobsAccepting.fetch_add(1, std::memory_order_relaxed);
  TERARK_SCOPE_EXIT(g_jobsAccepting.fetch_sub(1, std::memory_order_relaxed));
  auto t1 = pf.now();
  size_t lru_handle = size_t(-1);
  if (NFS_DYNAMIC_MOUNT) {
    int err;
    std::tie(lru_handle, err) = g_mnt_map.ensure_mnt(meta, conn, info_log);
    if (err) {
      g_jobsWaiting.fetch_sub(n_subcompacts, std::memory_order_relaxed);
      g_jobsPreFailed.fetch_add(1, std::memory_order_relaxed);
      return; // error was reported in ensure_mnt
    }
  }
  auto t2 = pf.now();
  string inFname = MakePath(output_dir, "rpc.params");
  string outFname = MakePath(attempt_dir, "rpc.results");
  FILE* in = fopen(inFname.c_str(), "rb");
  if (!in) {
    HttpErr(412, "fopen(%s, rb) = %s", inFname, strerror(errno));
    g_jobsWaiting.fetch_sub(n_subcompacts, std::memory_order_relaxed);
    g_jobsPreFailed.fetch_add(1, std::memory_order_relaxed);
    return;
  }
  auto t3 = pf.now();
  FILE* out = fopen(outFname.c_str(), "wb");
  if (!out) {
    fclose(in);
    HttpErr(412, "fopen(%s, wb) = %s", outFname, strerror(errno));
    g_jobsWaiting.fetch_sub(n_subcompacts, std::memory_order_relaxed);
    g_jobsPreFailed.fetch_add(1, std::memory_order_relaxed);
    return;
  }
  auto t4 = pf.now();
  INFO("accept %s : fopen(rpc.params) = %.6f sec, fopen(rpc.results) = %.6f sec",
       attempt_dir, pf.sf(t2, t3), pf.sf(t3, t4));
  g_acceptedJobs.add(j.get());
  j->accept_time = j->env->NowMicros();
  mg_printf(conn, "HTTP/1.1 200 OK\r\n"
                  "Content-type: text\r\n\r\n"
                  R"({"status": "ok", "addr": "%s"})",
            ADVERTISE_ADDR.c_str());

  // capture vars by value, not by ref
  g_workQueue.push_back({[=]() {
    j->start_run_time = j->env->NowMicros();
    auto t5 = pf.now();
    g_jobsRunning.fetch_add(n_subcompacts, std::memory_order_relaxed);
    g_jobsWaiting.fetch_sub(n_subcompacts, std::memory_order_relaxed);
    auto run = [=] {
      CompactionResults* results = j->results;
      results->mount_time_usec = pf.us(t1, t2);
      results->prepare_time_usec = pf.us(t2, t4);
      results->waiting_time_usec = pf.us(t4, t5);
      // {in,out} are closed in RunCompact
      j->RunCompact(in, out);
      //INFO("after j->RunCompact()");
    };
    if (MULTI_PROCESS) {
      info_log->Flush(); // flush before fork
      pid_t pid = fork();
      ROCKSDB_VERIFY_GE(pid, 0);
      auto t6 = pf.now();
      if (0 == pid) { // child process
        prctl(PR_SET_PDEATHSIG, SIGKILL);
        // LOG is not inherited in child process, create a new one
        j->CreateLogger("LOG.child");
        auto info_log = j->m_log.get(); // intentional hide outer info_log
        DEBG("%s: fork to child time = %f sec", attempt_dir, pf.sf(t5, t6));
        run();
        ::fflush(nullptr); // flush all FILE streams
        TRAC("%s: exiting child process(pid=%d)", attempt_dir, getpid());
      //::exit(0); // will hang in some global destructors
        ::_exit(0); // done in child process
      }
      else if (pid < 0) {
        ERROR("%s: fork() = %m", attempt_dir);
        fclose(in);
        fclose(out);
      }
      else { // parent process
        fclose(in);
        fclose(out);
        j->child_pid = pid;
        DEBG("%s: fork to parent time = %f sec", attempt_dir, pf.sf(t5, t6));
        int status = 0;
        pid_t wpid = waitpid(pid, &status, 0);
        if (wpid < 0) {
          ERROR("%s: waitpid(pid=%d) = {status = %d, err = %m}", attempt_dir, pid, status);
        } else if (WIFEXITED(status)) {
          DEBG("%s: waitpid(pid=%d) exit with status = %d", attempt_dir, pid, WEXITSTATUS(status));
        } else if (WIFSIGNALED(status)) {
          if (WCOREDUMP(status))
            WARN("%s: waitpid(pid=%d) coredump by signal %d", attempt_dir, pid, WTERMSIG(status));
          else
            WARN("%s: waitpid(pid=%d) killed by signal %d", attempt_dir, pid, WTERMSIG(status));
          DeleteTempFiles(pid, info_log);
        } else if (WIFSTOPPED(status)) {
          WARN("%s: waitpid(pid=%d) stop signal = %d", attempt_dir, pid, WSTOPSIG(status));
        } else if (WIFCONTINUED(status)) {
          WARN("%s: waitpid(pid=%d) continue status = %d(%#X)", attempt_dir, pid, status, status);
        } else {
          WARN("%s: waitpid(pid=%d) other status = %d(%#X)", attempt_dir, pid, status, status);
        }
      }
    }
    else {
      run();
    }

    j->NotifyEtcd();
    if (NFS_DYNAMIC_MOUNT) {
      g_mnt_map.lru_release(lru_handle);
    }
    //INFO("after lru_release");
    g_jobsFinished.fetch_add(1, std::memory_order_relaxed);
    g_jobsRunning.fetch_sub(n_subcompacts, std::memory_order_relaxed);
    g_acceptedJobs.del(j.get());
    //INFO("after g_acceptedJobs.del");
  }, n_subcompacts});
}

static void DeleteTempFiles(pid_t pid, Logger* info_log) {
  const char* tmpdir = getenv("ToplingZipTable_localTempDir");
  TERARK_VERIFY_F(nullptr != tmpdir, "env ToplingZipTable_localTempDir must be defined");
  auto prefix = terark::string_appender<>()|"Topling-"|pid|"-";
  using namespace std::filesystem;
  std::error_code ec;
  directory_iterator dir(tmpdir, ec);
  TERARK_VERIFY_S(!ec, "directory_iterator(%s) = %s", tmpdir, ec.message());
  for (auto& ent : dir) {
    auto path = ent.path();
    if (path.has_stem()) {
      std::string stem = ent.path().stem().string();
      if (Slice(stem).starts_with(prefix)) {
        if (std::filesystem::remove(path, ec))
          INFO("remove(%s) = ok", path.string());
        else
          WARN("remove(%s) = fail(%s)", path.string(), ec.message());
      }
    }
  }
}

}; // class Job

std::pair<size_t, bool>
AcceptedJobsMap::add(Job* j) noexcept {
  const auto key = BuildMetaKey(j->m_meta);
  mtx.lock();
  auto ib = map.insert_i(key, j);
  mtx.unlock();
  return ib;
}

intrusive_ptr<Job>
AcceptedJobsMap::find(const DcompactMeta& meta) const noexcept {
  const auto key = BuildMetaKey(meta);
  intrusive_ptr<Job> p;
  mtx.lock();
  auto idx = map.find_i(key);
  if (map.end_i() != idx) {
    p = map.val(idx);
  }
  mtx.unlock();
  return p;
}

void AcceptedJobsMap::del(Job* j) noexcept {
  const auto key = BuildMetaKey(j->m_meta);
  mtx.lock();
  TERARK_VERIFY_S(map.erase(key), "key = %s", key);
  mtx.unlock();
}

__attribute__((weak)) void AutoStartZipServer();

} // namespace ROCKSDB_NAMESPACE

int main(int argc, char* argv[]) {
  unsetenv("LD_PRELOAD");
  if (ROCKSDB_NAMESPACE::AutoStartZipServer) {
    ROCKSDB_NAMESPACE::AutoStartZipServer();
  }
  return ROCKSDB_NAMESPACE::Job::main(argc, argv);
}
