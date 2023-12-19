//
// Created by leipeng on 2021/3/26.
//
#include "dcompact_executor.h"
#include <logging/logging.h>
#include <topling/side_plugin_factory.h>

#include <terark/io/FileStream.hpp>
#include <terark/num_to_str.hpp>
#include <terark/util/atomic.hpp>
#include <terark/util/function.hpp>
#include <terark/util/linebuf.hpp>
#include <terark/util/process.hpp>
#include <terark/util/refcount.hpp>
#include <terark/lcast.hpp>
#include <terark/valvec.hpp>
#include <boost/intrusive_ptr.hpp>

#ifdef TOPLING_DCOMPACT_USE_ETCD
#undef __declspec // defined in port_posix.h and etcd/Client.h's include
#if defined(__clang__) || defined(__GNUC__) || defined(__GNUG__)
  #pragma GCC diagnostic push
  #pragma GCC diagnostic ignored "-Wnon-virtual-dtor"
  #include <etcd/Client.hpp>
  #include <etcd/Watcher.hpp>
  #pragma GCC diagnostic pop
#else
  #include <etcd/Client.hpp>
  #include <etcd/Watcher.hpp>
#endif
#endif // TOPLING_DCOMPACT_USE_ETCD

#include <curl/curl.h>
#if LIBCURL_VERSION_MAJOR * 10000 + LIBCURL_VERSION_MINOR * 10 >= 70440
#else
  #define CURLSSLOPT_NO_REVOKE 0
#endif

#include <filesystem>
#include <random>

#if 0
#define Err(fmt, ...) fprintf(stderr, "%s:%d: " fmt "\n", \
                              RocksLogShorterFileName(__FILE__), \
                              __LINE__, ##__VA_ARGS__)
#else
#define Err(fmt, ...) ROCKS_LOG_ERROR(m_log, "%s" fmt, TERARK_PP_SmartForPrintf("", ##__VA_ARGS__))
#endif

#define DEBG(fmt, ...) ROCKS_LOG_DEBUG(m_log, "%s" fmt, TERARK_PP_SmartForPrintf("", ##__VA_ARGS__))
#define INFO(fmt, ...) ROCKS_LOG_INFO(m_log, "%s" fmt, TERARK_PP_SmartForPrintf("", ##__VA_ARGS__))
#define WARN(fmt, ...) ROCKS_LOG_WARN(m_log, "%s" fmt, TERARK_PP_SmartForPrintf("", ##__VA_ARGS__))

extern const char* rocksdb_build_git_sha;
namespace ROCKSDB_NAMESPACE {

using namespace terark;

#define ToStr(...) std::string(buf, snprintf(buf, sizeof(buf), __VA_ARGS__))
#define AddFmt(str,...) str.append(buf,snprintf(buf,sizeof(buf),__VA_ARGS__))
#define CatFmt(str,...) (str + terark::fstring(buf,snprintf(buf,sizeof(buf),__VA_ARGS__)))

#ifdef TOPLING_DCOMPACT_USE_ETCD
struct EtcdConnectionParams {
  std::string url;
  std::string load_balancer = "round_robin";
  std::string username, password;
  std::string ca;   // connect by {ca, cert, key}
  std::string cert; // can be empty
  std::string key;  // can be empty

  explicit EtcdConnectionParams(const json& js) {
    ROCKSDB_JSON_REQ_PROP(js, url);
    ROCKSDB_JSON_OPT_PROP(js, load_balancer);
    if (js.contains("ca")) {
      ROCKSDB_JSON_REQ_PROP(js, ca);
      ROCKSDB_JSON_OPT_PROP(js, cert);
      ROCKSDB_JSON_OPT_PROP(js, key);
      if (ca.empty()) {
        THROW_InvalidArgument("ca must not be empty");
      }
    }
    else if (js.contains("username")) {
      ROCKSDB_JSON_REQ_PROP(js, username);
      ROCKSDB_JSON_REQ_PROP(js, password);
      if (username.empty()) {
        THROW_InvalidArgument("username must not be empty");
      }
    }
  }
  void ToJson(json& djs) {
    ROCKSDB_JSON_SET_PROP(djs, url);
    ROCKSDB_JSON_SET_PROP(djs, load_balancer);
    ROCKSDB_JSON_SET_PROP(djs, username);
  }

  etcd::Client* Connect() {
    if (!ca.empty()) {
      return new etcd::Client(url, ca, cert, key, load_balancer);
    }
    else if (!username.empty()) {
      return new etcd::Client(url, username, password, load_balancer);
    }
    else {
      return new etcd::Client(url, load_balancer);
    }
  }
};
#endif

class DcompactEtcdExec; // forward declare

class IgnoreCopyMutex : public std::mutex {
public:
  IgnoreCopyMutex() = default;
  IgnoreCopyMutex(const IgnoreCopyMutex&) {}
  IgnoreCopyMutex& operator=(const IgnoreCopyMutex&) { return *this; }
};
struct HttpParams {
  std::string web_url;
  std::string base_url;
  std::string url;
  std::string ca; // ca file
  std::string cert; // can be empty
  std::string key;  // can be empty
  unsigned weight = 100;
  mutable unsigned hits = 0;
  mutable unsigned live = 0;

  struct Labour : hash_strmap<gold_hash_set<DcompactEtcdExec*> > {
  #if 0
    size_t sum_runnings() const {
      size_t num = 0;
      for (size_t i = 0, n = this->end_i(); i < n; ++i) {
        if (!this->is_deleted(i))
          num += this->val(i).size();
      }
      return num;
    }
  #endif
    size_t m_sum_runnings = 0; // sum all jobs for all db
  };
  struct RunningMap : hash_strmap<Labour> {
    IgnoreCopyMutex m_mtx;
  };
  mutable RunningMap  m_running;
#define m_running_mtx m_running.m_mtx

  void FromJson(const json& js) {
    if (js.is_string()) {
      url = js.get_ref<const std::string&>();
      ca.clear();
    }
    else if (js.is_object()) {
      ROCKSDB_JSON_OPT_PROP(js, web_url);
      ROCKSDB_JSON_OPT_PROP(js, base_url);
      ROCKSDB_JSON_REQ_PROP(js, url);
      ROCKSDB_JSON_OPT_PROP(js, ca);
      ROCKSDB_JSON_OPT_PROP(js, cert);
      ROCKSDB_JSON_OPT_PROP(js, key);
      if (IsComment(url)) {
        return;
      }
      if (!ca.empty() && !Slice(url).starts_with("https://")) {
        THROW_InvalidArgument("{url,ca}.url must be https when ca is set");
      }
      ROCKSDB_JSON_OPT_PROP(js, weight);
      weight = std::max(weight, 1u);
    }
    else {
      THROW_InvalidArgument("json must be a string or object{url,ca}");
    }
    // url in json is required, but 'this->url' is just for submit "/dcompact"
    // base_url is used for:
    //   1. ${base_url}/probe
    //   2. ${base_url}/shutdown
    if (base_url.empty()) {
      // by default, ${url} == ${base_url}/dcompact
      if (Slice(url).ends_with("/dcompact")) {
        base_url = url.substr(0, url.size() - strlen("/dcompact"));
      }
      else {
        base_url = std::move(url);
        url = base_url + "/dcompact";
      }
    }
    else {
      // base_url is defined in json
      if (!Slice(url).ends_with("/dcompact")) {
        url += "/dcompact";
      }
    }
    if (web_url.empty()) {
      web_url = base_url;
    }
  }
  json ToJson(bool with_ca, bool html) const {
    json js;
    ROCKSDB_JSON_SET_PROP(js, url);
    ROCKSDB_JSON_SET_PROP(js, weight);
    if (html) {
      string_appender<> oss;
      oss|"<a href='"|web_url|"/stat'>"|hits|"</a>";
      js["hits"] = oss.str();
      oss.clear();
      oss|"<a href='"|web_url|"/list'>"|live|"</a>";
      js["live"] = std::move(oss.str());
    } else {
      ROCKSDB_JSON_SET_PROP(js, hits);
      ROCKSDB_JSON_SET_PROP(js, live);
    }
    if (with_ca) {
      js["ca"] = ca.empty() ? "N" : "Y";
    }
    return js;
  }
  static bool IsComment(Slice str) {
    return str.starts_with("//") || str.starts_with("#");
  }
  static void PushToVec(const json& js, std::vector<std::shared_ptr<HttpParams> >* v) {
      auto x = std::make_shared<HttpParams>(); x->FromJson(js);
      if (!IsComment(x->url)) {
        v->push_back(std::move(x));
      }
  }
  static void ParseJsonToVec(const json& js, std::vector<std::shared_ptr<HttpParams> >* v) {
    if (js.is_array()) {
      for (auto& one_js : js) {
        PushToVec(one_js, v);
      }
    }
    else {
      PushToVec(js, v);
    }
  }
  static json DumpVecToJson(const std::vector<std::shared_ptr<HttpParams> >& v, bool html) {
    json js;
    bool with_ca = false;
    for (auto& one : v) {
      if (!one->ca.empty()) { with_ca = true; break; }
    }
    for (auto& one : v) {
      js.push_back(one->ToJson(with_ca, html));
    }
    if (html) {
      json& cols = js[0]["<htmltab:col>"];
      cols.push_back("url");
      cols.push_back("weight");
      cols.push_back("hits");
      cols.push_back("live");
      if (with_ca)
        cols.push_back("ca");
    }
    return js;
  }
};

static std::deque<std::string> g_delayed_rm_queue;
static std::mutex              g_delayed_rm_queue_mtx;
static void perform_delayed_rm(Logger* m_log) {
  for (;;) {
    std::string dir;
    g_delayed_rm_queue_mtx.lock();
    if (!g_delayed_rm_queue.empty()) {
      dir = std::move(g_delayed_rm_queue.front());
      g_delayed_rm_queue.pop_front();
    }
    g_delayed_rm_queue_mtx.unlock();
    if (dir.empty()) {
      break; // done
    }
    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
    if (ec.value() == (int)std::errc::no_such_file_or_directory) {
      INFO("remove_all(%s) = no_such_file_or_directory", dir);
    }
    else if (ec.value() == 0) {
      INFO("remove_all(%s) = success", dir);
    }
    else {
      INFO("remove_all(%s) = %s", dir, ec.message());
      g_delayed_rm_queue_mtx.lock();
      g_delayed_rm_queue.push_back(dir); // re-queue
      g_delayed_rm_queue_mtx.unlock();
      break; // done
    }
  }
}

static uint64_t fee_units(uint64_t raw, uint64_t zip) {
  return uint64_t(sqrt(double(raw) * zip) / 1e6);
}
struct DcompactStatItem {
  size_t   num_cumu_exec = 0;
  size_t   num_live_exec = 0;
  double   m_sum_compact_time_us = 0; // speed in bytes-per-second
  uint64_t m_sum_input_raw_key_bytes = 0;
  uint64_t m_sum_input_raw_val_bytes = 0;
  uint64_t m_sum_input_zip_kv_bytes = 0;
  uint64_t m_last_access = 0;

  void add(const DcompactStatItem& y) {
    num_cumu_exec += y.num_cumu_exec;
    num_live_exec += y.num_live_exec;
    m_sum_compact_time_us += y.m_sum_compact_time_us;
    m_sum_input_raw_key_bytes += y.m_sum_input_raw_key_bytes;
    m_sum_input_raw_val_bytes += y.m_sum_input_raw_val_bytes;
    m_sum_input_zip_kv_bytes  += y.m_sum_input_zip_kv_bytes;
  }

  uint64_t sum_input_raw_bytes() const {
    return m_sum_input_raw_key_bytes + m_sum_input_raw_val_bytes;
  }
  uint64_t sum_input_zip_bytes() const {
    return m_sum_input_zip_kv_bytes;
  }

  double rt_estimate_speed_mb(size_t estimate_speed) const {
    double speed = sum_input_raw_bytes()
                  ? sum_input_raw_bytes() / m_sum_compact_time_us
                  : estimate_speed / 1e6;
    if (speed > 100.0) {
      // maybe all are deletes, or all data are deleted by range_del.
      // protect for such cases
      speed = 100; // 100 MB/sec
    }
    return speed;
  }

  json ToJson(std::string&& name) {
    char buf[64];
    double time_usec = m_sum_compact_time_us + 1;
    llong  kv_bytes = m_sum_input_raw_key_bytes + m_sum_input_raw_val_bytes;
    time_t rawtime = m_last_access / 1000000;
    struct tm t; // NOLINT
    struct tm* timeinfo = localtime_r(&rawtime, &t);
    std::string last_ac(buf, strftime(buf, sizeof(buf), "%F %T", timeinfo));
    return json{
        {"Name", std::move(name)},
        {"Finish", num_cumu_exec},
        {"Live", num_live_exec},
        {"FeeUnits", fee_units(kv_bytes, m_sum_input_zip_kv_bytes)},
        {"LastAccess", last_ac},
        {"Time(sec)", ToStr("%.3f", time_usec/1e6)},
        {"Speed", ToStr("%.3f MB/sec", kv_bytes/time_usec)},
        {"KeyBytes", SizeToString(m_sum_input_raw_key_bytes)},
        {"ValBytes", SizeToString(m_sum_input_raw_val_bytes)},
        {"KVBytes", SizeToString(kv_bytes)},
        {"Key/KV", ToStr("%.5f", m_sum_input_raw_key_bytes/double(kv_bytes))},
        {"AVG:Bytes", SizeToString(num_cumu_exec ? kv_bytes/num_cumu_exec : 0)},
    };
  }
};

struct DcompactStatMap : hash_strmap<DcompactStatItem> {
  DcompactStatMap() {
    this->enable_freelist();
  }
  json ToJson(bool html) {
    json js = json::array();
    DcompactStatItem sum;
    std::vector<std::pair<std::string, DcompactStatItem> > snapshot;
    snapshot.reserve(this->end_i() + 1); // 1 for rare extra
    {
      std::lock_guard<std::mutex> lock(m_mtx);
      for (size_t i = 0; i < this->end_i(); ++i) {
        if (!this->is_deleted(i)) {
          fstring name = this->key(i);
          auto&   item = this->val(i);
          snapshot.emplace_back(name.str(), item);
        }
      }
      sum = m_dead;
    }
    std::sort(snapshot.begin(), snapshot.end(), TERARK_CMP(second.m_last_access, >));
    for (auto& kv : snapshot) {
      maximize(sum.m_last_access, kv.second.m_last_access);
      sum.add(kv.second);
      js.push_back(kv.second.ToJson(std::move(kv.first)));
    }
    if (!html || snapshot.size() != 1 || m_dead.num_cumu_exec) {
      if (m_dead.num_cumu_exec) {
        js.push_back(m_dead.ToJson("discard"));
      }
      js.push_back(sum.ToJson("sum"));
    }
    if (html) {
      js[0]["<htmltab:col>"] = json::array({
        "Name", "Finish", "Live", "FeeUnits", "LastAccess", "Time(sec)", "Speed",
        "KeyBytes", "ValBytes", "KVBytes", "Key/KV", "AVG:Bytes"
      });
    }
    return js;
  }
  void RemoveOldestInLock(size_t num_cumu_exec) {
    size_t oldest_idx = size_t(-1);
    uint64_t oldest_ac = UINT64_MAX;
    for (size_t i = 0; i < this->end_i(); ++i) {
      if (!this->is_deleted(i) && 0 == this->val(i).num_live_exec) {
        uint64_t ac = this->val(i).m_last_access;
        if (ac < oldest_ac && val(i).num_cumu_exec * 3 < num_cumu_exec) {
          oldest_idx = i;
          oldest_ac = ac;
        }
      }
    }
    if (size_t(-1) != oldest_idx) {
      m_dead.add(this->val(oldest_idx));
      this->erase_i(oldest_idx);
    }
  }
  DcompactStatItem m_dead;
  IgnoreCopyMutex m_mtx;
};

struct DcompactFeeConfig {
  std::string url;
  std::string ca;
  std::string key;
  std::string cert;
  std::string provider;
  std::string instanceId;

  explicit DcompactFeeConfig(const json& js) {
    ROCKSDB_JSON_REQ_PROP(js, url);
    ROCKSDB_JSON_OPT_PROP(js, ca);
    ROCKSDB_JSON_OPT_PROP(js, key);
    ROCKSDB_JSON_OPT_PROP(js, cert);
    ROCKSDB_JSON_REQ_PROP(js, provider);
    ROCKSDB_JSON_OPT_PROP(js, instanceId);
  }
  json ToJson() const {
    json djs;
    ROCKSDB_JSON_SET_PROP(djs, url);
    ROCKSDB_JSON_SET_PROP(djs, ca);
    ROCKSDB_JSON_SET_PROP(djs, key);
    ROCKSDB_JSON_SET_PROP(djs, cert);
    ROCKSDB_JSON_SET_PROP(djs, provider);
    ROCKSDB_JSON_SET_PROP(djs, instanceId);
    return djs;
  }
};

json DcompactFeeReport::ToJson() const {
  json djs;
//ROCKSDB_JSON_SET_PROP(djs, provider); // temporary remove provider
  ROCKSDB_JSON_SET_PROP(djs, instanceId);
  ROCKSDB_JSON_SET_PROP(djs, labourId);
  ROCKSDB_JSON_SET_PROP(djs, dbId);
  ROCKSDB_JSON_SET_PROP(djs, dbStarts);
  ROCKSDB_JSON_SET_PROP(djs, starts);
  ROCKSDB_JSON_SET_PROP(djs, executesMs);
  ROCKSDB_JSON_SET_PROP(djs, compactionJobId);
  ROCKSDB_JSON_SET_PROP(djs, attempt);
  ROCKSDB_JSON_SET_PROP(djs, compactionInputRawBytes);
  ROCKSDB_JSON_SET_PROP(djs, compactionInputZipBytes);
  ROCKSDB_JSON_SET_PROP(djs, compactionOutputRawBytes);
  ROCKSDB_JSON_SET_PROP(djs, compactionOutputZipBytes);
  return djs;
}

ROCKSDB_ENUM_CLASS(LoadBalanceType, int, kRoundRobin, kWeight);
/* DONT make it too complex
struct AlertMetaShare : RefCounter {
  std::string labour_id;
  std::string full_server_id;
};
constexpr auto CmpAlertMetaShared = TERARK_CMP_P(labour_id, full_server_id);
using AlertMetaSharePtr = boost::intrusive_ptr<AlertMetaShare>;
struct AlertMeta {
  int job_id;
  int attempt;
  time_t alert_time;
  AlertMetaSharePtr alert_meta;
};
*/

class DcompactEtcdExecFactory final : public CompactExecFactoryCommon {
 public:
  Env* m_env = Env::Default();
  std::string etcd_url;
  std::string etcd_root;
  std::vector<std::shared_ptr<HttpParams> > http_workers; // http or https
  std::string nfs_type = "nfs"; // default is nfs, can be glusterfs...
  std::string nfs_mnt_src;
  std::string nfs_mnt_opt = "noatime";
  std::string alert_email;
  std::string alert_http;
  uint32_t    alert_interval = 60; // seconds
  std::string web_domain; // now just for iframe auto height
  std::string job_url_root;
  std::string m_start_time;
  uint64_t    m_start_time_epoch; // for ReportFee
  size_t estimate_speed = 10e6; // speed in bytes-per-second
  size_t max_book_dbcf = 20;
  bool copy_sst_files = false;
  float timeout_multiplier = 10.0;
  int http_max_retry = 3;
  int http_timeout = 3; // in seconds
  int overall_timeout = 5; // in seconds
  int retry_sleep_time = 1; // in seconds
  std::shared_ptr<DcompactFeeConfig> fee_conf;
  valvec<unsigned> m_weight_vec;
  mutable IgnoreCopyMutex m_rand_gen_mtx;
  mutable std::mt19937_64 m_rand_gen;
  unsigned m_weight_sum = 0;
  LoadBalanceType load_balance = LoadBalanceType::kRoundRobin;
  json dcompact_http_headers;

#ifdef TOPLING_DCOMPACT_USE_ETCD
  etcd::Client* m_etcd = nullptr;
  json m_etcd_js;
#endif

  mutable uint64_t m_last_alert_time_us = 0;
  // std::mutex m_alert_mtx;
  // std::set<AlertMetaSharePtr, decltype(CmpAlertMetaShared)>
  //   m_alert_share_set{CmpAlertMetaShared};

  // round robin http_workers, to avoid a http load balancer.
  // http_workers url can also be a proxy or load balancer, thus forms a
  // 2-level load balancer:
  //   - level-1 is this simple round robin
  //   - level-2 is a load balancer http_workers[x]
  mutable size_t m_round_robin_idx = 0; // this is a variable, not a config

  mutable DcompactStatMap  m_stat_map;
  mutable DcompactStatItem m_stat_sum;
  //Histogram m_histogram;

  double rt_estimate_speed_mb(size_t idx) const {
    m_stat_map.m_mtx.lock();
    double r = m_stat_map.val(idx).rt_estimate_speed_mb(estimate_speed);
    m_stat_map.m_mtx.unlock();
    return r;
  }

  DcompactEtcdExecFactory(const json& js, const SidePluginRepo& repo) {
    if (!TemplatePropLoadFromJson<CompactionExecutorFactory>(this, js, repo)) {
      time_t rawtime = time(nullptr);
      struct tm  tm_storage;
      struct tm* tp = localtime_r(&rawtime, &tm_storage);
      size_t cap = 64;
      m_start_time.resize(cap);
      m_start_time.resize(strftime(&m_start_time[0], cap, "%FT%H.%M.%S", tp));
      m_start_time.shrink_to_fit();
      m_start_time_epoch = rawtime;
    }
    CompactExecFactoryCommon::init(js, repo);
    ROCKSDB_JSON_OPT_PROP(js, copy_sst_files);
    ROCKSDB_JSON_OPT_PROP(js, alert_email);
    ROCKSDB_JSON_OPT_PROP(js, alert_http);
    ROCKSDB_JSON_OPT_PROP(js, web_domain);
    ROCKSDB_JSON_OPT_PROP(js, job_url_root);
    ROCKSDB_JSON_OPT_PROP(js, etcd_root);
    Update(js);
    if (!js.contains("http_workers")) {
      if (http_workers.empty()) // if from template, it is not empty
        THROW_InvalidArgument("json[\"http_workers\"] is required");
    }
    else {
      http_workers.clear(); // if has defined in template, overwrite it
      HttpParams::ParseJsonToVec(js["http_workers"], &http_workers);
      m_weight_vec.reserve(http_workers.size());
      unsigned sum = 0;
      for (auto& x : http_workers) {
        sum += x->weight;
        m_weight_vec.push_back(sum);
      }
      m_weight_sum = sum;
    }
    ROCKSDB_JSON_OPT_PROP(js, nfs_type);
    ROCKSDB_JSON_OPT_PROP(js, nfs_mnt_src);
    ROCKSDB_JSON_OPT_PROP(js, nfs_mnt_opt);
#ifdef TOPLING_DCOMPACT_USE_ETCD
    if (!js.contains("etcd")) {
      //THROW_InvalidArgument("json[\"etcd\"] is required");
      fprintf(stderr, "WARN: DcompactEtcd: etcd is not defined, ignored\n");
    }
    else {
      m_etcd_js = js["etcd"];
      EtcdConnectionParams ecp(m_etcd_js);
      m_etcd = ecp.Connect();
      etcd_url = ecp.url;
    }
#endif
    if (auto iter = js.find("fee_conf"); js.end() != iter) {
      auto& js_fee = iter.value();
      if (!js_fee.is_object()) {
        THROW_InvalidArgument("json[\"fee_conf\"] must be an object");
      }
      fee_conf.reset(new DcompactFeeConfig(js_fee));
    }
    if (http_workers.empty()) {
      THROW_InvalidArgument("http_workers must not be empty");
    }
    if (auto iter = js.find("dcompact_http_headers"); js.end() != iter) {
      dcompact_http_headers = iter.value();
      if (!dcompact_http_headers.is_object()) {
        THROW_InvalidArgument("dcompact_http_headers must be an json object");
      }
      auto it = dcompact_http_headers.begin();
      for (; dcompact_http_headers.end() != it; ++it) {
        if (!it.value().is_string())
          THROW_InvalidArgument("dcompact_http_headers[" + it.key() + "] is not a string");
      }
    }
    // m_round_robin_idx - start at a random idx
    auto seed = std::chrono::system_clock::now().time_since_epoch().count();
    m_rand_gen = std::mt19937_64(seed);
    m_round_robin_idx = m_rand_gen() % http_workers.size();
  }
  ~DcompactEtcdExecFactory() override {
#ifdef TOPLING_DCOMPACT_USE_ETCD
    delete m_etcd;
#endif
  }
  size_t PickWorker() const {
    if (http_workers.size() == 1) {
      return 0;
    }
    if (LoadBalanceType::kRoundRobin == load_balance) {
      return as_atomic(m_round_robin_idx)
                      .fetch_add(1, std::memory_order_relaxed)
                      % http_workers.size();
    } else {
      assert(LoadBalanceType::kWeight == load_balance);
      m_rand_gen_mtx.lock();
      auto rand = m_rand_gen();
      m_rand_gen_mtx.unlock();
      return lower_bound_a(m_weight_vec, rand % m_weight_sum);
    }
  }
  CompactionExecutor* NewExecutor(const Compaction*) const final;
  const char* Name() const final { return "DcompactEtcd"; }
  void ToJson(const json& dump_options, json& djs, const SidePluginRepo& repo) const final {
    if (int cols = JsonSmartInt(dump_options, "cols", 0)) {
      djs = WorkersView(dump_options, cols);
      return;
    }
    bool html = JsonSmartBool(dump_options, "html", true);
    if (html) {
      const auto document =
        "<a href='https://github.com/topling/topling-dcompact/blob/main/README.md'>Document(English)</a>"
        " | "
        "<a href='https://github.com/topling/topling-dcompact/blob/main/README-zh_CN.md'>文档(中文)</a>"
        ;
      ROCKSDB_JSON_SET_PROP(djs, document);
    }
    ROCKSDB_JSON_SET_TMPL(djs, compaction_executor_factory);
    char buf[64];
    djs["estimate_speed"] = ToStr("%.3f MB/sec", estimate_speed/1e6);
    ROCKSDB_JSON_SET_PROP(djs, timeout_multiplier);
    djs["stat"] = m_stat_map.ToJson(html);
    ROCKSDB_JSON_SET_PROP(djs, copy_sst_files);
    ROCKSDB_JSON_SET_PROP(djs, etcd_root);
    ROCKSDB_JSON_SET_PROP(djs, overall_timeout);
    ROCKSDB_JSON_SET_PROP(djs, http_timeout);
    ROCKSDB_JSON_SET_PROP(djs, http_max_retry);
    ROCKSDB_JSON_SET_PROP(djs, max_book_dbcf);
    ROCKSDB_JSON_SET_PROP(djs, retry_sleep_time);
    ROCKSDB_JSON_SET_PROP(djs, alert_email);
    ROCKSDB_JSON_SET_PROP(djs, alert_http);
    ROCKSDB_JSON_SET_PROP(djs, alert_interval);
    ROCKSDB_JSON_SET_PROP(djs, web_domain);
    ROCKSDB_JSON_SET_PROP(djs, job_url_root);
    djs[html ? R"(<a href='javascript:SetParam("cols","3")'>workers</a>)" : "workers"]
        = HttpParams::DumpVecToJson(http_workers, html);
    ROCKSDB_JSON_SET_ENUM(djs, load_balance);
    ROCKSDB_JSON_SET_PROP(djs, nfs_type);
    ROCKSDB_JSON_SET_PROP(djs, nfs_mnt_src);
    ROCKSDB_JSON_SET_PROP(djs, nfs_mnt_opt);
    CompactExecFactoryCommon::ToJson(dump_options, djs, repo);
    if (fee_conf) {
      djs["fee_conf"] = fee_conf->ToJson();
    }
#ifdef TOPLING_DCOMPACT_USE_ETCD
    auto& etcd_js = djs["etcd"] = m_etcd_js;
    if (etcd_js.contains("password")) {
      etcd_js["password"] = "****";
    }
#endif
    ROCKSDB_JSON_SET_PROP(djs, dcompact_http_headers);
  }
  std::string WorkersView(const json& dump_options, int cols) const;
  std::string JobUrl(const std::string& dbname, int job_id, int attempt) const final {
    std::string str;
    if (!job_url_root.empty()) {
      char buf[64];
      auto len = snprintf(buf, sizeof(buf), "job-%05d/att-%02d", job_id, attempt);
      str.reserve(job_url_root.size() + instance_name.size() + m_start_time.size() + dbname.size() + len + 20);
      (string_appender<>&)(str)
        |job_url_root|"/"|instance_name|"/"|m_start_time|"/"|dbname|"/"|fstring(buf, len)|"/summary.html";
    }
    return str;
  }
  void Update(const json& js) final {
    CompactExecFactoryCommon::Update(js);
    ROCKSDB_JSON_OPT_PROP(js, timeout_multiplier);
    ROCKSDB_JSON_OPT_SIZE(js, estimate_speed);
    ROCKSDB_JSON_OPT_PROP(js, overall_timeout);
    ROCKSDB_JSON_OPT_PROP(js, http_timeout);
    ROCKSDB_JSON_OPT_PROP(js, http_max_retry);
    ROCKSDB_JSON_OPT_ENUM(js, load_balance);
    ROCKSDB_JSON_OPT_PROP(js, max_book_dbcf);
    ROCKSDB_JSON_OPT_PROP(js, retry_sleep_time);
    ROCKSDB_JSON_OPT_PROP(js, alert_interval);
    maximize(overall_timeout, http_timeout);
  }
};

class DcompactEtcdExec : public CompactExecCommon {
  friend class DcompactEtcdExecFactory;
  size_t m_stat_idx = size_t(-1);
  int m_attempt = 0;
  bool m_done = false;
  const HttpParams* m_worker = nullptr;
  std::string m_labour_id;
  std::string m_full_server_id;
  std::vector<std::string> m_copyed_files;
  string_appender<> m_url;
  DcompactMeta meta;
  uint64_t m_input_raw_key_bytes = 0;
  uint64_t m_input_raw_val_bytes = 0;
  uint64_t m_input_zip_kv_bytes = 0;
  uint64_t m_start_ts = 0;
  uint64_t input_raw_bytes() const {
    return m_input_raw_key_bytes + m_input_raw_val_bytes;
  }
  void PostHttp(const std::string& urlstr, fstring body);
  void AlertDcompactFail(const Status&);
 public:
  explicit DcompactEtcdExec(const DcompactEtcdExecFactory* fac);
  Status SubmitHttp(const fstring action, const std::string& meta_jstr, size_t nth_http) noexcept;
  Status RenameFile(const std::string& src, const std::string& dst, off_t fsize) override;
  Status MaybeCopyFiles(const CompactionParams& params);
  Status Execute(const CompactionParams&, CompactionResults*) override;
  Status Attempt(const CompactionParams&, CompactionResults*);
  void CleanFiles(const CompactionParams&, const CompactionResults&) override;
  void ReportFee(const CompactionParams&, const CompactionResults&);
};

void DcompactEtcdExec::AlertDcompactFail(const Status& s) {
  auto f = static_cast<const DcompactEtcdExecFactory*>(m_factory);
  if (f->alert_email.empty() && f->alert_http.empty()) {
    return;
  }
  uint64_t now = f->m_env->NowMicros();
  uint64_t prev = f->m_last_alert_time_us;
  if (now < prev + f->alert_interval * 1000000) {
    INFO("Skip alert: job-%05d/att-%02d", meta.job_id, m_attempt);
    return;
  }
  f->m_last_alert_time_us = now;
  string_appender<> title;
  title|"instance "|f->instance_name|": dcompact fail, ";
  if (f->allow_fallback_to_local) {
    title|"fallback to local";
  } else {
    title|"die because allow_fallback_to_local is false";
  }
  json bjs;
  bjs["title"] = title;
  bjs["status"] = s.ToString();
  bjs["last_url"] = m_url;
  bjs["instance_name"] = f->instance_name;
  bjs["full_server_id"] = m_full_server_id;
  bjs["labour_id"] = m_labour_id;
  bjs["meta"] = meta.ToJsonObj();
  std::string body = bjs.dump();
  if (!f->alert_email.empty()) {
    std::string cmd = "mail -s '" + title + "' " + f->alert_email;
    std::string res = vfork_cmd(cmd, body).get();
    if (!res.empty()) {
      INFO("cmd %s output %s", cmd, res);
    }
  }
  if (!f->alert_http.empty()) {
    PostHttp(f->alert_http, body);
  }
}

Status DcompactEtcdExec::RenameFile(const std::string& src,
                                    const std::string& dst,
                                    off_t fsize) {
  auto f = static_cast<const DcompactEtcdExecFactory*>(m_factory);
  if (f->copy_sst_files) {
    if (Status st = CopyOneFile(src, dst, fsize); st.ok())
      return m_env->DeleteFile(src);
    else
      return st;
  }
  return CompactExecCommon::RenameFile(src, dst, fsize);
}

Status DcompactEtcdExec::MaybeCopyFiles(const CompactionParams& params) {
  auto f = static_cast<const DcompactEtcdExecFactory*>(m_factory);
  if (!f->copy_sst_files)
    return Status::OK();
  for (auto& cf_path : params.cf_paths)
    TERARK_VERIFY_S(!Slice(cf_path.path).starts_with(f->hoster_root),
                    "%s : %s", cf_path.path, f->hoster_root);
  const std::string& dbpath = params.dbname;
  std::string dbname = basename(dbpath.c_str());
  std::string dir = f->hoster_root + "/" + dbname;
  auto t0 = m_env->NowMicros();
  m_env->CreateDirIfMissing(dir);
  dir += "/";
  dir += params.cf_name;
  if (auto s = m_env->CreateDirIfMissing(dir); !s.ok())
    return s;
  auto& cf_paths = const_cast<CompactionParams&>(params).cf_paths;
  size_t sum_size = 0;
  for (auto& lev : *params.inputs) {
    for (FileMetaData* file : lev.files) {
      FileDescriptor& fd = file->fd;
      sum_size += fd.file_size;
      auto src = TableFileName(cf_paths, fd.GetNumber(), fd.GetPathId());
      auto dst = MakeTableFileName(dir, fd.GetNumber());
      auto status = CopyOneFile(src, dst, fd.file_size);
      m_copyed_files.push_back(std::move(dst)); // maybe partially copyed
      if (!status.ok())
        return status;
      // do not change packed_number_and_path_id because it is not owned by
      // CompactionParams, but owned by VersionStorage, it will be fixed by
      // dcompact_worker.
      //file->fd.packed_number_and_path_id &= kFileNumberMask;
    }
  }
  cf_paths = {{dir, UINT_MAX}};
  auto t1 = m_env->NowMicros();
  INFO("MaybeCopyFiles(%s[%s]/job-%05d) time %.3f sec, size %12s, speed %7.3f MB/s",
       dbname, params.cf_name, params.job_id, (t1-t0)/1e6,
       SizeToString(sum_size), double(sum_size)/(t1-t0));
  return Status::OK();
}

Status DcompactEtcdExec::Execute(const CompactionParams& params,
                                       CompactionResults* results)
{
  auto f = static_cast<const DcompactEtcdExecFactory*>(m_factory);
  Status s = MaybeCopyFiles(params);
  if (!s.ok()) {
    if (!m_factory->allow_fallback_to_local) {
      TERARK_DIE_S("MaybeCopyFiles(%s[%s]/job-%05d) failed = %s",
        params.dbname, params.cf_name, params.job_id, s.ToString());
    }
    WARN("MaybeCopyFiles(%s[%s]/job-%05d) failed = %s",
        params.dbname, params.cf_name, params.job_id, s.ToString());
    return s;
  }
  for (; m_attempt < f->http_max_retry; m_attempt++) {
    auto shutting_down = m_params->shutting_down;
    if (shutting_down && shutting_down->load(std::memory_order_relaxed)) {
      s = Status::ShutdownInProgress();
      goto Done;
    }
    s = Attempt(params, results);
    if (s.ok())
      goto Done;
    else if (s.IsBusy())
      m_env->SleepForMicroseconds(f->retry_sleep_time * 1000000);
  }
  AlertDcompactFail(s);
  if (!m_factory->allow_fallback_to_local) {
    TERARK_DIE_S("Fail with MaxRetry = %d, die: %s", m_attempt, s.ToString());
  }
  m_done = true; // fail
  CleanFiles(params, *results);
Done:
  f->m_stat_map.m_mtx.lock();
  ROCKSDB_VERIFY_GE(f->m_stat_map.val(m_stat_idx).num_live_exec, 1);
  f->m_stat_map.val(m_stat_idx).num_live_exec--;
  if (f->m_stat_map.size() > f->max_book_dbcf) {
    f->m_stat_map.RemoveOldestInLock(f->num_cumu_exec);
  }
  f->m_stat_map.m_mtx.unlock();
  return s;
}
template<class T, class U, class W>
T lim(T x, U lo, W hi) {
  if (x < lo) return lo;
  if (hi < x) return hi;
  return x;
}
Status DcompactEtcdExec::Attempt(const CompactionParams& params,
                                 CompactionResults* results)
#ifdef NDEBUG
try
#endif
{
  using namespace std::chrono;
  auto f = static_cast<const DcompactEtcdExecFactory*>(m_factory);
#ifdef TOPLING_DCOMPACT_USE_ETCD
  auto pEtcd = f->m_etcd;
#endif
  for (auto& level_inputs : *params.inputs) {
    for (auto& file : level_inputs.files) {
      file->job_attempt = m_attempt;
    }
  }
  char buf[64];
  results->status = Status::Incomplete("executing command: " + f->etcd_url);
  const std::string& dbpath = params.dbname;
  std::string dbname = basename(dbpath.c_str()); // now for simpliciy
  std::string dbcf_name = dbname + "/" + params.cf_name;
  string_appender<> key;
  key.reserve(256);
  key|f->instance_name|"/"|f->m_start_time|"/"|dbname;
  key^"/job-%05d"^params.job_id^"/att-%02d"^m_attempt;
  meta.n_subcompacts = params.max_subcompactions;
  meta.n_listeners = (uint16_t)params.listeners.size();
  meta.n_prop_coll_factory = (uint16_t)params.table_properties_collector_factories.size();
  {
    auto githash = strchr(rocksdb_build_git_sha, ':');
    ROCKSDB_VERIFY(nullptr != githash);
    githash++; // skip the ':'
    meta.code_githash = githash;
    meta.code_version = ROCKSDB_VERSION;
  }
  meta.job_id = params.job_id;
  meta.attempt = m_attempt;
  meta.output_level = params.output_level;
#ifdef TOPLING_DCOMPACT_USE_ETCD
  meta.etcd_root = f->etcd_root;
#endif
  meta.instance_name = f->instance_name;
  meta.dbname = dbname;
  meta.hoster_root = f->hoster_root;
  meta.output_root = params.cf_paths.back().path;
  meta.nfs_type = f->nfs_type;
  meta.nfs_mnt_src = f->nfs_mnt_src;
  meta.nfs_mnt_opt = f->nfs_mnt_opt;
  meta.start_time = f->m_start_time;
  meta.start_time_epoch = f->m_start_time_epoch;
  std::string output_dir = CatFmt(meta.output_root, "/job-%05d", meta.job_id);
  auto t0 = m_env->NowMicros();
  if (0 == m_attempt) {
    f->m_stat_map.m_mtx.lock();
    m_stat_idx = f->m_stat_map.insert_i(dbcf_name).first;
    f->m_stat_map.val(m_stat_idx).m_last_access = t0;
    f->m_stat_map.val(m_stat_idx).num_live_exec++;
    f->m_stat_map.m_mtx.unlock();
    auto kv_bytes = CalcInputRawBytes(params.inputs);
    m_input_raw_key_bytes = kv_bytes.first;
    m_input_raw_val_bytes = kv_bytes.second;
    m_input_zip_kv_bytes = 0;
    for (auto& level_inputs : *params.inputs) {
      for (auto& file : level_inputs.files)
        m_input_zip_kv_bytes += file->fd.file_size;
    }
    auto s1 = m_env->CreateDir(output_dir);
    TERARK_VERIFY_S(s1.ok(), "%s", s1.ToString());
    std::string params_fname = output_dir + "/rpc.params";
#ifdef NDEBUG
  try {
#endif
    FileStream fp(params_fname, "wb+");
    SerDeWrite(fp, &params);
#ifdef NDEBUG
  } catch (const std::exception& ex) {
    TERARK_DIE_S("file = %s, exception: %s", params_fname, ex);
  } catch (const Status& s) {
    TERARK_DIE_S("file = %s, Status: %s", params_fname, s.ToString());
  } catch (...) {
    TERARK_DIE_S("file = %s, exception: unknown", params_fname);
  }
#endif
  }
  auto t1 = m_env->NowMicros();
  auto s1 = m_env->CreateDir(AddFmt(output_dir, "/att-%02d", m_attempt));
  if (f->allow_fallback_to_local) {
    if (!s1.ok()) {
      Err("CreateDir(%s/att-%02d) = %s", output_dir, m_attempt, s1.ToString());
      return s1;
    }
  } else {
    TERARK_VERIFY_S(s1.ok(), "%s", s1.ToString());
  }
  double speed = f->rt_estimate_speed_mb(m_stat_idx);
  size_t estimate_time_us = (size_t)ceil(input_raw_bytes() / speed);
  meta.estimate_time_us = estimate_time_us;
  std::string meta_jstr = meta.ToJsonStr();
  auto t2 = m_env->NowMicros();
  size_t nth_http = f->PickWorker();
  m_start_ts = t1;
  const HttpParams* worker = f->http_workers[nth_http].get();
  as_atomic(worker->hits).fetch_add(1, std::memory_order_relaxed);
  as_atomic(worker->live).fetch_add(1, std::memory_order_relaxed);
  ROCKSDB_SCOPE_EXIT(
    as_atomic(worker->live).fetch_sub(1, std::memory_order_relaxed);
  );
  const std::string old_labour_id = m_labour_id;
  auto t3 = m_env->NowMicros();
  Status s = SubmitHttp("/dcompact", meta_jstr, nth_http);
  auto t4 = m_env->NowMicros();
  auto d1 = (t1 - t0) / 1e6; // wrt
  auto d2 = (t2 - t1) / 1e6; // mka
  auto d3 = (t3 - t2) / 1e6; // mkw
  auto d4 = (t4 - t3) / 1e6; // curl
  if (!s.ok()) {
    ROCKS_LOG_ERROR(m_log, "job-%05d/att-%02d: init time "
     "wrt = %6.3f, mka = %6.3f, mkw = %6.3f, curl %s = %6.3f sec, err: %s, input = %s",
      meta.job_id, m_attempt, d1, d2, d3, m_url.c_str(), d4, s.ToString().c_str(),
      SizeToString(input_raw_bytes()).c_str());
    CleanFiles(params, *results);
    return s;
  }
  if (m_worker != worker) {
    if (m_worker) {
      m_worker->m_running_mtx.lock();
      auto& labour = m_worker->m_running[old_labour_id];
      TERARK_VERIFY(labour[dbname].erase(this));
      labour.m_sum_runnings--;
      m_worker->m_running_mtx.unlock();
    }
    worker->m_running_mtx.lock();
    auto& labour = worker->m_running[m_labour_id];
    labour[dbname].insert_i(this);
    labour.m_sum_runnings++;
    worker->m_running_mtx.unlock();
    m_worker = worker;
  }

#ifdef TOPLING_DCOMPACT_USE_ETCD
  etcd::Response response;
  std::mutex cond_mtx;
  std::condition_variable cond_var;
  auto result_key = f->etcd_root + "/dcompact-res/" + key;
  TERARK_SCOPE_EXIT(if (pEtcd) pEtcd->rm(result_key)); // ignore rm error
  // 'done' is just for etcd watch
  volatile bool done = false;
  auto callback = [&](etcd::Response&& resp_task) {
    response = std::move(resp_task);
    done = true;
    cond_var.notify_all();
  };
  int from_index = 0;
  std::shared_ptr<etcd::Watcher> watcher;
  if (pEtcd) {
    try {
      watcher.reset(new etcd::Watcher(
        *pEtcd, result_key, from_index, std::ref(callback)));
    }
    catch (const std::exception& ex) {
      Err("Etcd.Watch(%s) : exception: %s", result_key, ex);
    }
  }
#else
  constexpr bool done = false;
#endif
  ROCKS_LOG_INFO(m_log, "job-%05d/att-%02d: init time "
    "wrt = %6.3f, mka = %6.3f, mkw = %6.3f, curl %s = %6.3f sec, input = %s",
    meta.job_id, m_attempt, d1, d2, d3, m_url.c_str(), d4,
    SizeToString(input_raw_bytes()).c_str());
  size_t timeout_us = std::max({
          size_t(f->timeout_multiplier * estimate_time_us),
          size_t(f->http_timeout)*1000*1000,
          size_t(f->overall_timeout)*1000*1000});
  const std::atomic<bool>  shutting_down_always_false{false};
  const std::atomic<bool>* shutting_down = params.shutting_down;
  if (!shutting_down) {
    shutting_down = &shutting_down_always_false;
  }
  auto t5 = m_env->NowMicros();
  size_t done_probe_fail_num = 0;
  size_t done_probe_fail_max = 5;
  auto min_sleep = std::min<size_t>(estimate_time_us/done_probe_fail_max, 2000000u);
  auto first_sleep = lim(estimate_time_us/4, min_sleep, 5000000u);
  std::this_thread::sleep_for(microseconds(first_sleep));
  auto one_timeout = microseconds(lim(estimate_time_us/16, min_sleep, 2000000u));
#ifdef TOPLING_DCOMPACT_USE_ETCD
  if (pEtcd) {
    size_t etcd_estimate_timeout_us = estimate_time_us * 5 / 4;
    while (!(done || *shutting_down) && t5 - t4 < etcd_estimate_timeout_us) {
      std::unique_lock<std::mutex> lock(cond_mtx);
      auto pred = [&]{ // pred is used to avoid spuriously awakenings
        // params.shutting_down change is not notified by this cond_var,
        // but it is really a condition
        return done || shutting_down->load(std::memory_order_relaxed);
      };
      cond_var.wait_for(lock, one_timeout, pred);
      t5 = m_env->NowMicros();
    }
  }
#endif
  std::string compact_done_file = output_dir + "/compact.done";
  FileStream compact_done_fp;
  while (!(done || shutting_down->load(std::memory_order_relaxed))) {
    t5 = m_env->NowMicros();
    s = SubmitHttp("/probe", meta_jstr, nth_http);
    if (!s.ok()) {
      if (compact_done_fp.xopen(compact_done_file, "r")) {
        INFO("fopen(%s) = OK: %8.3f sec", compact_done_file, (t5-t4)/1e6);
        goto CompactionDone;
      }
      else if (++done_probe_fail_num < done_probe_fail_max) {
        INFO("fopen(%s) = %m: %8.3f sec on http probe failed: %zd, retry",
              compact_done_file, (t5-t4)/1e6, done_probe_fail_num);
      }
      else {
        INFO("fopen(%s) = %m: %8.3f sec on http probe failed: %zd, giveup",
              compact_done_file, (t5-t4)/1e6, done_probe_fail_num);
        break;
      }
    }
    else {
      done_probe_fail_num = 0;
    }
    if (done) {
      INFO("done = true while check fopen(%s)", compact_done_file);
      break;
    }
    t5 = m_env->NowMicros(); // update t5
    if (t5 - t4 > timeout_us) {
      //---- t4 is etcd / worker job start time
      break;
    }
    std::this_thread::sleep_for(one_timeout);
  }
  if (done) {
#ifdef TOPLING_DCOMPACT_USE_ETCD
    if (response.action() == "create" || response.action() == "set") { // OK
#endif
     CompactionDone:
      if (!compact_done_fp) {
        compact_done_fp.open(compact_done_file, "r"); // may throw
      }
      const size_t fsize_full = lcast(LineBuf(compact_done_fp));
      FileStream fp(output_dir + "/rpc.results", "rb");
      for (int retry = 0; retry < 10; retry++) {
        const size_t fsize_view = fp.fsize();
        if (fsize_view == fsize_full)
          break;
        INFO("fsize(%s/rpc.results): view = %zd, full = %zd",
             output_dir, fsize_view, fsize_full);
        std::this_thread::sleep_for(one_timeout);
      }
      SerDeRead(fp, results);
      auto t6 = m_env->NowMicros();
      // remaining data in fp is for NotifyResults
      NotifyResults(fp, params); // maybe throw
      auto t7 = m_env->NowMicros();
      results->curl_time_usec = t4 - t3;
      ROCKS_LOG_INFO(m_log,
                      "job-%05d/att-%02d: done time curl %s = %6.3f, "
                      "mount = %6.3f, prepare = %6.3f, wait = %6.3f, etcd = %6.3f, "
                      "work = %6.3f, e2e = %6.3f, ntr = %6.3f sec, input = %s",
                      meta.job_id, m_attempt, m_url.c_str(),
                      results->curl_time_usec / 1e6,
                      results->mount_time_usec / 1e6,
                      results->prepare_time_usec / 1e6,
                      results->waiting_time_usec / 1e6,
                      (t5-t4) / 1e6, // etcd
                      results->work_time_usec / 1e6,
                      (t6-t3) / 1e6, (t7-t6) / 1e6,
                      SizeToString(input_raw_bytes()).c_str());
      m_done = true;
      s = Status::OK();
      auto all_usec = results->all_time_usec();
      // update f->estimate_speed_rt
      f->m_stat_map.m_mtx.lock();
      f->m_stat_sum.m_sum_input_raw_key_bytes += m_input_raw_key_bytes;
      f->m_stat_sum.m_sum_input_raw_val_bytes += m_input_raw_val_bytes;
      f->m_stat_sum.m_sum_input_zip_kv_bytes += m_input_zip_kv_bytes;
    //f->m_stat_sum.m_sum_compact_time_us += t6 - t3;
      f->m_stat_sum.m_sum_compact_time_us += t6 - t5 + all_usec;
      f->m_stat_map.val(m_stat_idx).num_cumu_exec++;
      f->m_stat_map.val(m_stat_idx).m_sum_input_raw_key_bytes += m_input_raw_key_bytes;
      f->m_stat_map.val(m_stat_idx).m_sum_input_raw_val_bytes += m_input_raw_val_bytes;
      f->m_stat_map.val(m_stat_idx).m_sum_input_zip_kv_bytes += m_input_zip_kv_bytes;
    //f->m_stat_map.val(m_stat_idx).m_sum_compact_time_us += t6 - t3;
      f->m_stat_map.val(m_stat_idx).m_sum_compact_time_us += t6 - t5 + all_usec;
      f->m_stat_map.val(m_stat_idx).m_last_access = t7;
      f->m_stat_map.m_mtx.unlock();
#ifdef TOPLING_DCOMPACT_USE_ETCD
    }
    else {
      s = Status::Corruption("DcompactEtcdExec::Attempt", ExceptionFormatString(
            "job-%05d/att-%02d at %s unexpected: etcd action = %s, value = %s",
            params.job_id, m_attempt, m_url.c_str(), response.action().c_str(),
            response.value().as_string().c_str()));
      ROCKS_LOG_ERROR(m_log, "done with fail: %s", s.ToString().c_str());
    }
#endif
  }
  else if (!*shutting_down) {
    auto tt = m_env->NowMicros();
    s = Status::TimedOut("DcompactEtcdExec::Attempt", ExceptionFormatString(
          "job-%05d/att-%02d: at %s estimate timeout = %.6f sec "
          "real = %.6f sec(submit = %.6f wait = %.6f), input = %s, send shutdown",
          params.job_id, m_attempt, m_url.c_str(), timeout_us/1e6,
          (tt-t2)/1e6, (t4-t3)/1e6, (t5-t4)/1e6,
          SizeToString(input_raw_bytes()).c_str()));
    ROCKS_LOG_ERROR(m_log, "%s", s.ToString().c_str());
    SubmitHttp("/shutdown", meta_jstr, nth_http);
  }
  else {
    ROCKS_LOG_WARN(m_log, "DcompactEtcd detected shutting down: %s", key.c_str());
    SubmitHttp("/shutdown", meta_jstr, nth_http);
    s = Status::ShutdownInProgress("DcompactEtcd detected shutting down", key);
  }
  if (!s.ok()) {
    CleanFiles(params, *results);
  }
//watcher.Cancel(); // watch will be canceled in destructor
  return s;
}
#ifdef NDEBUG
catch (const std::exception& ex) {
  ROCKS_LOG_ERROR(m_log, "job-%05d/att-%02d at %s, exception: %s",
    params.job_id, m_attempt, m_url.c_str(), ex.what());
  CleanFiles(params, *results);
  return Status::Corruption("DcompactEtcdExec::Attempt", ex.what());
}
catch (const Status& s) {
  ROCKS_LOG_ERROR(m_log, "job-%05d/att-%02d at %s, caught Status: %s",
    params.job_id, m_attempt, m_url.c_str(), s.ToString().c_str());
  CleanFiles(params, *results);
  return Status::Corruption("DcompactEtcdExec::Attempt", s.ToString());
}
catch (...) {
  CleanFiles(params, *results);
  return Status::Corruption("DcompactEtcdExec::Attempt", "caught unknown exception");
}
#endif

auto g_curl_init = curl_global_init(CURL_GLOBAL_DEFAULT); // NOLINT

size_t curl_my_recv(void* ptr, size_t, size_t len, void* userdata) {
  auto* str = (std::string*)userdata;
  str->append((const char*)ptr, len);
  return len;
}

void DcompactEtcdExec::PostHttp(const std::string& urlstr, fstring body) {
  char errbuf[CURL_ERROR_SIZE];
  CURL* curl = curl_easy_init();
  std::string result_buf;
  const char* url = urlstr.c_str();
  curl_easy_setopt(curl, CURLOPT_MAXCONNECTS, 32L);
  curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, errbuf);
#if LIBCURL_VERSION_MAJOR * 10000 + LIBCURL_VERSION_MINOR * 10 >= 70490
  curl_easy_setopt(curl, CURLOPT_TCP_FASTOPEN, 1L);
#endif
  curl_easy_setopt(curl, CURLOPT_TCP_NODELAY, 1L);
  curl_easy_setopt(curl, CURLOPT_TIMEOUT, 2L);
  curl_easy_setopt(curl, CURLOPT_NOSIGNAL, true); // disable signal
  curl_easy_setopt(curl, CURLOPT_URL, url);
  curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.data());
  curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, body.size());
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, &result_buf);
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, &curl_my_recv);
  auto headers = curl_slist_append(nullptr, "Content-Type: application/json");
  curl_slist_append(headers, "Expect:");
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
  TERARK_SCOPE_EXIT(curl_slist_free_all(headers));
  result_buf.reserve(512);
  auto err = curl_easy_perform(curl);
  if (err) {
    size_t len = strlen(errbuf);
    if (len && '\n' == errbuf[len - 1]) {
      errbuf[--len] = '\0';
    }
    if (len) {
      Err("libcurl: %s (%d): %s : %s : %s", url, int(err), errbuf, body, result_buf);
    }
    else {
      auto general_errmsg = curl_easy_strerror(err);
      Err("libcurl: %s (%d): %s : %s : %s", url, int(err), general_errmsg, body, result_buf);
    }
  }
  else {
    long response_code = 500; // set default as internal error
    err = curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code);
    if (err) {
      auto general_errmsg = curl_easy_strerror(err);
      Err("libcurl: %s (%d): %s : %s : %s", url, int(err), general_errmsg, body, result_buf);
    }
    else if (200 == response_code) { // 200 OK
      DEBG("PostHttpRequest: 200 OK: uri = %s, body = %s, response = %s", url, body, result_buf);
    }
    else {
      Err("libcurl: %s : response_code = %ld : %s", url, response_code, result_buf);
    }
  }
  curl_easy_cleanup(curl);
}

Status DcompactEtcdExec::SubmitHttp(const fstring action,
                                    const std::string& meta_jstr,
                                    size_t nth_http) noexcept {
  auto f = static_cast<const DcompactEtcdExecFactory*>(m_factory);
  char errbuf[CURL_ERROR_SIZE];
  const auto& params = *f->http_workers[nth_http];
  CURL* curl = curl_easy_init();
  std::string result_buf;
  auto headers = curl_slist_append(nullptr, "Content-Type: application/json");
  TERARK_SCOPE_EXIT(curl_slist_free_all(headers));
  curl_slist_append(headers, "Expect:");
  if (action == "/dcompact") {
    std::string kv;
    auto iter = f->dcompact_http_headers.begin();
    for (; iter != f->dcompact_http_headers.end(); ++iter) {
      kv.clear();
      kv.append(iter.key());
      kv.append(": ");
      kv.append(iter.value().get_ref<const std::string&>());
      curl_slist_append(headers, kv.c_str());
    }
    m_url.assign(params.url);
  } else {
    curl_easy_setopt(curl, CURLOPT_COOKIE, m_full_server_id.c_str());
    m_url.clear();
    m_url|params.base_url|action|"?labour="|m_labour_id;
  }
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
  const char* url = m_url.c_str();
  curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, errbuf);
#if LIBCURL_VERSION_MAJOR * 10000 + LIBCURL_VERSION_MINOR * 10 >= 70490
  curl_easy_setopt(curl, CURLOPT_TCP_FASTOPEN, 1L);
#endif
  curl_easy_setopt(curl, CURLOPT_TCP_NODELAY, 1L);
  curl_easy_setopt(curl, CURLOPT_TIMEOUT, (long)f->http_timeout);
  curl_easy_setopt(curl, CURLOPT_NOSIGNAL, true); // disable signal
  curl_easy_setopt(curl, CURLOPT_URL, url);
  curl_easy_setopt(curl, CURLOPT_POSTFIELDS, meta_jstr.c_str());
  curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, meta_jstr.size());
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, &result_buf);
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, &curl_my_recv);
  if (!params.ca.empty()) {
    curl_easy_setopt(curl, CURLOPT_CAPATH, params.ca.c_str());
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L); // skip verify
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L); // skip verify
  }
  if (!params.cert.empty()) {
    TERARK_VERIFY_S(!params.key.empty(), "while cert = %s", params.cert);
    curl_easy_setopt(curl, CURLOPT_SSLCERTTYPE, "PEM");
    curl_easy_setopt(curl, CURLOPT_SSLCERT, params.cert.c_str());
    curl_easy_setopt(curl, CURLOPT_SSLKEY, params.key.c_str());
    //curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
    curl_easy_setopt(curl, CURLOPT_SSL_OPTIONS, CURLSSLOPT_ALLOW_BEAST | CURLSSLOPT_NO_REVOKE);
  }
  auto err = curl_easy_perform(curl);
  Status s;
  if (err) {
    size_t len = strlen(errbuf);
    if (len && '\n' == errbuf[len - 1]) {
      errbuf[--len] = '\0';
    }
    if (len) {
      Err("libcurl: %s (%d): %s : %s : %s", url, int(err), errbuf, meta_jstr, result_buf);
      s = Status::Corruption(ROCKSDB_FUNC, errbuf);
    }
    else {
      auto general_errmsg = curl_easy_strerror(err);
      Err("libcurl: %s (%d): %s : %s : %s", url, int(err), general_errmsg, meta_jstr, result_buf);
      s = Status::Corruption(ROCKSDB_FUNC, general_errmsg);
    }
  }
  else {
    long response_code = 500; // set default as internal error
    err = curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code);
    if (err) {
      auto general_errmsg = curl_easy_strerror(err);
      Err("libcurl: %s (%d): %s : %s : %s", url, int(err), general_errmsg, meta_jstr, result_buf);
      s = Status::Corruption(ROCKSDB_FUNC, general_errmsg);
    }
    else if (200 == response_code) { // 200 OK
      struct curl_slist* cookie_list = nullptr;
      err = curl_easy_getinfo(curl, CURLINFO_COOKIELIST, &cookie_list);
      if (err) {
        auto general_errmsg = curl_easy_strerror(err);
        Err("libcurl.getcookie: %s (%d): %s : %s : %s", url, int(err),
             general_errmsg, meta_jstr, result_buf);
        s = Status::Corruption(ROCKSDB_FUNC, general_errmsg);
      }
      else {
        try {
          json js = json::parse(result_buf);
          auto& str = js["status"].get_ref<const std::string&>();
          if (str == "ok") {
            // do nothing
          }
          else if (str == "NotFound") {
            s = Status::NotFound(str);
            INFO("curl %s : 200 OK status = NotFound : job-%05d/att-%02d",
                 url, m_params->job_id, m_attempt);
          }
          else { // should not happen, but check for safe
            s = Status::Corruption(str, result_buf);
            Err("curl %s : 200 OK meta = %s, bad response content = %s",
                url, meta_jstr, result_buf);
          }
          auto& addr = js["addr"].get_ref<const std::string&>();
          if (action == "/dcompact") {
            m_labour_id = addr;
          }
          else if (addr != m_labour_id) {
            Err("curl %s : 200 OK meta = %s, bad addr: (%s) != (%s)",
                url, meta_jstr, addr, m_labour_id);
            s = Status::Corruption("bad addr: " + addr + ", meta = " + meta_jstr);
          }
        } catch (const std::exception& ex) {
          Err("curl %s : bad 200 OK response content = %s, exception = %s",
              url, result_buf, ex);
          s = Status::Corruption(result_buf);
        }
      }
      if (auto each = cookie_list) {
        while (each) {
          fstring cookie = each->data;
          if (cookie.starts_with("SERVERID=")) {
            if (action != "/dcompact") {
              auto sep = cookie.strchr('|');
              if (nullptr == sep) {
                Err("curl %s : meta = %s, bad cookie = %s", url, meta_jstr, cookie);
              }
              size_t len = sep - cookie.data();
              if (cookie.commonPrefixLen(m_full_server_id) < len)
                Err("curl %s : 200 OK meta = %s, mismatch: (%s) != (%s)",
                    url, meta_jstr, cookie, m_full_server_id);
            }
            // always set/update cookie m_full_server_id
            m_full_server_id = cookie.str();
            break;
          }
          each = each->next;
        }
        curl_slist_free_all(cookie_list);
      }
    }
    else {
      if (502 == response_code) {
        // bad gateway, returned by proxy
        Err("libcurl: %s : job-%05d/att-%02d : response_code = 502 bad gateway",
            url, m_params->job_id, m_attempt);
      } else {
        Err("libcurl: %s : job-%05d/att-%02d : response_code = %ld : %s",
            url, m_params->job_id, m_attempt, response_code, result_buf.c_str());
      }
      if (412 == response_code) { // Precondition Failed
        s = Status::InvalidArgument(ROCKSDB_FUNC, result_buf);
      }
      else if (503 == response_code) { // Server busy
        s = Status::Busy(ROCKSDB_FUNC, result_buf);
      }
      else {
        s = Status::Corruption(ROCKSDB_FUNC, result_buf);
      }
    }
  }
  curl_easy_cleanup(curl);
  return s;
}

static
uint64_t SumInputZipBytes(const std::vector<CompactionInputFiles>* inputs) {
  uint64_t sum = 0;
  for (auto& lv : *inputs) {
    for (auto& f : lv.files) {
      sum += f->fd.file_size;
    }
  }
  return sum;
}

static
uint64_t SumOutputZipBytes(const CompactionResults& results) {
  uint64_t sum = 0;
  for (auto& lv : results.output_files) {
    for (auto& f : lv)
      sum += f.file_size;
  }
  return sum;
}

void DcompactEtcdExec::ReportFee(const CompactionParams& params,
                                 const CompactionResults& results) {
  auto f = static_cast<const DcompactEtcdExecFactory*>(m_factory);
  auto conf = f->fee_conf.get();
  DcompactFeeReport fee;

  // set compaction starts time
  uint64_t now_usec = m_env->NowMicros();
  uint64_t work_start_time = now_usec - results.work_time_usec;
  time_t rawtime = work_start_time / 1000000; // seconds
#if 1
  fee.starts = rawtime;
#else
  struct tm  tm_storage;
  struct tm* tp = gmtime_r(&rawtime, &tm_storage);
  size_t cap = 64;
  fee.starts.resize(cap);
  fee.starts.resize(strftime(&fee.starts[0], cap, "%FT%H:%M:%SZ", tp));
#endif

  const std::string& dbpath = params.dbname;
  std::string dbname = basename(dbpath.c_str()); // now for simpliciy
  fee.provider = conf->provider;
  fee.dbId = dbname;
  fee.attempt = m_attempt;
  fee.dbStarts = f->m_start_time_epoch;
  fee.executesMs = results.work_time_usec / 1000;
  fee.instanceId = f->fee_conf->instanceId;
  fee.compactionJobId = params.job_id;
  fee.compactionInputRawBytes = input_raw_bytes();
  fee.compactionInputZipBytes = SumInputZipBytes(params.inputs);
  fee.compactionOutputRawBytes = results.output_index_size + results.output_data_size;
  fee.compactionOutputZipBytes = SumOutputZipBytes(results);

  std::string post_body = fee.ToJson().dump();

  char errbuf[CURL_ERROR_SIZE];
  CURL* curl = curl_easy_init();
  std::string result_buf;
  const char* url = conf->url.c_str();
  curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, errbuf);
#if LIBCURL_VERSION_MAJOR * 10000 + LIBCURL_VERSION_MINOR * 10 >= 70490
  curl_easy_setopt(curl, CURLOPT_TCP_FASTOPEN, 1L);
#endif
  curl_easy_setopt(curl, CURLOPT_TCP_NODELAY, 1L);
  curl_easy_setopt(curl, CURLOPT_TIMEOUT, (long)f->http_timeout);
  curl_easy_setopt(curl, CURLOPT_NOSIGNAL, true); // disable signal
  curl_easy_setopt(curl, CURLOPT_URL, url);
  curl_easy_setopt(curl, CURLOPT_POSTFIELDS, post_body.c_str());
  curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, post_body.size());
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, &result_buf);
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, &curl_my_recv);
  if (!conf->ca.empty()) {
    curl_easy_setopt(curl, CURLOPT_CAPATH, conf->ca.c_str());
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L); // skip verify
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L); // skip verify
  }
  if (!conf->cert.empty()) {
    TERARK_VERIFY_S(!conf->key.empty(), "while cert = %s", conf->cert);
    curl_easy_setopt(curl, CURLOPT_SSLCERTTYPE, "PEM");
    curl_easy_setopt(curl, CURLOPT_SSLCERT, conf->cert.c_str());
    curl_easy_setopt(curl, CURLOPT_SSLKEY, conf->key.c_str());
    //curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
    curl_easy_setopt(curl, CURLOPT_SSL_OPTIONS, CURLSSLOPT_ALLOW_BEAST | CURLSSLOPT_NO_REVOKE);
  }
  auto headers = curl_slist_append(nullptr, "Content-Type: application/json");
  curl_slist_append(headers, "Expect:");
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
  TERARK_SCOPE_EXIT(curl_slist_free_all(headers));
  auto err = curl_easy_perform(curl);
  if (err) {
    size_t len = strlen(errbuf);
    if (len && '\n' == errbuf[len - 1]) {
      errbuf[--len] = '\0';
    }
    if (len) {
      Err("libcurl: %s (%d): %s : %s : %s", url, int(err), errbuf, post_body, result_buf);
    }
    else {
      auto general_errmsg = curl_easy_strerror(err);
      Err("libcurl: %s (%d): %s : %s : %s", url, int(err), general_errmsg, post_body, result_buf);
    }
  }
  else {
    long response_code = 500; // set default as internal error
    err = curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code);
    if (err) {
      auto general_errmsg = curl_easy_strerror(err);
      Err("libcurl: %s (%d): %s : %s : %s", url, int(err), general_errmsg, post_body, result_buf);
    }
    else if (200 == response_code) { // 200 OK
     // do nothing
    }
    else {
      if (502 == response_code) {
        // bad gateway, returned by proxy
        Err("libcurl: %s : job-%05d/att-%02d : response_code = 502 bad gateway",
            url, m_params->job_id, m_attempt);
      } else {
        Err("libcurl: %s : job-%05d/att-%02d : response_code = %ld : %s",
            url, m_params->job_id, m_attempt, response_code, result_buf.c_str());
      }
    }
  }
  curl_easy_cleanup(curl);
}

void DcompactEtcdExec::CleanFiles(const CompactionParams& params,
                                  const CompactionResults& results) {
  auto f = static_cast<const DcompactEtcdExecFactory*>(m_factory);
  if (f->fee_conf) {
    // output_index_size and output_data_size will be set in
    // compaction_job.cc: RunRemote(), code change in this way will
    // minimize the efforts and maximize compatibility
    if (results.output_index_size + results.output_data_size) {
      ReportFee(params, results);
    }
  }
  auto t0 = m_env->NowMicros();
  char buf[64];
  const std::string& base_dir = params.cf_paths.back().path;
  std::string job_dir = CatFmt(base_dir, "/job-%05d", params.job_id);
  std::string attempt_dir = CatFmt(job_dir, "/att-%02d", m_attempt);
  auto shutting_down = m_params->shutting_down;
  if (shutting_down && shutting_down->load()) {
    // do not delete files, to prevent worker node from coredump
    INFO("job-%05d/att-%02d: CleanFiles: shutting down, do not delete files",
         params.job_id, m_attempt);
  }
  else {
    size_t fail_num = 0;
    auto rm = [&](const std::string& file) {
      DEBG("DeleteFile %s", file);
      Status s = m_env->DeleteFile(file);
      if (!s.ok())
        WARN("%s", s.ToString());
    };
    auto rmdir = [&](const std::string& dir) {
      DEBG("DeleteDir %s", dir);
      Status s = m_env->DeleteDir(dir);
      if (!s.ok())
        WARN("%s", s.ToString()), fail_num++;
    };
    for (const std::string& file : m_copyed_files) {
      ROCKSDB_VERIFY(f->copy_sst_files);
      rm(file);
    }
    rm(attempt_dir + "/compact.done");
    rm(attempt_dir + "/rpc.results");
    rmdir(attempt_dir);
    if (m_done) {
      rm(job_dir + "/rpc.params");
      for (auto& file : params.extra_serde_files) {
        rm(job_dir + "/" + file);
      }
      rmdir(job_dir);
      if (fail_num) {
        g_delayed_rm_queue_mtx.lock();
        g_delayed_rm_queue.push_back(job_dir);
        g_delayed_rm_queue_mtx.unlock();
      }
    }
  }
  if (m_worker) {
    const std::string& dbpath = params.dbname;
    fstring dbname = basename(dbpath.c_str()); // now for simpliciy
    m_worker->m_running_mtx.lock();
    auto& labour = m_worker->m_running[m_labour_id];
    TERARK_VERIFY(labour[dbname].erase(this));
    labour.m_sum_runnings--;
    m_worker->m_running_mtx.unlock();
    m_worker = nullptr;
  }
  auto t1 = m_env->NowMicros();
  perform_delayed_rm(m_log);
  auto t2 = m_env->NowMicros();
  ROCKS_LOG_INFO(m_log, "job-%05d/att-%02d: CleanFiles %8.3f sec : %8.3f",
                 params.job_id, m_attempt, (t1-t0)/1e6, (t2-t1)/1e6);
}

CompactionExecutor*
DcompactEtcdExecFactory::NewExecutor(const Compaction*) const {
  return new DcompactEtcdExec(this);
}

struct WorkerLabour : DcompactStatItem {
  enum UniqueBy {
    kFull,
    kWebUrl, // EtcdConnectionParams::web_url
    kLabour,
  };
  std::string dbname;
  std::string cfname;
  int job_id;
  int attempt;
  const std::string* web_url;
  std::string labour_id;
  uint64_t job_input_raw_bytes;
  uint64_t job_start_ts;

  bool IsEqual(const WorkerLabour& y, int uniqBy) const {
    switch (UniqueBy(uniqBy)) {
      default:  TERARK_DIE("bad UniqueBy = %d", uniqBy); break;
      case kFull  : return *web_url == *y.web_url && labour_id == y.labour_id;
      case kWebUrl: return *web_url == *y.web_url && labour_id == y.labour_id;
      case kLabour: return labour_id == y.labour_id;
    }
  }

  static void Unique(std::vector<WorkerLabour>& labours, int uniqBy) {
    auto beg = labours.begin(), end = labours.end();
    switch (UniqueBy(uniqBy)) {
      default:  TERARK_DIE("bad UniqueBy = %d", uniqBy); break;
      case kFull:
        std::sort(beg, end, TERARK_CMP(web_url, <, labour_id, <, dbname, <, job_id, <, attempt, <));
        //end = std::unique(beg, end, TERARK_EQUAL(web_url, labour_id, dbname, job_id, attempt));
        break;
      case kWebUrl:
        break;
      case kLabour:
        std::sort(beg, end, TERARK_CMP(labour_id, <, dbname, <, job_id, <, attempt, <));
        end = std::unique(beg, end, TERARK_EQUAL(labour_id, dbname, job_id, attempt));
        break;
    }
    labours.erase(end, labours.end());
  }
};

static const char* get_port_ptr(fstring addr) {
  const char* port = addr.strstr("://");
  port = strchr(port ? port + 3 : addr.p, ':');
  port = port ? port + 1 : "80";
  return port;
}

std::string
DcompactEtcdExecFactory::WorkersView(const json& dump_options, int cols)
const {
  std::vector<WorkerLabour> labours;
  labours.reserve(64);
  double speed_mbps = m_stat_sum.rt_estimate_speed_mb(estimate_speed);

  string_appender<std::string> str;
  str.reserve(64*1024);
  str|"<script>document.getElementById('time_stat_line').innerHTML += `";
  str|" , CumuExec = "|num_cumu_exec;
  str|" , LiveExec = "|num_live_exec;
  str^" , Speed = %.3f"^speed_mbps^" MB/sec, Total: ";
  str|"Key = "|SizeToString(m_stat_sum.m_sum_input_raw_key_bytes)|", ";
  str|"Val = "|SizeToString(m_stat_sum.m_sum_input_raw_val_bytes)|", ";
  str|"ZipKV = "|SizeToString(m_stat_sum.m_sum_input_zip_kv_bytes)|", ";
  str|"Units = "|fee_units(m_stat_sum.sum_input_raw_bytes(),
                           m_stat_sum.sum_input_zip_bytes());
  bool allworkers = JsonSmartBool(dump_options, "allworkers", true);
  str|"&nbsp;&nbsp;&nbsp;";
  str^R"(<a href='javascript:SetParam("allworkers","%d")'>)"^!allworkers;
  str|"all="|!allworkers;
  str|"</a>";
  auto workerlink = (uint)JsonSmartInt(dump_options, "workerlink", 2) % 3;
  str|"&nbsp;&nbsp;&nbsp;";
  str^R"(<a href='javascript:SetParam("workerlink","%d")'>)"^(workerlink+1)%3;
  str|"link="|(workerlink+1)%3;
  str|"</a>";
  str|"`;\n";
  if (!web_domain.empty())
    str|"document.domain = '"|web_domain|"';\n";
  str|R"(
function setIframeHeight(iframe) {
  var win = iframe.contentWindow || iframe.contentDocument.parentWindow;
  if (win.document.body) {
      iframe.height = win.document.documentElement.scrollHeight ||
                      win.document.body.scrollHeight;
  } else {
      alert("win is null, iframe id = " + iframe.height);
  }
};
)";
  str|"</script>\n";
  size_t table_str_pos = str.size();
  str|"<table width='100%' height='100%' border=1><tbody valign='top'>\n";
  int verbose = JsonSmartInt(dump_options, "verbose", 2);
  int version = JsonSmartInt(dump_options, "version", 0);
  int height = JsonSmartInt(dump_options, "height", 300);
  valvec<const HttpParams*> uniq(http_workers.size(), valvec_reserve());
  for (auto& x : http_workers) uniq.unchecked_push_back(x.get());
  if (!allworkers) {
    // do not show workers which is not serving my job
    uniq.trim(std::remove_if(uniq.begin(), uniq.end(),
      [](const HttpParams* worker) { // no locked with m_running_mtx
        // may be not consistency with belowing locked access, but we tolerate
        // it because it is very unlikely to happen.
        return worker->m_running.empty(); // this is just a rough check
    }));
  }
  std::sort(uniq.begin(), uniq.end(), TERARK_CMP_P(url, <));
  uniq.trim(std::unique(uniq.begin(), uniq.end(), TERARK_GET(->url)==cmp));
  int idx = 0;
  int num = (int)uniq.size();
  int non_empty = 0;
  auto now = m_env->NowMicros();
  for (; idx < num; idx++) {
    const HttpParams* worker = uniq[idx];
worker->m_running_mtx.lock();
    worker->m_running.for_each([&](const auto& labour_kv) {
      auto& labour = labour_kv.second;
      if (!allworkers && 0 == labour.m_sum_runnings) {
        return;
      }
      const fstring labour_id = labour_kv.first;
      labour.for_each([&](const auto& kv) {
        fstring dbname = kv.first;
        auto& jobs = kv.second;
        jobs.for_each([&](const DcompactEtcdExec* exec) {
          WorkerLabour wl;
          wl.dbname = dbname.str();
          wl.cfname = exec->m_params->cf_name;
          wl.job_id = exec->m_params->job_id;
          wl.attempt = exec->m_attempt;
          wl.labour_id = labour_id.str();
          wl.job_start_ts = exec->m_start_ts;
          wl.job_input_raw_bytes = exec->input_raw_bytes();
          wl.web_url = &worker->web_url;
          labours.push_back(std::move(wl));
        });
      });
    });
worker->m_running_mtx.unlock();
  }
  // now retrieve m_stat_map
  {
    string_appender<> dbcf_name; // = dbname + "/" + params.cf_name;
    dbcf_name.reserve(128);
    m_stat_map.m_mtx.lock();
    for (auto& wl : labours) {
      dbcf_name.clear();
      dbcf_name|wl.dbname|'/'|wl.cfname;
      auto idx = m_stat_map.find_i(dbcf_name);
      if (idx < m_stat_map.end_i()) {
        static_cast<DcompactStatItem&>(wl) = m_stat_map.val(idx);
      }
    }
    m_stat_map.m_mtx.unlock();
  }
  WorkerLabour::Unique(labours, workerlink);

  for (size_t i = 0, n = labours.size(); i < n; i++) {
    auto& wl = labours[i];
    auto& web_url = *wl.web_url;
    fstring web_addr = web_url;
    if (const char* p = web_addr.strstr("://")) {
      web_addr = fstring(p+3, web_addr.end());
    }
    auto& dbname = wl.dbname;
    auto& labour_id = wl.labour_id;
    if (0 == i || !wl.IsEqual(labours[i-1], workerlink)) {
      auto port = get_port_ptr(labour_id == "self" ? web_url : labour_id);
      if (non_empty % cols == 0) {
      //str^"<tr height=%d>\n"^height;
        str|"<tr>\n";
      }
      str|"<td>";
      str|"<a href='"|web_url|"/"|instance_name|"/"|m_start_time|"/?labour="|labour_id|"'>";
      switch (workerlink) {
        case 0: str|web_addr|":"|labour_id; break;
        case 1: str|web_addr              ; break;
        case 2: str             |labour_id; break;
      }
      str|"</a> ";
      str|"<a href='"|web_url|"/stdlog/stderr."|port|"?labour="|labour_id|"'>stderr</a><br/>";
      str|"\n";
      str|"<iframe width='100%' height='"|height|"' frameBorder=0";
      str|" onload='setIframeHeight(this)'";
      str|" src='"|web_url|"/stat?html=1&from_db_node=1&verbose="|verbose|"&labour="|labour_id|"&version="|version|"'>";
      str|"</iframe><br/>";
      str|"\n";
    }
    int job_id = wl.job_id;
    int attempt = wl.attempt;
    auto speed_bps = wl.rt_estimate_speed_mb(estimate_speed) * 1e6;
    auto elapsed_sec = (now - wl.job_start_ts)/1e6;
    auto estimate_sec = wl.job_input_raw_bytes / speed_bps;
    char buf[64];
    auto job_attempt = ToStr("job-%05d/att-%02d", job_id, attempt);
    str|"<pre>";
    str^"%6.2f"^elapsed_sec;
    str^"/<span title='estimate speed = %.3f MB/sec'>"^speed_mbps;
    str^"%6.2f"^estimate_sec^" sec</span> ";
    str|"<a href='"|web_url|"/"|instance_name|"/"|m_start_time|"/"|dbname|"/"|job_attempt|"/summary.html"|"?labour="|labour_id|"'>";
    str^"%6.3f GB"^wl.job_input_raw_bytes/1e9;
    str|"</a> ";
    str|"<a href='"|web_url|"/"|instance_name|"/"|m_start_time|"/"|dbname|"/"|"?labour="|labour_id|"'>"|dbname|"</a>/";
    str|"<a href='"|web_url|"/"|instance_name|"/"|m_start_time|"/"|dbname|"/"|job_attempt|"/"|"?labour="|labour_id|"'>"|job_attempt|"</a>/";
    str|"<a href='"|web_url|"/"|instance_name|"/"|m_start_time|"/"|dbname|"/"|job_attempt|"/LOG"|"?labour="|labour_id|"'>LOG</a>";
    str|"</pre>";
    str|"\n";
    if (i+1==n || !wl.IsEqual(labours[i+1], workerlink)) {
      str.append("</td>\n");
      non_empty++;
      if (non_empty % cols == 0) {
        str.append("</tr>\n");
      }
    }
  }
  if (0 == non_empty) {
    str.resize(table_str_pos);
    str|"<p>there is no any jobs</p>";
  } else {
    if (non_empty % cols != 0) {
      str.append("</tr>\n");
    }
    str.append("</tbody></table>");
  }
  return std::move(str);
}

DcompactEtcdExec::DcompactEtcdExec(const DcompactEtcdExecFactory* fac)
    : CompactExecCommon(fac) {
}

ROCKSDB_REG_Plugin("DcompactEtcd",
                    DcompactEtcdExecFactory,
                    CompactionExecutorFactory);

} // ROCKSDB_NAMESPACE
