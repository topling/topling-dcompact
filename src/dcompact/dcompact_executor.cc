//
// Created by leipeng on 2021/1/19.
//

#include "dcompact_executor.h"
#include <logging/logging.h>
#include <rocksdb/merge_operator.h>
#include <topling/side_plugin_repo.h>
#include <topling/side_plugin_factory.h>
#include <topling/side_plugin_internal.h>

// requires terark-core
#include <terark/io/DataIO.hpp>
#include <terark/io/FileStream.hpp>
#include <terark/io/MemStream.hpp>
#include <terark/num_to_str.hpp>
#include <terark/util/atomic.hpp>
#include <terark/util/stat.hpp>
#include <boost/core/demangle.hpp>

extern const char* rocksdb_build_git_sha;
const char* git_version_hash_info_topling_dcompact();

namespace ROCKSDB_NAMESPACE {

#define PrintLog(level, prefix, fmt, ...) \
  do { if (SidePluginRepo::DebugLevel() >= level) \
    Log(info_log->GetInfoLogLevel(), info_log, "%s:%d: " prefix fmt "\n", \
        RocksLogShorterFileName(__FILE__), \
        TERARK_PP_SmartForPrintf(__LINE__, ## __VA_ARGS__)); \
  } while (0)
#define TRAC(...) PrintLog(4, "TRAC: ", __VA_ARGS__)
#define DEBG(...) PrintLog(3, "DEBG: ", __VA_ARGS__)
#define INFO(...) PrintLog(2, "INFO: ", __VA_ARGS__)
#define WARN(...) PrintLog(1, "WARN: ", __VA_ARGS__)

using terark::ExplicitSerDePointer;

template<class DataIO>
void DataIO_saveObject(DataIO& dio, const Slice& x) {
  dio << terark::var_size_t(x.size_);
  dio.ensureWrite(x.data_, x.size_);
}
template<class DataIO>
void DataIO_loadObject(DataIO& dio, InternalKey& x) {
  dio >> *x.rep();
}
template<class DataIO>
void DataIO_saveObject(DataIO& dio, const InternalKey& x) {
  dio << x.Encode();
}
struct Status_SerDe : Status {
  Status_SerDe(uint8_t _code, uint8_t _subcode, const Slice& msg)
    : Status(Code(_code), SubCode(_subcode), msg, Slice("")) {}
};
template<class DataIO>
void DataIO_loadObject(DataIO& dio, Status& x) {
  uint8_t code, subcode;
  std::string msg;
  dio >> code >> subcode >> msg;
  if (Status::Code(code) == Status::kOk)
    x = Status::OK();
  else
    x = Status_SerDe(code, subcode, msg);
}
template<class DataIO>
void DataIO_saveObject(DataIO& dio, const Status& x) {
  dio << uint8_t(x.code());
  dio << uint8_t(x.subcode());
  if (x.ok()) {
    dio.writeByte(0);
  }
  else {
    dio << Slice(x.getState());
  }
}
using CompactionStats = InternalStats::CompactionStats;
DATA_IO_DUMP_RAW_MEM_E(CompressionOptions)
DATA_IO_DUMP_RAW_MEM_E(CompressionType)
DATA_IO_DUMP_RAW_MEM_E(CompactionReason)
DATA_IO_DUMP_RAW_MEM_E(CompactionStats)
DATA_IO_DUMP_RAW_MEM_E(FileSampledStats)
DATA_IO_DUMP_RAW_MEM_E(InfoLogLevel)
DATA_IO_LOAD_SAVE_E(DbPath, & path & target_size)
DATA_IO_LOAD_SAVE_E(FileDescriptor, & packed_number_and_path_id & file_size
                  & smallest_seqno & largest_seqno)

#if (ROCKSDB_MAJOR * 10000 + ROCKSDB_MINOR * 10 + ROCKSDB_PATCH) >= 70100
  #define FileMetaData_v70100  & epoch_number & unique_id[0] & unique_id[1]
#else
  #define FileMetaData_v70100
#endif

DATA_IO_LOAD_SAVE_E(FileMetaData, & fd & smallest & largest & stats
                  & compensated_file_size
                  & num_entries & num_deletions
                  & raw_key_size & raw_value_size
                //& being_compacted // not needed, will be re-populated
                  & init_stats_from_file
                  & marked_for_compaction & oldest_blob_file_number
                  & oldest_ancester_time & file_creation_time
                  & file_checksum & file_checksum_func_name
                  FileMetaData_v70100
                  )
using terark::pass_by_value;
struct FileMetaDataVec_1P { const std::vector<FileMetaData*>* val; };
struct FileMetaDataVec_2P { const std::vector<FileMetaData*>* const* val; };
template<class DataIO>
void FileMetaDataVec_load(DataIO& dio, std::vector<FileMetaData*>& vec) {
  size_t num = dio.template load_as<terark::var_size_t>().t;
  vec.clear();
  vec.reserve(num);
  for (size_t i = 0; i < num; ++i) {
    std::unique_ptr<FileMetaData> meta(new FileMetaData());
    dio >> *meta;
    vec.push_back(meta.release());
  }
}
template<class DataIO>
void FileMetaDataVec_save(DataIO& dio, const std::vector<FileMetaData*>& vec) {
  size_t num = vec.size();
  dio << terark::var_size_t(num);
  for (FileMetaData* p : vec) {
    dio << *p;
  }
}
template<class DataIO>
void DataIO_loadObject(DataIO& dio, FileMetaDataVec_1P pvec) {
  FileMetaDataVec_load(dio, *const_cast<std::vector<FileMetaData*>*>(pvec.val));
}
template<class DataIO>
void DataIO_saveObject(DataIO& dio, FileMetaDataVec_1P pvec) {
  FileMetaDataVec_save(dio, *pvec.val);
}
template<class DataIO>
void DataIO_loadObject(DataIO& dio, FileMetaDataVec_2P ppvec) {
  auto new_vec = new std::vector<FileMetaData*>;
  FileMetaDataVec_load(dio, *new_vec);
  // *ppvec.val always not null
  const_cast<const std::vector<FileMetaData*>*&>(*ppvec.val) = new_vec;
}
template<class DataIO>
void DataIO_saveObject(DataIO& dio, FileMetaDataVec_2P ppvec) {
  FileMetaDataVec_save(dio, **ppvec.val);
}

/*
template<class DataIO>
void DataIO_loadObject(DataIO& dio, AtomicCompactionUnitBoundary& x) {
  std::unique_ptr<InternalKey> smallest(new InternalKey());
  std::unique_ptr<InternalKey> largest(new InternalKey());
  dio >> *smallest;
  dio >> *largest;
  x.smallest = smallest.release();
  x.largest = largest.release();
}
template<class DataIO>
void DataIO_saveObject(DataIO& dio, const AtomicCompactionUnitBoundary& x) {
  dio << *x.smallest;
  dio << *x.largest;
}
*/

template<class DataIO>
void DataIO_loadObject(DataIO& dio, ObjectRpcParam& x) {
  dio >> x.clazz;
  dio >> x.params;
  if (x.serde)
    x.serde(dio.fp(), x);
}
template<class DataIO>
void DataIO_saveObject(DataIO& dio, const ObjectRpcParam& x) {
  dio << x.clazz;
  dio << x.params;
  if (x.serde)
    x.serde(dio.fp(), x);
}

DATA_IO_LOAD_SAVE_E(CompactionInputFiles,
                  & level
                  & pass_by_value<FileMetaDataVec_1P>({&files})
                  // & atomic_compaction_unit_boundaries // will be populated
                  )
DATA_IO_DUMP_RAW_MEM_E(VersionSetSerDe)

// now just for listeners and table_properties_collector_factories
template<class DataIO>
void DataIO_loadObject(DataIO& dio, std::vector<ObjectRpcParam>& vec) {
  const size_t num = dio.read_var_uint32();
  ROCKSDB_VERIFY_EQ(vec.size(), num);
  for (size_t i = 0; i < num; ++i) {
    DataIO_loadObject(dio, vec[i]);
  }
}

DATA_IO_LOAD_SAVE_E(CompactionParams,
                  & job_id & num_levels & output_level & cf_id
                  & cf_name
                  & version_set
                  & target_file_size
                  & max_compaction_bytes & cf_paths & max_subcompactions
                  & compression & compression_opts & score
                  & manual_compaction & deletion_compaction
                  & compaction_log_level
                  & compaction_reason
                  & preserve_deletes_seqnum & smallest_seqno
                  & earliest_write_conflict_snapshot & paranoid_file_checks
                  & rocksdb_src_version & rocksdb_src_githash
                  & hoster_root & instance_name
                  & dbname & db_id & db_session_id & full_history_ts_low

                  // must before ObjectRpcParam fields
                  & smallest_user_key & largest_user_key
                  & ExplicitSerDePointer(inputs)
                  & pass_by_value<FileMetaDataVec_2P>({&grandparents})

                  & compaction_filter_factory & merge_operator
                  & user_comparator & table_factory & prefix_extractor
                  & sst_partitioner_factory & html_user_key_coder
                  & allow_ingest_behind
                  & preserve_deletes & bottommost_level
                  & listeners
                  & table_properties_collector_factories
                  & ExplicitSerDePointer(existing_snapshots)
                //& ExplicitSerDePointer(compaction_job_stats)
                  )

using FileMinMeta = CompactionResults::FileMinMeta;
using RawStatistics = CompactionResults::RawStatistics;

DATA_IO_LOAD_SAVE_E(FileMinMeta, & file_number & file_size
                  & smallest_seqno & largest_seqno
                  & smallest_ikey  & largest_ikey
                  & marked_for_compaction
                  )
DATA_IO_LOAD_SAVE_E(RawStatistics, & tickers & histograms)
DATA_IO_DUMP_RAW_MEM_E(HistogramStat)
DATA_IO_LOAD_SAVE_E(CompactionJobStats, & elapsed_micros & cpu_micros
                  & num_input_records & num_input_files
                  & num_input_files_at_output_level
                  & num_output_records & num_output_files
                  & is_full_compaction & is_manual_compaction
                  & total_input_bytes & total_output_bytes
                  & num_records_replaced
                  & total_input_raw_key_bytes
                  & total_input_raw_value_bytes
                  & num_input_deletion_records
                  & num_expired_deletion_records
                  & num_corrupt_keys & file_write_nanos
                  & file_range_sync_nanos & file_fsync_nanos
                  & file_prepare_write_nanos
                  & smallest_output_key_prefix & largest_output_key_prefix
                  & num_single_del_fallthru & num_single_del_mismatch
                  )
DATA_IO_LOAD_SAVE_E(CompactionResults,
                  & output_dir & output_files
                  & compaction_stats & job_stats
                  & statistics & status
                  & work_time_usec
                  & mount_time_usec & prepare_time_usec
                  & waiting_time_usec
                  )

void SerDeRead(FILE* fp, CompactionParams* p) {
  using namespace terark;
  LittleEndianDataInput<NonOwnerFileStream> dio(fp);
  dio >> *p;
  if (SidePluginRepo::DebugLevel() >= 4)
    ROCKS_LOG_INFO(p->info_log, "%s", p->DebugString().c_str());
  p->is_deserialized = true;
  TERARK_VERIFY(IsCompactionWorker());
  //TERARK_VERIFY_EQ(p->rocksdb_src_version, ROCKSDB_VERSION);
  //TERARK_VERIFY_S_EQ(p->rocksdb_src_githash, rocksdb_build_git_sha);
  struct ll_stat st;
  TERARK_VERIFY_F(ll_fstat(fileno(fp), &st) == 0, "%m");
  if (S_ISREG(st.st_mode))
    TERARK_VERIFY_EQ(dio.tell(), stream_position_t(st.st_size));
}
void SerDeRead(FILE* fp, CompactionResults* res) {
  using namespace terark;
  LittleEndianDataInput<NonOwnerFileStream> dio(fp);
  dio >> *res;
}
void SerDeRead(Slice data, CompactionResults* res) {
  using namespace terark;
  LittleEndianDataInput<MemIO> dio(data.data(), data.size());
  dio >> *res;
}
void SerDeWrite(FILE* fp, const CompactionParams* p) {
  using namespace terark;
  LittleEndianDataOutput<NonOwnerFileStream> dio(fp);
  dio << *p;
}
void SerDeWrite(FILE* fp, const CompactionResults* res) {
  using namespace terark;
  LittleEndianDataOutput<NonOwnerFileStream> dio(fp);
  dio << *res;
}

static std::string ClassParams_get_params(const json* js, const char* func) {
  // js is: { "class": "ClassName", "params": { ... } }
  std::string res;
  if (js->is_object()) {
    auto iter = js->find("params");
    if (js->end() == iter) {
      throw Status::NotFound(func, "not found 'params' in: " + js->dump());
    }
    res = iter.value().dump();
  }
  return res;
}

template<class RawPtr>
void SerDe_SerializeReq(FILE* f, const ObjectRpcParam& x,
                        RawPtr p, const CompactionParams& params,
                        const SidePluginRepo& repo) {
  auto js = JS_CompactionParamsEncodePtr(&params);
  auto serde = SerDeFac(p)->AcquirePlugin(x.clazz, js, repo);
  Logger* info_log = params.info_log;
  TRAC("SerDe_SerializeReq: clazz = %s, js = %s", x.clazz, js.dump());
  serde->Serialize(f, *p);
}

template<class RawPtr>
void SerDe_SerializeOpt(FILE* f, const ObjectRpcParam& x,
                        RawPtr p, const CompactionParams& params,
                        const SidePluginRepo& repo) {
  auto js = JS_CompactionParamsEncodePtr(&params);
  auto serde = SerDeFac(p)->NullablePlugin(x.clazz, js, repo);
  Logger* info_log = params.info_log;
  TRAC("SerDe_SerializeOpt: serde = %p, clazz = %s, js = %s",
        serde.get(), x.clazz, js.dump());
  if (serde) {
    assert(nullptr != p);
    serde->Serialize(f, *p);
  }
}

template<class ObjectPtr>
void SetObjectRpcParamReqTpl(ObjectRpcParam& p, const ObjectPtr& obj,
                             const CompactionParams& params,
                             const SidePluginRepo& repo) {
  Logger* info_log = params.info_log;
  if (auto p_obj = GetRawPtr(obj)) {
    p.clazz = obj->Name();
    p.serde = [&,p_obj](FILE* f, const ObjectRpcParam& x) {
      SerDe_SerializeReq(f, x, p_obj, params, repo);
    };
    if (const json* js = repo.GetCreationSpec(obj)) {
      p.params = ClassParams_get_params(js, ROCKSDB_FUNC);
    } else {
      TERARK_DIE_S("obj must be defined in json repo: %s", p.clazz);
    }
    TRAC("SetObjectRpcParamReq: clazz = %s, params = %s", p.clazz, p.params);
  }
  else
    TRAC("SetObjectRpcParamReq: clazz = %s, obj = null",
        boost::core::demangle(typeid(decltype(*obj)).name()));
}
template<class ObjectPtr>
void SetObjectRpcParamOptTpl(ObjectRpcParam& p, const ObjectPtr& obj,
                             const CompactionParams& params,
                             const SidePluginRepo& repo) {
  Logger* info_log = params.info_log;
  if (auto p_obj = GetRawPtr(obj)) {
    p.clazz = obj->Name();
    p.serde = [&,p_obj](FILE* f, const ObjectRpcParam& x) {
      SerDe_SerializeOpt(f, x, p_obj, params, repo);
    };
    if (const json* js = repo.GetCreationSpec(obj)) {
      p.params = ClassParams_get_params(js, ROCKSDB_FUNC);
      TRAC("SetObjectRpcParamOpt: clazz = %s, params = %s", p.clazz, p.params);
    } else
      TRAC("SetObjectRpcParamOpt: clazz = %s, params = nil", p.clazz);
  }
  else
    TRAC("SetObjectRpcParamOpt: clazz = %s, params = nil",
         boost::core::demangle(typeid(decltype(*obj)).name()));
}

void CompactExecCommon::SetParams(CompactionParams* params, const Compaction* c) {
  // In the current design, a CompactionJob is always created
  // for non-trivial compaction.
  TERARK_VERIFY(!c->IsTrivialMove() || c->is_manual_compaction());
  const ImmutableOptions& imm_cfo = *c->immutable_options();
  const MutableCFOptions& mut_cfo = *c->mutable_cf_options();
  TERARK_VERIFY(nullptr != m_factory->m_repo);
  const SidePluginRepo& repo = *m_factory->m_repo;
  Logger* info_log = imm_cfo.info_log.get();
  m_env = imm_cfo.env;
  m_log = info_log;
  m_compaction = c;
  m_params = params;
  params->info_log = info_log;
  if (auto coder = imm_cfo.html_user_key_coder.get()) {
    params->p_html_user_key_coder = dynamic_cast<UserKeyCoder*>(coder);
    TERARK_VERIFY(nullptr != params->p_html_user_key_coder);
  }
  params->rocksdb_src_version = ROCKSDB_VERSION;
  params->rocksdb_src_githash = rocksdb_build_git_sha;
  if (NUM_INFO_LOG_LEVELS == m_factory->info_log_level)
    params->compaction_log_level = imm_cfo.info_log_level;
  else
    params->compaction_log_level = m_factory->info_log_level;

#define SetObjectRpcParamReq(cfo, field) \
  SetObjectRpcParamReqTpl(params->field, cfo.field, *params, repo)
#define SetObjectRpcParamOpt(cfo, field) \
  SetObjectRpcParamOptTpl(params->field, cfo.field, *params, repo)

  SetObjectRpcParamOpt(imm_cfo, compaction_filter_factory);
  SetObjectRpcParamOpt(imm_cfo, sst_partitioner_factory);
  SetObjectRpcParamOpt(imm_cfo, html_user_key_coder);
  SetObjectRpcParamOpt(mut_cfo, prefix_extractor);
  SetObjectRpcParamReq(imm_cfo, table_factory);
  SetObjectRpcParamOpt(imm_cfo, merge_operator);
  SetObjectRpcParamOpt(imm_cfo, user_comparator);
  size_t n_listeners = imm_cfo.listeners.size();
  params->listeners.resize(n_listeners);
  for (size_t i = 0; i < n_listeners; ++i) {
    SetObjectRpcParamOpt(imm_cfo, listeners[i]);
  }
  size_t n_tbl_prop_coll = imm_cfo.table_properties_collector_factories.size();
  params->table_properties_collector_factories.resize(n_tbl_prop_coll);
  for (size_t i = 0; i < n_tbl_prop_coll; ++i) {
    SetObjectRpcParamOpt(imm_cfo, table_properties_collector_factories[i]);
  }
  params->allow_ingest_behind = imm_cfo.allow_ingest_behind;
  params->cf_paths = imm_cfo.cf_paths;

  ColumnFamilyData* cfd = c->column_family_data();
  params->output_level = c->output_level();
  params->num_levels = c->number_levels();
  params->cf_id = cfd->GetID();
  params->cf_name = cfd->GetName();
  params->inputs = c->inputs();
  params->target_file_size = c->max_output_file_size();
  params->max_compaction_bytes = c->max_compaction_bytes();
  params->compression = c->output_compression();
  params->compression_opts = c->output_compression_opts();
  params->grandparents = &c->grandparents();
  params->score = c->score();
  params->manual_compaction = c->is_manual_compaction();
  params->deletion_compaction = c->deletion_compaction();
  params->compaction_reason = c->compaction_reason();
  params->bottommost_level = c->bottommost_level();
  params->smallest_user_key = c->GetSmallestUserKey().ToString();
  params->largest_user_key = c->GetLargestUserKey().ToString();
  params->smallest_seqno = c->GetSmallestSeqno();
  params->hoster_root = m_factory->hoster_root;
  params->instance_name = m_factory->instance_name;

  TRAC("CompactExecCommon::SetParams():\n%s", params->DebugString());
}

std::pair<uint64_t, uint64_t> CompactExecCommon::CalcInputRawBytes(
            const std::vector<CompactionInputFiles>* inputs) const {
  std::pair<uint64_t, uint64_t> sum(0, 0);
  for (auto& lv : *inputs) {
    for (auto& f : lv.files) {
      sum.first += f->raw_key_size;
      sum.second += f->raw_value_size;
    }
  }
  return sum;
}

void CompactExecCommon::NotifyResults(FILE* results, const CompactionParams& params) {
  using namespace terark;
  LittleEndianDataInput<NonOwnerFileStream> dio(results);
  const Compaction* c = m_compaction;
  const ImmutableOptions& imm_cfo = *c->immutable_options();
  const MutableCFOptions& mut_cfo = *c->mutable_cf_options();
  const SidePluginRepo& repo = *m_factory->m_repo;
  const auto js = JS_CompactionParamsEncodePtr(&params);
#define NotifyPlugin1(opt, field) \
  SerDe_DeSerialize(results, params.field.clazz, opt.field, js, repo)
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  NotifyPlugin1(imm_cfo, compaction_filter_factory);
  NotifyPlugin1(imm_cfo, user_comparator);
  NotifyPlugin1(imm_cfo, merge_operator);
  NotifyPlugin1(imm_cfo, table_factory);
  NotifyPlugin1(mut_cfo, prefix_extractor);
  NotifyPlugin1(imm_cfo, sst_partitioner_factory);
//NotifyPlugin1(imm_cfo, html_user_key_coder); // not needed
  auto n_listeners = (size_t)dio.read_var_uint64();
  TERARK_VERIFY_EQ(n_listeners, imm_cfo.listeners.size());
  for (size_t i = 0; i < n_listeners; i++) {
    NotifyPlugin1(imm_cfo, listeners[i]);
  }
  auto n_tbl_prop_coll = (size_t)dio.read_var_uint64();
  TERARK_VERIFY_EQ(n_tbl_prop_coll,
                   imm_cfo.table_properties_collector_factories.size());
  for (size_t i = 0; i < n_tbl_prop_coll; i++) {
    NotifyPlugin1(imm_cfo, table_properties_collector_factories[i]);
  }
}

CompactExecFactoryCommon::CompactExecFactoryCommon(const json& js, const SidePluginRepo& repo) {
  m_repo = &repo;
  ROCKSDB_JSON_OPT_PROP(js, dcompact_min_level); // default 2
  ROCKSDB_JSON_OPT_PROP(js, hoster_root);
  ROCKSDB_JSON_OPT_ENUM(js, info_log_level);
  ROCKSDB_JSON_OPT_PROP(js, allow_fallback_to_local);
  if (js.contains("instance_name")) {
    ROCKSDB_JSON_REQ_PROP(js, instance_name);
  } else {
    instance_name.resize(1024);
    int len = gethostname(&instance_name[0], instance_name.size());
    if (len < 0) {
      THROW_STD(logic_error, "gethostname() = %s", strerror(errno));
    }
    instance_name.resize(len);
  }
  auto iter = js.find("dbcf_min_level");
  if (js.end() != iter) {
    // if using this feature, dbname should be same as basename(cf_path[0])
    const json& js_dbcf = iter.value();
    if (!js_dbcf.is_object()) {
      THROW_InvalidArgument("json[dbcf_min_level] must be object");
    }
    for (auto& item : js_dbcf.items()) {
      const std::string& key = item.key(); // db:cf
      const json& val = item.value();
      if (!val.is_number_integer()) {
        THROW_InvalidArgument("json[dbcf_min_level][*].value() must be integer");
      }
      int min_level = val.get<int>();
      m_dbcf_min_level[key] = min_level;
    }
    m_dbcf_min_level_js.reset(new json(js_dbcf));
  }
}
void CompactExecFactoryCommon::ToJson(const json& dump_options, json& djs)
const {
  ROCKSDB_JSON_SET_PROP(djs, dcompact_min_level);
  ROCKSDB_JSON_SET_PROP(djs, hoster_root);
  ROCKSDB_JSON_SET_PROP(djs, instance_name);
  ROCKSDB_JSON_SET_ENUM(djs, info_log_level);
  ROCKSDB_JSON_SET_PROP(djs, allow_fallback_to_local);
  ROCKSDB_JSON_SET_PROP(djs, num_cumu_exec);
  ROCKSDB_JSON_SET_PROP(djs, num_live_exec);
  if (!m_dbcf_min_level.empty()) {
    djs["dbcf_min_level"] = *m_dbcf_min_level_js;
  }
}
void CompactExecFactoryCommon::Update(const json& js) {
  ROCKSDB_JSON_OPT_PROP(js, dcompact_min_level); // default 2
  ROCKSDB_JSON_OPT_ENUM(js, info_log_level);
  ROCKSDB_JSON_OPT_PROP(js, allow_fallback_to_local);
}

bool CompactExecFactoryCommon::ShouldRunLocal(const Compaction* c) const {
  // -1 means unknown level, will run local
  int output_level = c->output_level();
  if (!m_dbcf_min_level.empty()) {
    const std::string& cfname = c->column_family_data()->GetName();
    // use base name of cfpath as dbname, so --
    // if using this feature, dbname should be same as basename(cf_path.back())
    const std::string& cfpath = c->immutable_options()->cf_paths.back().path;
    auto dbname = (const char*)memrchr(cfpath.data(), '/', cfpath.size());
    dbname = dbname ? dbname + 1 : cfpath.data();
    const std::string colon_cfname = ":" + cfname;
    const std::string dbcf = terark::fstring(dbname) + colon_cfname;
    size_t idx = m_dbcf_min_level.find_i(dbcf); // try dbname:cfname
    if (m_dbcf_min_level.end_i() == idx) { // not found dbname:cfname
      idx = m_dbcf_min_level.find_i(dbname); // try dbname
    }
    if (m_dbcf_min_level.end_i() == idx) {
      idx = m_dbcf_min_level.find_i(colon_cfname); // try :cfname
    }
    if (m_dbcf_min_level.end_i() != idx) {
      int min_level = m_dbcf_min_level.val(idx);
      Logger* info_log = c->immutable_options()->info_log.get();
      DEBG("ShouldRunLocal: output_level = %d, min_level[%s ~ %s] = %d",
            output_level, dbcf, m_dbcf_min_level.key(idx), min_level);
      if (-1 == min_level) {
        // this is a corner case(a bit useful for todis hash/set/zset)!
        // run dcompact on bottom most only, if compact this cf needs read
        // meta data from another db or cf, toper level may read many useless
        // meta data, in this case, bottom most level compaction needs little
        // meta data.
        return output_level < dcompact_min_level || !c->bottommost_level();
      }
      return output_level < min_level;
    }
  }
  return output_level < dcompact_min_level;
}

bool CompactExecFactoryCommon::AllowFallbackToLocal() const {
  return allow_fallback_to_local;
}

using terark::as_atomic;
CompactExecCommon::CompactExecCommon(const CompactExecFactoryCommon* fac)
: m_factory(fac) {
  m_compaction = nullptr;
  m_params = nullptr;
  m_env = nullptr;
  m_log = nullptr;
  as_atomic(fac->num_cumu_exec).fetch_add(1, std::memory_order_relaxed);
  as_atomic(fac->num_live_exec).fetch_add(1, std::memory_order_relaxed);
}

CompactExecCommon::~CompactExecCommon() {
  as_atomic(m_factory->num_live_exec).fetch_sub(1, std::memory_order_relaxed);
}

struct CompactExecFactoryCommon_Manip :
          PluginManipFunc<CompactionExecutorFactory> {
  void Update(CompactionExecutorFactory* p, const json&, const json& js,
              const SidePluginRepo& repo) const final {
    if (auto dc = dynamic_cast<CompactExecFactoryCommon*>(p)) {
      dc->Update(js);
      return;
    }
    std::string name = p->Name();
    THROW_InvalidArgument("Is not DistCompaction, but is: " + name);
  }
  std::string ToString(const CompactionExecutorFactory& fac,
                       const json& dump_options,
                       const SidePluginRepo& repo) const final {
    if (auto dc = dynamic_cast<const CompactExecFactoryCommon*>(&fac)) {
      json djs;
      dc->ToJson(dump_options, djs);
      return JsonToString(djs, dump_options);
    }
    std::string name = fac.Name();
    THROW_InvalidArgument("Is not DistCompaction, but is: " + name);
  }
};
ROCKSDB_REG_PluginManip("DcompactCmd", CompactExecFactoryCommon_Manip);
ROCKSDB_REG_PluginManip("DcompactEtcd", CompactExecFactoryCommon_Manip);

void DcompactMeta::FromJsonObj(const json& js) {
  n_listeners = 0;
  n_prop_coll_factory = 0;
  ROCKSDB_JSON_OPT_PROP(js, n_listeners);
  ROCKSDB_JSON_OPT_PROP(js, n_prop_coll_factory);
  ROCKSDB_JSON_REQ_PROP(js, rocksdb_src_version);
  ROCKSDB_JSON_REQ_PROP(js, rocksdb_src_githash);
//ROCKSDB_JSON_REQ_PROP(js, etcd_root);
  ROCKSDB_JSON_REQ_PROP(js, hoster_root);
  ROCKSDB_JSON_REQ_PROP(js, output_root);
  ROCKSDB_JSON_REQ_PROP(js, nfs_mnt_src);
  ROCKSDB_JSON_REQ_PROP(js, nfs_mnt_opt);
  ROCKSDB_JSON_REQ_PROP(js, instance_name);
  ROCKSDB_JSON_REQ_PROP(js, dbname);
  ROCKSDB_JSON_REQ_PROP(js, start_time);
  ROCKSDB_JSON_REQ_PROP(js, job_id);
  ROCKSDB_JSON_REQ_PROP(js, attempt);
}
void DcompactMeta::FromJsonStr(const std::string& jstr) {
  json js = json::parse(jstr);
  FromJsonObj(js);
}
json DcompactMeta::ToJsonObj() const {
  json js;
  if (n_listeners)
    ROCKSDB_JSON_SET_PROP(js, n_listeners);
  if (n_prop_coll_factory)
    ROCKSDB_JSON_SET_PROP(js, n_prop_coll_factory);
  ROCKSDB_JSON_SET_PROP(js, rocksdb_src_version);
  ROCKSDB_JSON_SET_PROP(js, rocksdb_src_githash);
//ROCKSDB_JSON_SET_PROP(js, etcd_root);
  ROCKSDB_JSON_SET_PROP(js, hoster_root);
  ROCKSDB_JSON_SET_PROP(js, output_root);
  ROCKSDB_JSON_SET_PROP(js, nfs_mnt_src);
  ROCKSDB_JSON_SET_PROP(js, nfs_mnt_opt);
  ROCKSDB_JSON_SET_PROP(js, instance_name);
  ROCKSDB_JSON_SET_PROP(js, dbname);
  ROCKSDB_JSON_SET_PROP(js, start_time);
  ROCKSDB_JSON_SET_PROP(js, job_id);
  ROCKSDB_JSON_SET_PROP(js, attempt);
  return js;
}
std::string DcompactMeta::ToJsonStr() const {
  json js = ToJsonObj();
  return js.dump();
}

std::string CatJobID(const std::string& path, const DcompactMeta& meta) {
  return CatJobID(path, meta.job_id);
}
std::string CatAttempt(const std::string& path, const DcompactMeta& meta) {
  return CatAttempt(path, meta.attempt);
}

void JS_ToplingDcompact_AddVersion(json& djs, bool html) {
  auto& ver = djs["topling-dcompact"];
  const char* git_ver = git_version_hash_info_topling_dcompact();
  if (html) {
    std::string git_sha_beg = HtmlEscapeMin(strstr(git_ver, "commit ") + strlen("commit "));
    auto headstr = [](const std::string& s, auto pos) {
      return terark::fstring(s.data(), pos - s.begin());
    };
    auto tailstr = [](const std::string& s, auto pos) {
      return terark::fstring(&*pos, s.end() - pos);
    };
    auto git_sha_end = std::find_if(git_sha_beg.begin(), git_sha_beg.end(), &isspace);
    terark::string_appender<> oss_rocks;
    oss_rocks|"<pre>"
             |"<a href='https://github.com/topling/topling-dcompact/commit/"
             |headstr(git_sha_beg, git_sha_end)|"'>"
             |headstr(git_sha_beg, git_sha_end)|"</a>"
             |tailstr(git_sha_beg, git_sha_end)
             |"</pre>";
    ver = static_cast<std::string&&>(oss_rocks);
  } else {
    ver = git_ver;
  }
}

}
