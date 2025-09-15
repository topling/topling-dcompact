//
// Created by leipeng on 2021/2/23.
//
#pragma once

#include <db/compaction/compaction_executor.h>
#include <topling/json_fwd.h>
#include <terark/hash_strmap.hpp>

namespace ROCKSDB_NAMESPACE {

using nlohmann::json;

class CompactExecFactoryCommon : public CompactionExecutorFactory {
public:
  const class SidePluginRepo* m_repo;
  int dcompact_min_level = 2;
  int dcompact_max_level = INT_MAX;
  InfoLogLevel info_log_level = NUM_INFO_LOG_LEVELS;
  bool allow_fallback_to_local = false;
  bool need_db_get = false;
  std::string hoster_root;
  std::string instance_name; // default is hostname
  mutable int num_cumu_exec = 0;
  mutable int num_live_exec = 0;

  void init(const json&, const class SidePluginRepo&);

  bool ShouldRunLocal(const Compaction*) const override;
  bool AllowFallbackToLocal() const override;
  terark::implicit_convertible_fstring
  basename(const std::string& path, bool strict) const;

  // new virtual functions
  virtual void ToJson(const json& dump_options, json&, const SidePluginRepo&) const;
  virtual void Update(const json&);
};

class CompactExecCommon : public CompactionExecutor {
 protected:
  const CompactExecFactoryCommon* m_factory;
  const Compaction*               m_compaction;
  const CompactionParams*         m_params;
  Env* m_env;
  Logger* m_log;

  std::pair<uint64_t, uint64_t>
    CalcInputRawBytes(const std::vector<CompactionInputFiles>*) const;

 public:
  explicit CompactExecCommon(const CompactExecFactoryCommon* fac);
  ~CompactExecCommon() override;
  void SetParams(CompactionParams*, const Compaction*) override;
  void NotifyResults(FILE*, const CompactionParams& params);
  Status CopyOneFile(const std::string& src, const std::string& dst, off_t fsize) override;
  Status RenameFile(const std::string& src, const std::string& dst, off_t fsize) override;
};

struct DcompactMeta {
  uint32_t    n_subcompacts = 1;
  uint16_t    n_listeners = 0;
  uint16_t    n_prop_coll_factory = 0;
  uint32_t    code_version = 0;
  std::string code_githash;
//std::string etcd_root;
  std::string hoster_root;
  std::string output_root; // output_dir = "output_root/${job_id}"
  std::string nfs_type;
  std::string nfs_mnt_src;
  std::string nfs_mnt_opt;
  std::string instance_name;
  std::string dbname; // dbname in SidePluginRepo, not dbpath
  std::string start_time; // hoster process start time
  uint64_t start_time_epoch; // time_t of start_time
  size_t   estimate_time_us = 0;
  int job_id = -1;
  int attempt = 0;
  int output_level = 0;
  void FromJsonObj(const json&);
  void FromJsonStr(const std::string&);
  json ToJsonObj() const;
  std::string ToJsonStr() const;
};

struct DcompactFeeReport {
  std::string provider;   // cloud provider
  std::string instanceId; // DB instance
  std::string labourId;   // dcompact worker/labour id
  std::string dbId;
  uint64_t dbStarts; // DB node start time, from epoch in second
  uint64_t starts;   // compact start time, from epoch in second
  size_t executesMs; // compact execution time duration, in milliseconds
  int compactionJobId;
  int attempt;

  uint64_t compactionInputRawBytes;
  uint64_t compactionInputZipBytes;
  uint64_t compactionOutputRawBytes;
  uint64_t compactionOutputZipBytes;
//std::string jsonInfo;

  json ToJson() const;
};

void SerDeRead(FILE*, CompactionParams*);
void SerDeRead(FILE*, CompactionResults*);
void SerDeRead(Slice, CompactionResults*);
void SerDeWrite(FILE*, const CompactionParams*);
void SerDeWrite(FILE*, const CompactionResults*);

void SetAsCompactionWorker(); // defined in compaction_executor.cc
std::string CatJobID(const std::string& path, const DcompactMeta& meta);
std::string CatAttempt(const std::string& path, const DcompactMeta& meta);

} // ROCKSDB_NAMESPACE
