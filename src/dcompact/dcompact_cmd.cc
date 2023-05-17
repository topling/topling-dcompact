//
// Created by leipeng on 2021/3/26.
//
#include "dcompact_executor.h"
#include <topling/side_plugin_factory.h>

#include <terark/num_to_str.hpp>
#include <terark/util/process.hpp>
#include <chrono>

namespace ROCKSDB_NAMESPACE {

using namespace terark;

class DcompactCmdExecFactory : public CompactExecFactoryCommon {
 public:
  std::string cmd;
  std::string vfork_temp_dir;
  DcompactCmdExecFactory(const json& js, const SidePluginRepo& repo) {
    CompactExecFactoryCommon::init(js, repo);
    vfork_temp_dir = "/tmp/dcompact-vfork";  // default
    ROCKSDB_JSON_REQ_PROP(js, cmd);
    ROCKSDB_JSON_OPT_PROP(js, vfork_temp_dir);
  }
  CompactionExecutor* NewExecutor(const Compaction*) const final;
  const char* Name() const final { return "DcompactCmd"; }
  void ToJson(const json& dump_options, json& djs, const SidePluginRepo& repo) const final {
    bool html = JsonSmartBool(dump_options, "html", true);
    ROCKSDB_JSON_SET_TMPL(djs, compaction_executor_factory);
    ROCKSDB_JSON_SET_PROP(djs, cmd);
    ROCKSDB_JSON_SET_PROP(djs, vfork_temp_dir);
    CompactExecFactoryCommon::ToJson(dump_options, djs, repo);
  }
  void Update(const json&) final {}
};

class DcompactCmdExec : public CompactExecCommon {
  std::string m_cmd;
 public:
  explicit DcompactCmdExec(const DcompactCmdExecFactory* fac)
      : CompactExecCommon(fac), m_cmd(fac->cmd) {}
  Status Execute(const CompactionParams& params,
                 CompactionResults* results) override try {
    using namespace std::chrono;
    auto const t0 = steady_clock::now();
    auto write_pipe = [&](ProcPipeStream& pipe) {
      SerDeWrite(pipe.fp(), &params);
      pipe.flush();
      auto t1 = steady_clock::now();
      results->curl_time_usec = duration_cast<microseconds>(t1-t0).count();
    };
    terark::string_appender<> cmd;
    cmd.reserve(m_cmd.size() + 100);
    cmd << "env JOB_ID=" << params.job_id << ' ' << m_cmd;
    results->status = Status::Incomplete("executing command: " + cmd);
    auto factory = static_cast<const DcompactCmdExecFactory*>(m_factory);
    auto future = vfork_cmd(cmd, ref(write_pipe), factory->vfork_temp_dir);
    auto output = future.get();
    SerDeRead(output, results);
    return Status::OK();
  } catch (const std::exception& ex) {
    return Status::Corruption(ROCKSDB_FUNC, ex.what());
  } catch (...) {
    return Status::Corruption(ROCKSDB_FUNC, "caught unknown exception");
  }
  void CleanFiles(const CompactionParams&,
                  const CompactionResults& results) override {
    if (!results.output_dir.empty()) {
      m_env->DeleteFile(results.output_dir);
    }
  }
};

CompactionExecutor*
DcompactCmdExecFactory::NewExecutor(const Compaction*) const {
  return new DcompactCmdExec(this);
}

ROCKSDB_REG_Plugin("DcompactCmd",
                    DcompactCmdExecFactory,
                    CompactionExecutorFactory);

} // ROCKSDB_NAMESPACE
