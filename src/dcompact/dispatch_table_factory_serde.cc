//
// Created by leipeng on 2021-01-27.
//
#if defined(_MSC_VER)
//#error: _CRT_NONSTDC_NO_DEPRECATE must be defined to use posix functions on Visual C++
#define _CRT_NONSTDC_NO_DEPRECATE
#define TERARK_DATA_IO_SLOW_VAR_INT
#endif
#include <topling/internal_dispather_table.h>
#include <terark/io/DataIO.hpp>
#include <terark/io/FileStream.hpp>
#include <terark/util/atomic.hpp>
#include <topling/side_plugin_internal.h>

#if 0
  #define TRACE(fmt, ...) \
    fprintf(stderr, "%s:%d: " fmt "\n", __FILE__, \
            TERARK_PP_SmartForPrintf(__LINE__, ##__VA_ARGS__))
#else
  #define TRACE(fmt, ...)
#endif

namespace ROCKSDB_NAMESPACE {

using namespace terark;

using DioWriter = LittleEndianDataOutput<NonOwnerFileStream>;
using DioReader = LittleEndianDataInput<NonOwnerFileStream>;

using Stat = DispatcherTableFactory::Stat;
DATA_IO_DUMP_RAW_MEM_E(Stat)

class DispatcherTableFactoryEx : public DispatcherTableFactory {
public:
  std::map<const TableFactory*, std::string> GetPtrToNameMap() const {
    std::map<const TableFactory*, std::string> ptr2name;
    std::vector<const TableFactory*> uniq;
    for (auto& kv : *m_all) {
      if (!dynamic_cast<DispatcherTableFactory*>(kv.second.get())) {
        uniq.push_back(kv.second.get());
        ptr2name.emplace(kv.second.get(), kv.first);
      }
    }
    std::sort(uniq.begin(), uniq.end());
    uniq.erase(std::unique(uniq.begin(), uniq.end()), uniq.end());
    for (auto iter = ptr2name.begin(); ptr2name.end() != iter; ) {
      if (std::binary_search(uniq.begin(), uniq.end(), iter->first)) {
         ++iter; // keep
      } else {
        iter = ptr2name.erase(iter);
      }
    }
    return ptr2name;
  }
  void HosterSide_Serialize(DioWriter& dio) const {
    auto ptr2name = GetPtrToNameMap();
    //dio << m_json_str; // has been set by cons
    dio << terark::var_size_t(ptr2name.size());
    for (auto& kv : ptr2name) {
      const TableFactory* tf = kv.first;
      const std::string& name = kv.second;
      const std::string clazz = tf->Name();
      auto ith = lower_bound_ex_a(m_factories_spec, tf, TERARK_GET(.first));
      TERARK_VERIFY_LT(ith, m_factories_spec.size());
      std::string cons_jstr = m_factories_spec[ith].second->dump();
      dio << name << clazz << cons_jstr;
      SerDe_SerializeOpt(dio, clazz, tf);
    }
  }
  void WorkerSide_DeSerialize(DioReader& dio) {
    size_t num = dio.load_as<terark::var_size_t>();
    SidePluginRepo repo;
    for (size_t i = 0; i < num; ++i) {
      std::string name, clazz, cons_jstr;
      dio >> name >> clazz >> cons_jstr;
      TRACE("beg: i = %zd, num = %zd, clazz = %s, cons_jstr = %s", i, num, clazz, cons_jstr);
      json js = json::parse(cons_jstr);
      auto tf = PluginFactorySP<TableFactory>::AcquirePlugin(js, repo);
      TERARK_VERIFY_F( // clazz maybe diff to tf->Name(), but are same plugin
          PluginFactorySP<TableFactory>::SamePlugin(tf->Name(), clazz),
          "%s : %s", tf->Name(), clazz.c_str());
      SerDe_DeSerialize(dio, clazz, tf.get());
      repo.Put(name, tf);
      repo.m_impl->table_factory.p2name[tf.get()].spec = std::move(js);
      TRACE("end: i = %zd, num = %zd", i, num);
    }
    BackPatch(repo); // NOLINT
    TRACE("After BackPatch()");
  }
  void WorkerSide_SerializeSub(DioWriter& dio) const {
    auto ptr2name = GetPtrToNameMap();
    dio << terark::var_size_t(ptr2name.size());
    for (auto& kv : ptr2name) {
      const TableFactory* tf = kv.first;
      const std::string& name = kv.second;
      const std::string clazz = tf->Name();
      dio << name;
      SerDe_SerializeOpt(dio, clazz, tf);
    }
  }
  void WorkerSide_Serialize(DioWriter& dio) const {
    size_t level, num_levels = m_stats[0].size();
    for (level = 0; level < num_levels; ++level) {
      if (m_stats[0][level].st.entry_cnt) {
        dio.writeByte(1);
        dio << terark::var_size_t(level);
        dio << m_stats[0][level].st;
        dio << m_writer_files[level];
        WorkerSide_SerializeSub(dio);
        return;
      }
    }
    dio.writeByte(0);
  }
  void HosterSide_DeSerialize(DioReader& dio) {
    const auto hasData = dio.readByte();
    if (hasData) {
      Stat st;
      size_t writer_files = 0;
      size_t level = (size_t)dio.load_as<terark::var_size_t>();
      dio >> st;
      dio >> writer_files;
      HosterSide_DeSerializeSub(dio);
      as_atomic(m_writer_files[level]).fetch_add(writer_files);
      this->UpdateStat(level, st); // NOLINT
      as_atomic(m_stats[0][level].st.file_size).fetch_add(
                st.file_size, std::memory_order_relaxed);
    }
  }
  void HosterSide_DeSerializeSub(DioReader& dio) {
    size_t num = dio.load_as<terark::var_size_t>();
    for (size_t i = 0; i < num; ++i) {
      std::string name;
      dio >> name;
      auto iter = m_all->find(name);
      if (m_all->end() == iter) {
        THROW_Corruption("name = " + name + " is missing");
      } else {
        SerDe_DeSerialize(dio, iter->second);
      }
    }
  }
};

struct DispatcherTableFactory_SerDe : SerDeFunc<TableFactory> {
  void Serialize(FILE* fp, const TableFactory& object)
  const override {
    auto& f1 = dynamic_cast<const DispatcherTableFactory&>(object);
    auto& factory = static_cast<const DispatcherTableFactoryEx&>(f1);
    DioWriter output(fp);
    if (IsCompactionWorker()) {
      factory.WorkerSide_Serialize(output);
    } else {
      factory.HosterSide_Serialize(output);
    }
  }
  void DeSerialize(FILE* fp, TableFactory* object)
  const override {
    auto* f1 = dynamic_cast<DispatcherTableFactory*>(object);
    auto* factory = static_cast<DispatcherTableFactoryEx*>(f1);
    DioReader input(fp);
    if (IsCompactionWorker()) {
      factory->WorkerSide_DeSerialize(input);
    } else {
      factory->HosterSide_DeSerialize(input);
    }
  }
};
ROCKSDB_REG_PluginSerDe("DispatcherTable", DispatcherTableFactory_SerDe);
ROCKSDB_REG_PluginSerDe("Dispather", DispatcherTableFactory_SerDe);
ROCKSDB_REG_PluginSerDe("Dispath", DispatcherTableFactory_SerDe);


} // ROCKSDB_NAMESPACE
