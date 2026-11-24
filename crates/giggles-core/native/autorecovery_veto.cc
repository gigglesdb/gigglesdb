#include "rocksdb/c.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/listener.h"
#include "rocksdb/options.h"
#include "rocksdb/wal_filter.h"
#include "rocksdb/write_batch.h"

#include <algorithm>
#include <atomic>
#ifndef _WIN32
#include <fcntl.h>
#endif
#if defined(__linux__)
#include <cerrno>
#include <linux/magic.h>
#include <sys/vfs.h>
#endif
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

using namespace ROCKSDB_NAMESPACE;

// The build checks these definitions against the vendored db/c.cc.
struct rocksdb_options_t { Options rep; };

#ifndef GIGGLES_ROCKSDB_AUTORECOVERY_TEST

namespace {
class AutoRecoveryVeto final : public EventListener {
 public:
  void OnErrorRecoveryBegin(BackgroundErrorReason, Status error,
                            bool* auto_recovery) override {
    error.PermitUncheckedError();
    // 11.8.1: SetBGError routes NoSpace to HandleKnownErrors
    // (db/error_handler.cc:470,531). After OverrideNoSpaceError (:349,559),
    // NotifyOnBackgroundError calls this hook (db/event_helpers.cc:65).
    // The flag gates RecoverFromNoSpace and StartErrorRecovery
    // (db/error_handler.cc:375,591). Without the veto, the manager calls
    // RecoverFromBGError (file/sst_file_manager_impl.cc:319), which enters
    // ResumeImpl (db/error_handler.cc:671). The retryable path ignores the
    // flag (:525-528) and is stopped by max_bgerror_resume_count=0 (:694).
    if (*auto_recovery) {
      ++vetoes_;
    }
    *auto_recovery = false;
  }

  uint64_t Vetoes() const { return vetoes_.load(); }

 private:
  std::atomic<uint64_t> vetoes_{0};
};
}  // namespace

extern "C" void giggles_rocksdb_install_autorecovery_veto(
    rocksdb_options_t* options) noexcept {
  options->rep.listeners.push_back(std::make_shared<AutoRecoveryVeto>());
}

extern "C" uint64_t giggles_rocksdb_autorecovery_veto_count(
    const rocksdb_options_t* options) noexcept {
  for (const auto& listener : options->rep.listeners) {
    if (auto* veto = dynamic_cast<const AutoRecoveryVeto*>(listener.get())) {
      return veto->Vetoes();
    }
  }
  return 0;
}

#ifndef _WIN32
extern "C" int giggles_marker_no_follow_flag() noexcept { return O_NOFOLLOW; }
#endif

extern "C" bool giggles_rocksdb_uses_default_env(
    const rocksdb_options_t* options) noexcept {
  auto* env = options->rep.env;
  return env == Env::Default() && env->GetFileSystem() == FileSystem::Default();
}

#if defined(__linux__)
extern "C" int giggles_rocksdb_check_directory_pin(const char* path) noexcept {
  struct statfs info;
  if (statfs(path, &info) != 0) {
    return errno;
  }
  // WSL's 9p-mounted Windows drives can resolve children in the
  // replacement directory even through an open /proc/self/fd handle.
  return info.f_type == V9FS_MAGIC ? EOPNOTSUPP : 0;
}
#endif

namespace {
class GroupReader final : public WriteBatch::Handler {
 public:
  GroupReader(uint32_t meta_cf, const Slice& key,
              std::vector<std::optional<uint64_t>>& groups)
      : meta_cf_(meta_cf), key_(key), groups_(groups) {}

  Status PutCF(uint32_t cf, const Slice& key, const Slice& value) override {
    if (cf == meta_cf_ && key == key_) {
      if (value.size() != sizeof(uint64_t)) {
        groups_.push_back(std::nullopt);
      } else {
        uint64_t group = 0;
        for (size_t i = 0; i < value.size(); ++i) {
          group = (group << 8) | static_cast<unsigned char>(value[i]);
        }
        groups_.push_back(group);
      }
    }
    return Status::OK();
  }

  // Leave other operations to RocksDB's replay handler.
  Status DeleteCF(uint32_t, const Slice&) override { return Status::OK(); }
  Status DeleteRangeCF(uint32_t, const Slice&, const Slice&) override {
    return Status::OK();
  }
  Status SingleDeleteCF(uint32_t, const Slice&) override { return Status::OK(); }
  Status MergeCF(uint32_t, const Slice&, const Slice&) override {
    return Status::OK();
  }
  Status TimedPutCF(uint32_t, const Slice&, const Slice&, uint64_t) override {
    return Status::OK();
  }
  Status PutEntityCF(uint32_t, const Slice&, const Slice&) override {
    return Status::OK();
  }
  Status PutBlobIndexCF(uint32_t, const Slice&, const Slice&) override {
    return Status::OK();
  }
  Status MarkNoop(bool) override { return Status::OK(); }

 private:
  uint32_t meta_cf_;
  Slice key_;
  std::vector<std::optional<uint64_t>>& groups_;
};

enum class FilterMode { Refuse, Chain };
enum class StopReason : uint64_t { None, Gap, OutOfOrder, InvalidValue, Unmarked };

struct WalStop {
  StopReason reason = StopReason::None;
  uint64_t wal = 0;
  uint64_t expected = 0;
  uint64_t found = 0;
  uint64_t sst_log = 0;
};

class CommitGroupFilter final : public WalFilter {
 public:
  CommitGroupFilter(const char* meta_name, const char* key, size_t key_len)
      : meta_name_(meta_name), key_(key, key_len) {}

  const char* Name() const override { return "GigglesCommitGroup"; }

  // The filter is supplied at runtime. Keep the OPTIONS file readable by
  // stock RocksDB tools that cannot reconstruct this object.
  std::string GetId() const override { return "nullptr"; }

  void ColumnFamilyLogNumberMap(
      const std::map<uint32_t, uint64_t>& log_numbers,
      const std::map<std::string, uint32_t>& cf_ids) noexcept override {
    meta_cf_.reset();
    last_ = seed_;
    stop_ = {};
    if (auto meta = cf_ids.find(meta_name_); meta != cf_ids.end()) {
      meta_cf_ = meta->second;
    }
    live_sst_log_ = 0;
    for (const auto& name : live_sst_cfs_) {
      auto cf = cf_ids.find(name);
      if (cf != cf_ids.end()) {
        auto log = log_numbers.find(cf->second);
        if (log != log_numbers.end()) {
          live_sst_log_ = std::max(live_sst_log_, log->second);
        }
      }
    }
  }

  WalProcessingOption LogRecordFound(unsigned long long wal,
                                      const std::string&, const WriteBatch& batch,
                                      WriteBatch* replacement,
                                      bool* batch_changed) noexcept override {
    if (!meta_cf_) {
      return WalProcessingOption::kContinueProcessing;
    }
    if (mode_ == FilterMode::Refuse) {
      const auto& data = batch.Data();
      if (data.find(key_.data(), 0, key_.size()) == std::string::npos) {
        return WalProcessingOption::kContinueProcessing;
      }
    }
    record_groups_.clear();
    GroupReader reader(*meta_cf_, key_, record_groups_);
    const bool valid = batch.Iterate(&reader).ok();
    if (mode_ == FilterMode::Refuse && !record_groups_.empty()) {
      stop_ = {StopReason::Unmarked, wal, 0, record_groups_.front().value_or(0), 0};
      Refuse(batch, replacement, batch_changed);
    } else if (valid) {
      for (auto group : record_groups_) {
        if (!CheckGroup(wal, group)) {
          continue;
        }
        if (stop_.reason == StopReason::Gap && stop_.sst_log <= wal) {
          return WalProcessingOption::kStopReplay;
        }
        Refuse(batch, replacement, batch_changed);
        break;
      }
    }
    // RocksDB handles structurally invalid batches using its recovery mode.
    return WalProcessingOption::kContinueProcessing;
  }

  void NoteLiveSst(const char* name) {
    live_sst_cfs_.emplace_back(name);
  }

  void BeginReplay(uint64_t seed) noexcept {
    mode_ = FilterMode::Chain;
    seed_ = last_ = seed;
    stop_ = {};
  }

  WalStop stop() const { return stop_; }

 private:
  static void Refuse(const WriteBatch& batch, WriteBatch* replacement,
                     bool* batch_changed) {
    // A filter stop retires WALs, and point-in-time recovery can proceed
    // after kCorruptedRecord. The replacement-count check instead aborts
    // both modes before committing any recovery edits. Marked stores run
    // this filter read-only first so a refusal leaves live files untouched.
    // See db_impl_open.cc:1136-1155,1727. The replacement is never applied.
    *replacement = WriteBatch(batch.Data());
    replacement->Put(Slice(), Slice()).PermitUncheckedError();
    *batch_changed = true;
  }

  bool CheckGroup(uint64_t wal, std::optional<uint64_t> group) {
    if (group && *group <= seed_) {
      return false;
    }
    // last_ may reach UINT64_MAX. No higher group exists, so a later put above
    // seed_ always repeats or goes backwards in that case.
    const uint64_t expected = last_ == UINT64_MAX ? last_ : last_ + 1;
    if (group && *group > last_ && *group == expected) {
      last_ = *group;
      return false;
    }

    const auto reason = !group ? StopReason::InvalidValue
                         : *group <= last_ ? StopReason::OutOfOrder
                                           : StopReason::Gap;
    stop_ = {reason, wal, expected, group.value_or(0), live_sst_log_};
    return true;
  }

  const std::string meta_name_;
  const Slice key_;
  FilterMode mode_ = FilterMode::Refuse;
  uint64_t seed_ = 0;
  uint64_t last_ = 0;
  std::optional<uint32_t> meta_cf_;
  uint64_t live_sst_log_ = 0;
  std::vector<std::string> live_sst_cfs_;
  std::vector<std::optional<uint64_t>> record_groups_;
  WalStop stop_;
};
}  // namespace

extern "C" CommitGroupFilter* giggles_rocksdb_new_commit_group_filter(
    const char* meta_name, const char* key, size_t key_len) noexcept {
  // key points to immutable Rust static storage.
  return new CommitGroupFilter(meta_name, key, key_len);
}

extern "C" void giggles_rocksdb_set_commit_group_filter(
    rocksdb_options_t* options, CommitGroupFilter* filter) noexcept {
  options->rep.wal_filter = filter;
}

extern "C" void giggles_rocksdb_commit_group_filter_live_sst(
    CommitGroupFilter* filter, const char* name) noexcept {
  filter->NoteLiveSst(name);
}

extern "C" void giggles_rocksdb_begin_commit_group_replay(
    CommitGroupFilter* filter, uint64_t seed) noexcept {
  filter->BeginReplay(seed);
}

extern "C" void giggles_rocksdb_commit_group_stop(
    const CommitGroupFilter* filter, WalStop* stop) noexcept {
  *stop = filter->stop();
}

extern "C" void giggles_rocksdb_drop_commit_group_filter(
    CommitGroupFilter* filter) noexcept {
  delete filter;
}

#else

#include <utility>

struct rocksdb_env_t { Env* rep; bool is_default; };

namespace {
struct WalFaultCounts {
  uint64_t sync_failures;
  uint64_t dir_sync_failures;
  uint64_t full_space_checks;
  uint64_t flushes;
  uint64_t recoveries;
  uint64_t recovery_begins_enabled;
};

struct WalFaultState {
  std::atomic<bool> armed{false};
  std::atomic<bool> dir_armed{false};
  std::atomic<uint64_t> sync_failures{0};
  std::atomic<uint64_t> dir_sync_failures{0};
  std::atomic<uint64_t> full_space_checks{0};
  std::atomic<uint64_t> flushes{0};
  std::atomic<uint64_t> recoveries{0};
  std::atomic<uint64_t> recovery_begins_enabled{0};
};

class FaultWalFile final : public FSWritableFileOwnerWrapper {
 public:
  FaultWalFile(std::unique_ptr<FSWritableFile> file,
               std::shared_ptr<WalFaultState> state)
      : FSWritableFileOwnerWrapper(std::move(file)), state_(std::move(state)) {}

  IOStatus Sync(const IOOptions& options, IODebugContext* debug) override {
    if (state_->armed.load()) {
      ++state_->sync_failures;
      return IOStatus::NoSpace("injected WAL Sync");
    }
    return target()->Sync(options, debug);
  }

  IOStatus Fsync(const IOOptions& options, IODebugContext* debug) override {
    if (state_->armed.load()) {
      ++state_->sync_failures;
      return IOStatus::NoSpace("injected WAL Fsync");
    }
    return target()->Fsync(options, debug);
  }

 private:
  std::shared_ptr<WalFaultState> state_;
};

class FaultWalDirectory final : public FSDirectoryWrapper {
 public:
  FaultWalDirectory(std::unique_ptr<FSDirectory> directory,
                    std::shared_ptr<WalFaultState> state)
      : FSDirectoryWrapper(std::move(directory)), state_(std::move(state)) {}

  IOStatus FsyncWithDirOptions(const IOOptions& options, IODebugContext* debug,
                               const DirFsyncOptions& dir_options) override {
    if (dir_options.reason == DirFsyncOptions::kNewFileSynced &&
        state_->dir_armed.exchange(false)) {
      ++state_->dir_sync_failures;
      return IOStatus::IOError("injected WAL directory fsync");
    }
    return FSDirectoryWrapper::FsyncWithDirOptions(options, debug, dir_options);
  }

 private:
  std::shared_ptr<WalFaultState> state_;
};

class FaultWalFileSystem final : public FileSystemWrapper {
 public:
  FaultWalFileSystem(std::shared_ptr<WalFaultState> state, std::string wal_dir)
      : FileSystemWrapper(FileSystem::Default()), state_(std::move(state)),
        wal_dir_(std::move(wal_dir)) {}

  const char* Name() const override { return "GigglesFaultWalFileSystem"; }

  IOStatus NewWritableFile(const std::string& path, const FileOptions& options,
                           std::unique_ptr<FSWritableFile>* file,
                           IODebugContext* debug) override {
    auto status = target()->NewWritableFile(path, options, file, debug);
    if (status.ok() && path.ends_with(".log")) {
      *file = std::make_unique<FaultWalFile>(std::move(*file), state_);
    }
    return status;
  }

  IOStatus NewDirectory(const std::string& path, const IOOptions& options,
                         std::unique_ptr<FSDirectory>* directory,
                         IODebugContext* debug) override {
    auto status = target()->NewDirectory(path, options, directory, debug);
    if (status.ok() && !wal_dir_.empty() && path == wal_dir_) {
      *directory =
          std::make_unique<FaultWalDirectory>(std::move(*directory), state_);
    }
    return status;
  }

  IOStatus GetFreeSpace(const std::string&, const IOOptions&, uint64_t* bytes,
                        IODebugContext*) override {
    if (state_->armed.load()) {
      *bytes = 0;
      ++state_->full_space_checks;
    } else {
      *bytes = uint64_t{1} << 40;
    }
    return IOStatus::OK();
  }

 private:
  std::shared_ptr<WalFaultState> state_;
  const std::string wal_dir_;
};

class RecoveryObserver final : public EventListener {
 public:
  explicit RecoveryObserver(std::shared_ptr<WalFaultState> state)
      : state_(std::move(state)) {}

  void OnFlushCompleted(DB*, const FlushJobInfo&) override { ++state_->flushes; }

  void OnErrorRecoveryBegin(BackgroundErrorReason, Status error,
                            bool* auto_recovery) override {
    error.PermitUncheckedError();
    if (*auto_recovery) {
      ++state_->recovery_begins_enabled;
    }
  }

  void OnErrorRecoveryEnd(const BackgroundErrorRecoveryInfo& info) override {
    if (info.new_bg_error.ok()) {
      ++state_->recoveries;
    }
  }

 private:
  std::shared_ptr<WalFaultState> state_;
};

struct FaultEnv {
  std::shared_ptr<WalFaultState> state;
  std::unique_ptr<Env> env;
};
}  // namespace

extern "C" FaultEnv* giggles_rocksdb_install_wal_fault(
    rocksdb_options_t* options, bool keep_veto) noexcept {
  try {
    auto state = std::make_shared<WalFaultState>();
    auto fs = std::make_shared<FaultWalFileSystem>(state, options->rep.wal_dir);
    auto handle = std::make_unique<FaultEnv>(FaultEnv{state, NewCompositeEnv(fs)});
    if (!keep_veto) {
      // The production options builder installs only the veto listener.
      options->rep.listeners.clear();
    }
    options->rep.listeners.push_back(std::make_shared<RecoveryObserver>(state));
    rocksdb_env_t env{handle->env.get(), false};
    rocksdb_options_set_env(options, &env);
    return handle.release();
  } catch (...) {
    return nullptr;
  }
}

extern "C" void giggles_rocksdb_drop_wal_fault(FaultEnv* handle) noexcept {
  delete handle;
}

extern "C" void giggles_rocksdb_arm_wal_fault(FaultEnv* handle,
                                             bool armed) noexcept {
  handle->state->armed.store(armed);
}

extern "C" void giggles_rocksdb_arm_wal_dir_fault(FaultEnv* handle) noexcept {
  handle->state->dir_armed.store(true);
}

extern "C" void giggles_rocksdb_wal_fault_counts(const FaultEnv* handle,
                                                WalFaultCounts* counts) noexcept {
  const auto& state = *handle->state;
  *counts = {state.sync_failures.load(), state.dir_sync_failures.load(),
             state.full_space_checks.load(),
             state.flushes.load(), state.recoveries.load(),
             state.recovery_begins_enabled.load()};
}

#endif
