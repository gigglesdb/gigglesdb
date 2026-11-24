#include "rocksdb/c.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/listener.h"
#include "rocksdb/options.h"

#include <memory>

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
    *auto_recovery = false;
  }
};
}  // namespace

extern "C" void giggles_rocksdb_install_autorecovery_veto(
    rocksdb_options_t* options) noexcept {
  options->rep.listeners.push_back(std::make_shared<AutoRecoveryVeto>());
}

extern "C" bool giggles_rocksdb_uses_default_env(
    const rocksdb_options_t* options) noexcept {
  auto* env = options->rep.env;
  return env == Env::Default() && env->GetFileSystem() == FileSystem::Default();
}

#else

#include <atomic>
#include <cstdint>
#include <utility>

struct rocksdb_env_t { Env* rep; bool is_default; };

namespace {
struct WalFaultCounts {
  uint64_t sync_failures;
  uint64_t full_space_checks;
  uint64_t flushes;
  uint64_t recoveries;
};

struct WalFaultState {
  std::atomic<bool> armed{false};
  std::atomic<uint64_t> sync_failures{0};
  std::atomic<uint64_t> full_space_checks{0};
  std::atomic<uint64_t> flushes{0};
  std::atomic<uint64_t> recoveries{0};
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

class FaultWalFileSystem final : public FileSystemWrapper {
 public:
  explicit FaultWalFileSystem(std::shared_ptr<WalFaultState> state)
      : FileSystemWrapper(FileSystem::Default()), state_(std::move(state)) {}

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
};

class RecoveryObserver final : public EventListener {
 public:
  explicit RecoveryObserver(std::shared_ptr<WalFaultState> state)
      : state_(std::move(state)) {}

  void OnFlushCompleted(DB*, const FlushJobInfo&) override { ++state_->flushes; }

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
    auto handle = std::make_unique<FaultEnv>(
        FaultEnv{state, NewCompositeEnv(std::make_shared<FaultWalFileSystem>(state))});
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

extern "C" void giggles_rocksdb_wal_fault_counts(const FaultEnv* handle,
                                                WalFaultCounts* counts) noexcept {
  const auto& state = *handle->state;
  *counts = {state.sync_failures.load(), state.full_space_checks.load(),
             state.flushes.load(), state.recoveries.load()};
}

#endif
