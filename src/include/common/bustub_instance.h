//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// bustub_instance.h
//
// Identification: src/include/common/bustub_instance.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>

#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "concurrency/lock_manager.h"
#include "recovery/checkpoint_manager.h"
#include "recovery/log_manager.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

class BustubInstance {  // db 实例
 public:
  /*
   * @db_file_name 数据库文件名称
   */
  explicit BustubInstance(const std::string &db_file_name) {
    enable_logging = false;

    // storage related
    disk_manager_ = new DiskManager(db_file_name);  // 磁盘存储管理器

    // log related
    log_manager_ = new LogManager(disk_manager_);  // 日志管理器

    buffer_pool_manager_ = new BufferPoolManager(BUFFER_POOL_SIZE, disk_manager_, log_manager_);  // 缓存池管理器

    // txn related
    lock_manager_ = new LockManager();                                           // 锁管理器
    transaction_manager_ = new TransactionManager(lock_manager_, log_manager_);  // 事务管理器

    // checkpoints  检查点管理器，防止 crash 恢复
    checkpoint_manager_ = new CheckpointManager(transaction_manager_, log_manager_, buffer_pool_manager_);
  }

  ~BustubInstance() {
    if (enable_logging) {
      log_manager_->StopFlushThread();
    }
    delete checkpoint_manager_;
    delete log_manager_;
    delete buffer_pool_manager_;
    delete lock_manager_;
    delete transaction_manager_;
    delete disk_manager_;
  }

  DiskManager *disk_manager_;
  BufferPoolManager *buffer_pool_manager_;
  LockManager *lock_manager_;
  TransactionManager *transaction_manager_;
  LogManager *log_manager_;
  CheckpointManager *checkpoint_manager_;
};

}  // namespace bustub
