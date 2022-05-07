//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <unordered_map>

#include "catalog/catalog.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

std::unordered_map<txn_id_t, Transaction *> TransactionManager::txn_map = {};

Transaction *TransactionManager::Begin(Transaction *txn, IsolationLevel isolation_level) {
  // Acquire the global transaction latch in shared mode.
  global_txn_latch_.RLock();  // 加读锁

  if (txn == nullptr) {
    txn = new Transaction(next_txn_id_++, isolation_level);  // 新建一个事务
  }

  if (enable_logging) {
    assert(txn->GetPrevLSN() == INVALID_LSN);
    // 新建一个事务开始日志记录
    LogRecord log_record{txn->GetTransactionId(), txn->GetPrevLSN(), LogRecordType::BEGIN};
    // 加入记录，并设置 prev_lsn
    txn->SetPrevLSN(log_manager_->AppendLogRecord(&log_record));
  }

  txn_map[txn->GetTransactionId()] = txn;  // 注册事务
  return txn;
}

void TransactionManager::Commit(Transaction *txn) {
  txn->SetState(TransactionState::COMMITTED);  // 设置事务状态

  if (enable_logging) {
    // 事务提交日志
    LogRecord log_record{txn->GetTransactionId(), txn->GetPrevLSN(), LogRecordType::COMMIT};
    txn->SetPrevLSN(log_manager_->AppendLogRecord(&log_record));
    log_manager_->Flush(false);  // 提交后记得刷磁盘，非强制
  }

  // Perform all deletes before we commit. 调用所有 delete 操作
  auto write_set = txn->GetWriteSet();
  while (!write_set->empty()) {
    auto &item = write_set->back();
    auto table = item.table_;
    if (item.wtype_ == WType::DELETE) {
      // Note that this also releases the lock when holding the page latch.
      table->ApplyDelete(item.rid_, txn);
    }
    write_set->pop_back();  // 队列弹出
  }
  write_set->clear();

  // Release all the locks.
  ReleaseLocks(txn);  // 释放事务上的锁
  // Release the global transaction latch.
  global_txn_latch_.RUnlock();  // 释放读锁
}

void TransactionManager::Abort(Transaction *txn) {
  txn->SetState(TransactionState::ABORTED);  // 设置状态
  if (enable_logging) {
    // 记录终止日志
    LogRecord log_record{txn->GetTransactionId(), txn->GetPrevLSN(), LogRecordType::ABORT};
    txn->SetPrevLSN(log_manager_->AppendLogRecord(&log_record));
    log_manager_->Flush(false);
  }
  // Rollback before releasing the lock.
  auto table_write_set = txn->GetWriteSet();
  while (!table_write_set->empty()) {
    auto &item = table_write_set->back();
    auto table = item.table_;
    if (item.wtype_ == WType::DELETE) {
      table->RollbackDelete(item.rid_, txn);    // 如果是删除，那么回退删除
    } else if (item.wtype_ == WType::INSERT) {  // 如果是插入，那么删掉
      // Note that this also releases the lock when holding the page latch.
      table->ApplyDelete(item.rid_, txn);
    } else if (item.wtype_ == WType::UPDATE) {  // 如果是更新，那么更新为原来的状态
      table->UpdateTuple(item.tuple_, item.rid_, txn);
    }
    table_write_set->pop_back();
  }
  table_write_set->clear();
  // Rollback index updates
  auto index_write_set = txn->GetIndexWriteSet();  // 索引写集
  while (!index_write_set->empty()) {
    auto &item = index_write_set->back();
    auto catalog = item.catalog_;
    // Metadata identifying the table that should be deleted from.
    TableMetadata *table_info = catalog->GetTable(item.table_oid_);
    IndexInfo *index_info = catalog->GetIndex(item.index_oid_);
    auto new_key = item.tuple_.KeyFromTuple(table_info->schema_, *(index_info->index_->GetKeySchema()),
                                            index_info->index_->GetKeyAttrs());
    if (item.wtype_ == WType::DELETE) {
      index_info->index_->InsertEntry(new_key, item.rid_, txn);  // 逆操作
    } else if (item.wtype_ == WType::INSERT) {
      index_info->index_->DeleteEntry(new_key, item.rid_, txn);  // 逆操作
    } else if (item.wtype_ == WType::UPDATE) {                   // 逆操作
      // Delete the new key and insert the old key
      index_info->index_->DeleteEntry(new_key, item.rid_, txn);
      // 这个地方有BUG，必须传入 old_tuple
      auto old_key = item.old_tuple_.KeyFromTuple(table_info->schema_, *(index_info->index_->GetKeySchema()),
                                                  index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(old_key, item.rid_, txn);
    }
    index_write_set->pop_back();
  }
  table_write_set->clear();
  index_write_set->clear();

  // Release all the locks.
  ReleaseLocks(txn);
  // Release the global transaction latch.
  global_txn_latch_.RUnlock();
}

// 直接加全局写锁
void TransactionManager::BlockAllTransactions() { global_txn_latch_.WLock(); }

// 释放全局写锁
void TransactionManager::ResumeTransactions() { global_txn_latch_.WUnlock(); }

}  // namespace bustub
