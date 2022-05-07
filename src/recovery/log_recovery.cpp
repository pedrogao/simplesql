//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// log_recovery.cpp
//
// Identification: src/recovery/log_recovery.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "recovery/log_recovery.h"

#include "storage/page/table_page.h"

namespace bustub {
/*
 * deserialize a log record from log buffer
 * @link: https://www.bdwms.com/?p=750
 * @return: true means deserialize succeed, otherwise can't deserialize cause
 * incomplete log record
 */
bool LogRecovery::DeserializeLogRecord(const char *data, LogRecord *log_record) {
  // LogRecord 反序列化
  if (data + LogRecord::HEADER_SIZE > log_buffer_ + LOG_BUFFER_SIZE) {
    return false;
  }
  memcpy(log_record, data, LogRecord::HEADER_SIZE);  // 拷贝头部数据
  // 如果 log_record 大小 <= 0 或者
  if (log_record->size_ <= 0 || data + log_record->size_ > log_buffer_ + LOG_BUFFER_SIZE) {
    return false;
  }
  data += LogRecord::HEADER_SIZE;
  switch (log_record->log_record_type_) {
    case LogRecordType::INSERT:
      log_record->insert_rid_ = *reinterpret_cast<const RID *>(data);
      // 已经包含了处理 tuple_size
      log_record->insert_tuple_.DeserializeFrom(data + sizeof(RID));
      break;
    case LogRecordType::MARKDELETE:
    case LogRecordType::APPLYDELETE:
    case LogRecordType::ROLLBACKDELETE:
      log_record->delete_rid_ = *reinterpret_cast<const RID *>(data);
      log_record->delete_tuple_.DeserializeFrom(data + sizeof(RID));
      break;
    case LogRecordType::UPDATE:
      log_record->update_rid_ = *reinterpret_cast<const RID *>(data);
      log_record->old_tuple_.DeserializeFrom(data + sizeof(RID));
      log_record->new_tuple_.DeserializeFrom(data + sizeof(RID) + sizeof(int32_t) + log_record->old_tuple_.GetLength());
      break;
    case LogRecordType::BEGIN:
    case LogRecordType::COMMIT:
    case LogRecordType::ABORT:
      break;
    case LogRecordType::NEWPAGE:
      log_record->prev_page_id_ = *reinterpret_cast<const page_id_t *>(data);
      log_record->page_id_ = *reinterpret_cast<const page_id_t *>(data + sizeof(page_id_t));
      break;
    default:
      assert(false);
  }
  return true;
}

/*
 * redo phase on TABLE PAGE level(table/table_page.h)
 * read log file from the beginning to end (you must prefetch log records into
 * log buffer to reduce unnecessary I/O operations), remember to compare page's
 * LSN with log_record's sequence number, and also build active_txn_ table &
 * lsn_mapping_ table
 * 重做，对缓冲区中的日志记录进行重做
 * 这里的 Redo 有些暴力，从头到尾的读取日志文件，然后记录 lsn_mapping_ 和 active_txn_
 */
void LogRecovery::Redo() {
  assert(enable_logging == false);
  // 从头开始读到结尾
  offset_ = 0;
  int buffer_offset = 0;
  // 从磁盘中读取日志数据到 log_buffer 中
  while (disk_manager_->ReadLog(log_buffer_ + buffer_offset, LOG_BUFFER_SIZE - buffer_offset, offset_)) {
    int buffer_start = offset_;                    // 开始偏移
    offset_ += (LOG_BUFFER_SIZE - buffer_offset);  // 增加偏移
    buffer_offset = 0;                             // 重制缓冲区
    LogRecord log;
    while (DeserializeLogRecord(log_buffer_ + buffer_offset, &log)) {
      lsn_mapping_[log.GetLSN()] = buffer_start + buffer_offset;  // 更新 lsn_mapping
      active_txn_[log.txn_id_] = log.lsn_;                        // 更新 active_txn
      buffer_offset += log.size_;                                 // 更新 buffer_offset
      // 事务开始无需操作
      if (log.log_record_type_ == LogRecordType::BEGIN) {
        continue;
      }
      // 事务提交、终止无需操作
      if (log.log_record_type_ == LogRecordType::COMMIT || log.log_record_type_ == LogRecordType::ABORT) {
        // 如果事务已经提交、或者已经终止了，那么就从 active_txn_ 中删除
        // active_txn 中只记录活跃的事务
        assert(active_txn_.erase(log.GetTxnId()) > 0);  // 从 active_txn_ 中删除 lsn
        continue;
      }
      // 新页
      if (log.log_record_type_ == LogRecordType::NEWPAGE) {
        // 获取新页
        auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(log.page_id_));
        assert(page != nullptr);
        bool need_redo = log.lsn_ > page->GetLSN();  // 判断 lsn 是否需要重做
        if (need_redo) {                             // redo
          // 初始化 page
          page->Init(log.page_id_, PAGE_SIZE, log.prev_page_id_, nullptr, nullptr);
          // 设置 lsn
          page->SetLSN(log.lsn_);
          // 如果有前页
          if (log.prev_page_id_ != INVALID_PAGE_ID) {
            auto prev_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(log.prev_page_id_));
            assert(prev_page != nullptr);
            bool needChange =
                prev_page->GetNextPageId() == log.page_id_;  // 判断前页的 next_page 是否为 page，是否需要改变
            prev_page->SetNextPageId(log.page_id_);          // 设置 next_page_id
            buffer_pool_manager_->UnpinPage(prev_page->GetPageId(), needChange);
          }
        }
        buffer_pool_manager_->UnpinPage(page->GetPageId(), need_redo);
        continue;
      }
      // 插入、更新、删除
      RID rid = log.log_record_type_ == LogRecordType::INSERT   ? log.insert_rid_
                : log.log_record_type_ == LogRecordType::UPDATE ? log.update_rid_
                                                                : log.delete_rid_;
      auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
      assert(page != nullptr);
      bool need_redo = log.lsn_ > page->GetLSN();  // lsn 记录了序号，lsn 必须大于页的 lsn 才能 redo
      if (need_redo) {
        if (log.log_record_type_ == LogRecordType::INSERT) {
          page->InsertTuple(log.insert_tuple_, &rid, nullptr, nullptr, nullptr);
        } else if (log.log_record_type_ == LogRecordType::UPDATE) {
          page->UpdateTuple(log.new_tuple_, &log.old_tuple_, rid, nullptr, nullptr, nullptr);
        } else if (log.log_record_type_ == LogRecordType::MARKDELETE) {
          page->MarkDelete(rid, nullptr, nullptr, nullptr);
        } else if (log.log_record_type_ == LogRecordType::APPLYDELETE) {
          page->ApplyDelete(rid, nullptr, nullptr);
        } else if (log.log_record_type_ == LogRecordType::ROLLBACKDELETE) {
          page->RollbackDelete(rid, nullptr, nullptr);
        } else {
          assert(false);  // 非法
        }
        page->SetLSN(log.lsn_);  // redo 后更新 lsn
      }
      // 记得 unpin
      buffer_pool_manager_->UnpinPage(page->GetPageId(), need_redo);
    }
    // 移动 log_buffer_ + buffer_offset 到 log_buffer_
    memmove(log_buffer_, log_buffer_ + buffer_offset, LOG_BUFFER_SIZE - buffer_offset);
    buffer_offset = LOG_BUFFER_SIZE - buffer_offset;  // 更新 buffer_offset
  }
}

/*
 * undo phase on TABLE PAGE level(table/table_page.h)
 * iterate through active txn map and undo each operation
 * 撤销，对缓冲区的日志执行撤销
 */
void LogRecovery::Undo() {
  assert(enable_logging == false);
  for (auto &txn : active_txn_) {
    lsn_t lsn = txn.second;  // 得到 lsn
    while (lsn != INVALID_LSN) {
      LogRecord log;
      // 读取日志数据
      disk_manager_->ReadLog(log_buffer_, PAGE_SIZE, lsn_mapping_[lsn]);
      assert(DeserializeLogRecord(log_buffer_, &log));  // 序列化日志记录
      assert(log.lsn_ == lsn);
      lsn = log.prev_lsn_;  // 得到上一个 lsn，为什么是上一个？因为是 undo，所以得反向执行
      if (log.log_record_type_ == LogRecordType::BEGIN) {  // 事务开始
        assert(log.prev_lsn_ == INVALID_LSN);              // 上一个 lsn 应该是 invalid
        continue;
      }
      // 事务提交、终止，不应该出现
      if (log.log_record_type_ == LogRecordType::COMMIT || log.log_record_type_ == LogRecordType::ABORT) {
        assert(false);
      }
      // 剩下的全部反操作
      if (log.log_record_type_ == LogRecordType::NEWPAGE) {     // 如果是 new page
        if (!buffer_pool_manager_->DeletePage(log.page_id_)) {  // 那么删除 page(反向执行)
          disk_manager_->DeallocatePage(log.page_id_);
        }
        if (log.prev_page_id_ != INVALID_PAGE_ID) {  // 上一页的 next_page 得重置
          auto prev_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(log.prev_page_id_));
          assert(prev_page != nullptr);
          assert(prev_page->GetNextPageId() == log.page_id_);
          prev_page->SetNextPageId(INVALID_PAGE_ID);
          buffer_pool_manager_->UnpinPage(prev_page->GetPageId(), true);
        }
        continue;
      }
      // 插入、更新、删除
      RID rid = log.log_record_type_ == LogRecordType::INSERT   ? log.insert_rid_
                : log.log_record_type_ == LogRecordType::UPDATE ? log.update_rid_
                                                                : log.delete_rid_;
      auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));  // 获取页
      assert(page != nullptr);
      assert(page->GetLSN() >= log.lsn_);
      if (log.log_record_type_ == LogRecordType::INSERT) {
        // insert，反向执行 apply delete
        page->ApplyDelete(log.insert_rid_, nullptr, nullptr);
      } else if (log.log_record_type_ == LogRecordType::UPDATE) {
        // update 交换 old,new 执行
        Tuple tuple;
        page->UpdateTuple(log.old_tuple_, &tuple, log.update_rid_, nullptr, nullptr, nullptr);
        assert(tuple.GetLength() == log.new_tuple_.GetLength() &&
               memcmp(tuple.GetData(), log.new_tuple_.GetData(), tuple.GetLength()) == 0);
      } else if (log.log_record_type_ == LogRecordType::MARKDELETE) {
        // mark_delete 反向执行 roll back delete
        page->RollbackDelete(log.delete_rid_, nullptr, nullptr);
      } else if (log.log_record_type_ == LogRecordType::APPLYDELETE) {
        // apply delete 反向执行 insert
        page->InsertTuple(log.delete_tuple_, &log.delete_rid_, nullptr, nullptr, nullptr);
      } else if (log.log_record_type_ == LogRecordType::ROLLBACKDELETE) {
        // rollback delete 反向执行 mark delete
        page->MarkDelete(log.delete_rid_, nullptr, nullptr, nullptr);
      } else {
        assert(false);
      }
      // 记得 unpin
      buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    }
  }
  // 清空
  active_txn_.clear();
  lsn_mapping_.clear();
}

}  // namespace bustub
