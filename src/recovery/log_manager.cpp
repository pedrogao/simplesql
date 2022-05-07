//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// log_manager.cpp
//
// Identification: src/recovery/log_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "recovery/log_manager.h"
#include "common/logger.h"

namespace bustub {
/*
 * set enable_logging = true
 * Start a separate thread to execute flush to disk operation periodically
 * The flush can be triggered when timeout or the log buffer is full or buffer
 * pool manager wants to force flush (it only happens when the flushed page has
 * a larger LSN than persistent LSN)
 *
 * This thread runs forever until system shutdown/StopFlushThread
 */
void LogManager::RunFlushThread() {
  // 如果已经开启，那么直接返回
  if (enable_logging) {
    return;
  }
  enable_logging = true;
  // 新建刷新线程
  flush_thread_ = new std::thread([&] {
    while (enable_logging) {
      std::unique_lock<std::mutex> latch(latch_);  // 初始化锁
      // 当前线程等待，直到超时，或者 need_flush 为 true
      cv_.wait_for(latch, log_timeout, [&] { return need_flush_.load(); });
      assert(flush_buffer_size_ == 0);
      // 如果 log_buffer 偏移 > 0
      if (log_buffer_offset_ > 0) {
        // 注意：
        // log_buffer 用于装日志数据，待需要刷磁盘时，和 flush_buffer 交换，然后将 flush_buffer 刷到磁盘中
        //
        // 交换 log_buffer 和 flush_buffer
        std::swap(log_buffer_, flush_buffer_);
        // 交换 log_buffer_offset_ 与 flush_buffer_size_
        // log_buffer 的偏移就是 flush_buffer 的大小
        std::swap(log_buffer_offset_, flush_buffer_size_);
        // 写日志
        disk_manager_->WriteLog(flush_buffer_, flush_buffer_size_);
        flush_buffer_size_ = 0;       // flush_buffer 大小为 0
        SetPersistentLSN(last_lsn_);  // 设置持久化日志序号
      }
      need_flush_ = false;      // flush 完毕
      append_cv_.notify_all();  // 通知追加线程
    }
  });
}

/*
 * Stop and join the flush thread, set enable_logging = false
 */
void LogManager::StopFlushThread() {
  if (!enable_logging) {
    return;
  }
  enable_logging = false;
  Flush(true);            // 强制刷新磁盘
  flush_thread_->join();  // 刷新线程 join
  assert(log_buffer_offset_ == 0 && flush_buffer_size_ == 0);
  delete flush_thread_;  // 删除刷新线程
}

/*
 * append a log record into log buffer
 * you MUST set the log record's lsn within this method
 * @return: lsn that is assigned to this log record
 *
 *
 * example below
 * // First, serialize the must have fields(20 bytes in total)
 * log_record.lsn_ = next_lsn_++;
 * memcpy(log_buffer_ + offset_, &log_record, 20);
 * int pos = offset_ + 20;
 *
 * if (log_record.log_record_type_ == LogRecordType::INSERT) {
 *    memcpy(log_buffer_ + pos, &log_record.insert_rid_, sizeof(RID));
 *    pos += sizeof(RID);
 *    // we have provided serialize function for tuple class
 *    log_record.insert_tuple_.SerializeTo(log_buffer_ + pos);
 *  }
 *
 */
lsn_t LogManager::AppendLogRecord(LogRecord *log_record) {
  std::unique_lock<std::mutex> latch(latch_);  // 加锁
  // 如果 log_buffer 偏移 + 日志记录大小 >= 日志缓冲区大小
  // 表示添加当前日志记录后，需要刷日志到磁盘
  if (log_buffer_offset_ + log_record->GetSize() >= LOG_BUFFER_SIZE) {
    need_flush_ = true;  // 需要刷日志
    cv_.notify_one();    // 通知一个线程刷日志
    // 当前追加日志线程等待，等待 log_buffer 能够容纳这次日志记录
    append_cv_.wait(latch, [&] { return log_buffer_offset_ + log_record->GetSize() < LOG_BUFFER_SIZE; });
  }
  log_record->lsn_ = next_lsn_++;  // 日志记录 lsn
  // 将日志记录 header 拷贝到 log_buffer 中
  memcpy(log_buffer_ + log_buffer_offset_, log_record, LogRecord::HEADER_SIZE);
  // 拷贝后的 log_buffer 偏移
  int pos = log_buffer_offset_ + LogRecord::HEADER_SIZE;
  // 如果是插入记录
  if (log_record->log_record_type_ == LogRecordType::INSERT) {
    // 拷贝 tuple_rid
    memcpy(log_buffer_ + pos, &log_record->insert_rid_, sizeof(RID));
    pos += sizeof(RID);
    // 拷贝 tuple，即 tuple_size + tuple_data
    log_record->insert_tuple_.SerializeTo(log_buffer_ + pos);
  } else if (log_record->log_record_type_ == LogRecordType::MARKDELETE ||
             log_record->log_record_type_ == LogRecordType::APPLYDELETE ||
             log_record->log_record_type_ == LogRecordType::ROLLBACKDELETE) {
    // mark_delete apply_delete rollback_delete 日志数据都是一样的
    memcpy(log_buffer_ + pos, &log_record->delete_rid_, sizeof(RID));
    pos += sizeof(RID);
    log_record->delete_tuple_.SerializeTo(log_buffer_ + pos);
  } else if (log_record->log_record_type_ == LogRecordType::UPDATE) {
    // update
    memcpy(log_buffer_ + pos, &log_record->update_rid_, sizeof(RID));
    pos += sizeof(RID);
    // 旧数据
    log_record->old_tuple_.SerializeTo(log_buffer_ + pos);
    // pos 更新
    pos += (log_record->old_tuple_.GetLength() + sizeof(int32_t));
    // 新数据
    log_record->new_tuple_.SerializeTo(log_buffer_ + pos);
  } else if (log_record->log_record_type_ == LogRecordType::NEWPAGE) {
    // new page
    memcpy(log_buffer_ + pos, &log_record->prev_page_id_, sizeof(page_id_t));
    pos += sizeof(page_id_t);
    memcpy(log_buffer_ + pos, &log_record->page_id_, sizeof(page_id_t));
  }
  log_buffer_offset_ += log_record->GetSize();  // 更新 log_buffer_offset_，包括 header_size
  last_lsn_ = log_record->lsn_;
  return last_lsn_;
}

void LogManager::Flush(bool force) {
  std::unique_lock<std::mutex> latch(latch_);  // 加锁
  if (force) {                                 // 是否强制刷磁盘
    need_flush_ = true;
    cv_.notify_one();  // 通知一个线程刷磁盘
    if (enable_logging) {
      // append_cv_ 用来阻塞追加线程，等待刷磁盘完成后解锁
      append_cv_.wait(latch, [&] { return !need_flush_.load(); });
    } else {
      // 等待，知道 append_cv 被磁盘刷完后唤醒
      append_cv_.wait(latch);
    }
  }
}

}  // namespace bustub
