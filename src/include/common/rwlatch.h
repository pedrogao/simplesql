//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// rwmutex.h
//
// Identification: src/include/common/rwlatch.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <climits>
#include <condition_variable>  // NOLINT
#include <mutex>               // NOLINT

#include "common/macros.h"

namespace bustub {

/**
 * Reader-Writer latch backed by std::mutex.  读写锁
 */
class ReaderWriterLatch {
  using mutex_t = std::mutex;                    // 锁
  using cond_t = std::condition_variable;        // 条件变量
  static const uint32_t MAX_READERS = UINT_MAX;  // reader 最大个数

 public:
  ReaderWriterLatch() = default;
  ~ReaderWriterLatch() { std::lock_guard<mutex_t> guard(mutex_); }

  DISALLOW_COPY(ReaderWriterLatch);  // 禁止拷贝

  /**
   * Acquire a write latch. 获取写锁
   */
  void WLock() {
    std::unique_lock<mutex_t> latch(mutex_);
    while (writer_entered_) {  // 如果有其他人准备写，那么一直等待
      reader_.wait(latch);
    }
    writer_entered_ = true;      // 获取到了写锁，那么设置 writer_entered_ 为 true
    while (reader_count_ > 0) {  // 如果仍然有 reader 正在读，那么一直等待
      writer_.wait(latch);
    }
  }

  /**
   * Release a write latch. 释放写锁
   */
  void WUnlock() {
    std::lock_guard<mutex_t> guard(mutex_);
    writer_entered_ = false;  // 置为 false
    reader_.notify_all();     // 通知所有 reader
  }

  /**
   * Acquire a read latch. 获取读锁
   */
  void RLock() {
    std::unique_lock<mutex_t> latch(mutex_);  // 在函数声明周期内加锁
    while (writer_entered_ || reader_count_ == MAX_READERS) {
      reader_.wait(latch);  // 如果有人准备写，或者已达最大数量读，那么等待
    }
    reader_count_++;  // 获得了读锁，count++
  }

  /**
   * Release a read latch. 释放读锁
   */
  void RUnlock() {
    std::lock_guard<mutex_t> guard(mutex_);
    reader_count_--;             // count --
    if (writer_entered_) {       // 已经有人准备写
      if (reader_count_ == 0) {  // 如果没有人读了，那么通知一个 writer
        writer_.notify_one();
      }
    } else {
      if (reader_count_ == MAX_READERS - 1) {  // 如果 reader 的数量尚未达到最大值，那么通知一个阻塞的 reader
        reader_.notify_one();
      }
    }
  }

 private:
  mutex_t mutex_;               // 内部锁
  cond_t writer_;               // 可写变量
  cond_t reader_;               // 可读变量
  uint32_t reader_count_{0};    // reader 个数
  bool writer_entered_{false};  // 是否正在被写
};

}  // namespace bustub
