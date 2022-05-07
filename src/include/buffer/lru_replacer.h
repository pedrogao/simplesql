//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.h
//
// Identification: src/include/buffer/lru_replacer.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <iostream>
#include <list>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"

namespace bustub {
/**
 * LRUReplacer implements the lru replacement policy, which approximates the Least Recently Used policy.
 * However, not all the frames are considered as in the LRUReplacer.
 * The LRUReplacer is initialized to have no frame in it.
 * Then, only the newly unpinned ones will be considered in the LRUReplacer.
 *
 * 然后，并不是所有的 frames 都在 LRUReplacer 中；
 * LRUReplacer 被初始化后，不会包含任何 frame；
 * 只有新的 unpinned frame 才将被考虑加入到 LRUReplacer 中。
 */
class LRUReplacer : public Replacer {
 public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  /**
   * Remove the object that was accessed the least recently compared to all the elements being tracked by the Replacer.
   * Replace 负责记录页的访问情况，Victim 会删除最近最少使用的页，并将被删除页的 id 赋给 frame_id，然后返回 true
   */
  bool Victim(frame_id_t *frame_id) override;

  /*
   * BufferPoolManager pin 一个 page 为 frame 后调用 Pin.
   * Pin 应该删除一个包含了 page 的 frame
   *
   * Pin 也会删除一个 frame，不同的是 Victim 会按照 LRU 策略来删除，而 Pin 直接删除 frame_id 的 frame
   */
  void Pin(frame_id_t frame_id) override;

  /*
   * 当 page 的 pin_count 变成 0 时，Unpin 应该被调用。
   * Unpin 应该将包含 unpinned page 的 frame 加入到 LRUReplacer 中。
   *
   * 将 frame 加入到 LRU 中，frame 已经可以被淘汰了
   */
  void Unpin(frame_id_t frame_id) override;

  /*
   * 返回 LRUReplacer 当前的 frames 的个数
   */
  size_t Size() override;

  void print_list();

  void print_map();

 private:
  // TODO(student): implement me!
  size_t num_pages;
  std::list<frame_id_t> _list;
  std::unordered_map<frame_id_t, std::list<frame_id_t>::iterator> _map;

  // 锁
  std::mutex mu;
};

}  // namespace bustub
