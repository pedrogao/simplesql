//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"
#include "common/logger.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) { this->num_pages = num_pages; }

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> lock(mu);
  // 找到最近最少使用的 frame，并且删除
  if (this->_map.empty()) {
    return false;
  }
  // 删除头部节点
  *frame_id = _list.back();
  _map.erase(*frame_id);
  _list.pop_back();
  // LOG_INFO("# Victim frame_id: %d", *frame_id);
  //   print_list();
  //   print_map();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(mu);
  // LOG_INFO("# Pin frame_id: %d", frame_id);
  if (_map.find(frame_id) == _map.end()) {
    return;  // 没有则直接返回
  }
  std::list<frame_id_t>::iterator it = _map[frame_id];
  // 删除这个节点
  _list.erase(it);
  _map.erase(frame_id);
  //   print_list();
  //   print_map();
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(mu);
  // LOG_INFO("# Unpin frame_id: %d", frame_id);
  // 如果超过了最大数量，或者已经包含了 frame_id，则直接返回
  if (_map.size() >= num_pages || _map.find(frame_id) != _map.end()) {
    return;
  }
  // 将 frame 加入到 LRU 中
  _list.push_front(frame_id);
  std::list<frame_id_t>::iterator begin = _list.begin();  // begin 节点不会改变，而 end 会随着链表增长而改变
  _map[frame_id] = begin;
  //   print_list();
  //   print_map();
}

size_t LRUReplacer::Size() {
  std::lock_guard<std::mutex> lock(mu);
  size_t sz = this->_map.size();
  // LOG_INFO("# Size: %zu", sz);
  return sz;
}

void LRUReplacer::print_list() {
  // Print out the list
  std::cout << "l = { ";
  for (int n : _list) {
    std::cout << n << ", ";
  }
  std::cout << "};\n";
}

void LRUReplacer::print_map() {
  for (const auto &[key, value] : this->_map) {
    std::cout << "Key:[" << key << "] Value:[" << *value << "]\n";
  }
}

}  // namespace bustub
