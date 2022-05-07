//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include "common/logger.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  std::lock_guard<std::mutex> lock(latch_);
  auto p = page_table_.find(page_id);
  if (p != page_table_.end()) {
    // 找到了 page
    auto frame_id = p->second;
    replacer_->Pin(frame_id);
    auto page = &pages_[frame_id];
    page->pin_count_++;
    return page;
  }
  // 未找到 Page，则从 free list 和 replacer 中查找 R
  frame_id_t free_frame_id;
  if (!free_list_.empty()) {
    free_frame_id = free_list_.front();
    free_list_.pop_front();  // 删除最前面的
  } else {
    // 如果 free_list 中没有则淘汰一个页
    bool ok = replacer_->Victim(&free_frame_id);
    if (!ok) {
      return nullptr;
    }
  }
  auto page = &pages_[free_frame_id];
  // 如果 free_frame_id 的页是脏的，则刷至磁盘
  // 注意：刷的是 free_frame_id 关联的 page_id
  if (page->IsDirty()) {
    // 写日志
    if (enable_logging && log_manager_->GetPersistentLSN() < page->GetLSN()) {
      log_manager_->Flush(true);
    }
    disk_manager_->WritePage(page->GetPageId(), page->GetData());
    page->is_dirty_ = false;
  }
  // 从 page_table 中删除 R，然后插入 P
  page_table_.erase(page->GetPageId());
  page_table_[page_id] = free_frame_id;
  // 更新 P 的元数据
  page->page_id_ = page_id;
  page->pin_count_ = 1;
  page->is_dirty_ = false;
  page->ResetMemory();  // 重置内存
  // LOG_ERROR("fetch page: %d, frame_id: %d .", page_id, free_frame_id);
  disk_manager_->ReadPage(page_id, page->GetData());  // 重新从磁盘中读取数据
  return page;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  std::lock_guard<std::mutex> lock(latch_);
  auto p = page_table_.find(page_id);
  if (p == page_table_.end()) {
    return false;
  }
  auto frame_id = p->second;
  auto page = &pages_[frame_id];
  if (page->pin_count_ < 0) {
    return false;
  }
  if (page->pin_count_ > 0) {
    page->pin_count_--;
  }
  if (page->pin_count_ == 0) {
    replacer_->Unpin(frame_id);
  }
  if (is_dirty) {
    page->is_dirty_ = is_dirty;
  }
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  std::lock_guard<std::mutex> lock(latch_);
  auto p = page_table_.find(page_id);
  if (p == page_table_.end()) {
    return false;
  }
  auto frame_id = p->second;
  auto page = &pages_[frame_id];
  if (page->IsDirty()) {
    // 写日志
    if (enable_logging && log_manager_->GetPersistentLSN() < page->GetLSN()) {
      log_manager_->Flush(true);
    }
    disk_manager_->WritePage(page_id, page->GetData());
    page->is_dirty_ = false;
  }
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::lock_guard<std::mutex> lock(latch_);
  *page_id = disk_manager_->AllocatePage();
  // 寻找新的 frame_id
  frame_id_t free_frame_id;
  if (!free_list_.empty()) {
    free_frame_id = free_list_.front();
    free_list_.pop_front();  // 删除最前面的
  } else {
    // 如果 free_list 中没有则淘汰一个页
    bool ok = replacer_->Victim(&free_frame_id);
    if (!ok) {
      return nullptr;
    }
  }
  auto page = &pages_[free_frame_id];
  // 如果 free_frame_id 的页是脏的，则刷至磁盘
  if (page->IsDirty()) {
    // 写日志
    if (enable_logging && log_manager_->GetPersistentLSN() < page->GetLSN()) {
      log_manager_->Flush(true);
    }
    // 写到 page
    disk_manager_->WritePage(page->GetPageId(), page->GetData());
    page->is_dirty_ = false;
  }
  // 从 page_table 中删除 R，然后插入 P
  page_table_.erase(page->GetPageId());
  page_table_[*page_id] = free_frame_id;  // 更新
  // 更新 P 的元数据
  page->ResetMemory();  // 重置内存
  page->page_id_ = *page_id;
  page->pin_count_ = 0;
  page->is_dirty_ = false;
  return &pages_[free_frame_id];
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::lock_guard<std::mutex> lock(latch_);
  // LOG_DEBUG("attempt to delete page: %d .", page_id);
  auto p = page_table_.find(page_id);
  if (p == page_table_.end()) {
    return true;
  }
  auto frame_id = p->second;
  auto page = &pages_[frame_id];
  if (page->pin_count_ > 0) {
    LOG_ERROR("Delete page but pin count > 0.");
    return false;
  }
  if (page->IsDirty()) {
    // 写日志
    if (enable_logging && log_manager_->GetPersistentLSN() < page->GetLSN()) {
      log_manager_->Flush(true);
    }
    disk_manager_->WritePage(page->GetPageId(), page->GetData());
    page->is_dirty_ = false;
  }
  // 元数据重置
  page->pin_count_ = 0;
  page->is_dirty_ = false;
  page->ResetMemory();
  page->page_id_ = INVALID_PAGE_ID;
  // 删除
  page_table_.erase(page_id);
  free_list_.push_back(frame_id);
  disk_manager_->DeallocatePage(page_id);
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  std::lock_guard<std::mutex> lock(latch_);
  for (size_t i = 0; i < pool_size_; i++) {
    auto page_id = static_cast<page_id_t>(i);
    auto frame_id = page_table_[page_id];
    auto page = &pages_[frame_id];
    if (page->IsDirty()) {
      // 写日志
      if (enable_logging && log_manager_->GetPersistentLSN() < page->GetLSN()) {
        log_manager_->Flush(true);
      }
      disk_manager_->WritePage(page_id, page->GetData());
      page->is_dirty_ = false;
    }
  }
}

}  // namespace bustub
