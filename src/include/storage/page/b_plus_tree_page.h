//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_page.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <cassert>
#include <climits>
#include <cstdlib>
#include <string>

#include "buffer/buffer_pool_manager.h"
#include "storage/index/generic_key.h"

namespace bustub {

// k-v 类型
#define MappingType std::pair<KeyType, ValueType>

// k-v，k-comparator类型，如何比较 k
#define INDEX_TEMPLATE_ARGUMENTS template <typename KeyType, typename ValueType, typename KeyComparator>

// define page type enum
enum class IndexPageType { INVALID_INDEX_PAGE = 0, LEAF_PAGE, INTERNAL_PAGE };

/**
 * Both internal and leaf page are inherited from this page.
 *
 * It actually serves as a header part for each B+ tree page and
 * contains information shared by both leaf page and internal page.
 *
 * B树的阶指的是字节点个数
 *
 * Header format (size in byte, 24 bytes in total):
 * ----------------------------------------------------------------------------
 * | PageType (4) | LSN (4) | CurrentSize (4) | MaxSize (4) |
 * ----------------------------------------------------------------------------
 * | ParentPageId (4) | PageId(4) |
 * ----------------------------------------------------------------------------
 */
class BPlusTreePage {
 public:
  bool IsLeafPage() const;
  bool IsRootPage() const;
  void SetPageType(IndexPageType page_type);
  IndexPageType GetPageType();

  int GetSize() const;
  void SetSize(int size);
  void IncreaseSize(int amount);

  int GetMaxSize() const;
  void SetMaxSize(int max_size);
  int GetMinSize() const;

  page_id_t GetParentPageId() const;
  void SetParentPageId(page_id_t parent_page_id);

  page_id_t GetPageId() const;
  void SetPageId(page_id_t page_id);

  void SetLSN(lsn_t lsn = INVALID_LSN);

 private:
  // member variable, attributes that both internal and leaf page share
  IndexPageType page_type_ __attribute__((__unused__));   // 页类型，是内部节点，还是叶子节点
  lsn_t lsn_ __attribute__((__unused__));                 // log sequence number
  int size_ __attribute__((__unused__));                  // 页中 k-v对的数量
  int max_size_ __attribute__((__unused__));              // k-v 对的最大数量
  page_id_t parent_page_id_ __attribute__((__unused__));  // 父节点 id
  page_id_t page_id_ __attribute__((__unused__));         // 节点 id
};

}  // namespace bustub
