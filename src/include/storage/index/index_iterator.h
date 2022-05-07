//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {  // 索引迭代器
 public:
  // you may define your own constructor based on your member variables
  IndexIterator(BufferPoolManager *bpm, Page *page, int idx);
  ~IndexIterator();

  bool isEnd();

  const MappingType &operator*();

  IndexIterator &operator++();

  bool operator==(const IndexIterator &itr) const;

  bool operator!=(const IndexIterator &itr) const;

 private:
  // add your own private member variables here
  BufferPoolManager *buffer_pool_manager_;
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf{nullptr};
  Page *page_{nullptr};
  int idx_{0};
};

}  // namespace bustub
