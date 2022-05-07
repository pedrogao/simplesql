//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/b_plus_tree.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <queue>
#include <string>
#include <utility>
#include <vector>

#include "concurrency/transaction.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

// 并发操作B树操作类型
enum class OperationType { SEARCH, INSERT, DELETE };

/**
 * Main class providing the API for the Interactive B+ Tree.
 * B+树
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key  只支持唯一主键
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;  // 内部节点
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;          // 叶子节点

 public:
  explicit BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                     int leaf_max_size = LEAF_PAGE_SIZE, int internal_max_size = INTERNAL_PAGE_SIZE);

  // Returns true if this B+ tree has no keys and values.
  bool IsEmpty() const;

  // Insert a key-value pair into this B+ tree.
  bool Insert(const KeyType &key, const ValueType &value, Transaction *transaction = nullptr);

  // Remove a key and its value from this B+ tree.
  void Remove(const KeyType &key, Transaction *transaction = nullptr);

  // return the value associated with a given key
  bool GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction = nullptr);

  // index iterator
  INDEXITERATOR_TYPE begin();
  INDEXITERATOR_TYPE Begin(const KeyType &key);
  INDEXITERATOR_TYPE end();

  void Print(BufferPoolManager *bpm) {
    ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
  }

  void Draw(BufferPoolManager *bpm, const std::string &outf) {
    std::ofstream out(outf);
    out << "digraph G {" << std::endl;
    ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
    out << "}" << std::endl;
    out.close();
  }

  // read data from file and insert one by one
  void InsertFromFile(const std::string &file_name, Transaction *transaction = nullptr);

  // read data from file and remove one by one
  void RemoveFromFile(const std::string &file_name, Transaction *transaction = nullptr);
  // expose for test purpose
  Page *FindLeafPage(const KeyType &key, bool leftMost = false);

  // int isBalanced(page_id_t pid);
  // bool isPageCorr(page_id_t pid, std::pair<KeyType, KeyType> &out);
  // bool Check(bool forceCheck);

 private:
  std::pair<Page *, bool> FindLeafPageByOperation(const KeyType &key, bool leftMost = false, bool rightMost = false,
                                                  OperationType op_type = OperationType::SEARCH,
                                                  Transaction *transaction = nullptr);

  void ClearTransactionPageSet(Transaction *transaction);

  void ClearTransactionPageSetAndUnpin(Transaction *transaction);

  void StartNewTree(const KeyType &key, const ValueType &value);

  bool InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction = nullptr);

  void InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                        Transaction *transaction = nullptr, bool is_root_page_id_locked = false);

  template <typename N>
  N *Split(N *node);

  template <typename N>
  bool CoalesceOrRedistribute(N *node, Transaction *transaction = nullptr, bool is_root_page_id_locked = false);

  template <typename N>
  bool Coalesce(N **neighbor_node, N **node, BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent,
                int index, Transaction *transaction = nullptr, bool is_root_page_id_locked = false);

  template <typename N>
  void Redistribute(N *neighbor_node, N *node, BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent,
                    int index, bool is_root_page_id_locked);

  bool AdjustRoot(BPlusTreePage *node, bool is_root_page_id_locked);

  void UpdateRootPageId(int insert_record = 0);

  bool IsSafety(BPlusTreePage *node, OperationType op_type);

  /* Debug Routines for FREE!! */
  void ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const;

  void ToString(BPlusTreePage *page, BufferPoolManager *bpm) const;

  // member variable
  std::string index_name_;                  // 索引名称
  std::mutex root_page_id_mutex_;           // 根节点 id 锁
  page_id_t root_page_id_;                  // 根节点 id，需要加锁保护
  BufferPoolManager *buffer_pool_manager_;  // 缓存池
  KeyComparator comparator_;                // 比较器
  int leaf_max_size_;                       // 叶子节点最大项数量
  int internal_max_size_;                   // 内部节点最大项数量
};

}  // namespace bustub
