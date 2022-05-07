//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <iostream>
#include <iterator>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);  // 内部节点
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  // if (index < 0 || index >= GetSize()) {
  //   return {};
  // }
  KeyType key = this->array[index].first;
  return key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  // if (index < 0 || index >= GetSize()) {
  //   return;
  // }
  this->array[index].first = key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
  // if (index < 0 || index >= GetSize()) {
  //   return;
  // }
  this->array[index].second = value;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  int sz = GetSize();
  // key 是有序的，但 value 不是，所以只能 O(n) 寻找了
  auto p = std::find_if(array, array + sz, [&value](const auto &pair) { return pair.second == value; });
  return std::distance(array, p);
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const {
  // assert(index >= 0);
  // if (GetSize() > 0) {
  //   assert(index < GetSize());
  // }

  return array[index].second;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
  // 二分查找
  // <= 1 还怎么找
  int sz = GetSize();
  assert(sz >= 1);
  int st = 1;
  int ed = sz - 1;
  while (st <= ed) {  // find the last key in array <= input
    int mid = (ed - st) / 2 + st;
    if (comparator(array[mid].first, key) <= 0) {
      st = mid + 1;
    } else {
      ed = mid - 1;
    }
  }
  return array[st - 1].second;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 *
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
  // 当前内部节点成为根节点
  // 第一个 key 是 invalid，old_value 作为左边孩子
  // 第二个 key 是 new_key，new_value 作为右孩子
  array[0].second = old_value;
  array[1].first = new_key;
  array[1].second = new_value;
  SetSize(2);  // 两个孩子，所以 size = 2
}
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value) {
  // 在 old_value 所在序号的后面插入一个元素
  auto new_index = ValueIndex(old_value) + 1;
  int sz = GetSize();
  // 将数组 [new_index,array) 中的数据向后拷贝 1 位
  std::move_backward(array + new_index, array + sz, array + sz + 1);
  array[new_index].first = new_key;
  array[new_index].second = new_value;
  IncreaseSize(1);  // 多了一个孩子
  return sz + 1;
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) {
  // 当前页分裂，一半数据移动到 recipient 中
  auto start_idx = GetMinSize();
  // 拷贝[start_idx, size)
  recipient->CopyNFrom(array + start_idx, GetSize() - start_idx, buffer_pool_manager);
  SetSize(start_idx);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  // 将 items 拷贝到 array 中
  int sz = GetSize();
  // copy(src_start, src_end, target_start)
  // [src_start, src_end) => target_start
  std::copy(items, items + size, array + sz);
  IncreaseSize(size);
  // 已经拷进去了，所以可以通过 ValueAt 直接获取 page_id
  // 内部节点拷贝后，孩子节点的父节点发生了变化，因此需要更新 parent_id
  auto pid = GetPageId();
  for (int i = 0; i < size; i++) {
    auto page_id = ValueAt(i + sz);  // 获得子节点对应的 page_id
    auto page = buffer_pool_manager->FetchPage(page_id);
    BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(page);
    node->SetParentPageId(pid);                     // 更新为当前的 page_id
    buffer_pool_manager->UnpinPage(page_id, true);  // 并且 unpin
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  int sz = GetSize();
  // 注意：move_backward 是向后移动，而 move 是向前移动
  // 向前移动，将 index+1 移动到 index，那么就相当于删除了 index
  std::move(array + index + 1, array + sz, array + index);
  IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  // 移除和返回最后一个 value
  SetSize(0);
  return ValueAt(0);
}
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager) {
  // 将当前页内的所有 pairs 移动到 recipient 中
  // middle_key 也必须移动到 recipient 中
  int sz = GetSize();
  // middle_key 是父节点上的值，因此必须放在第一位
  SetKeyAt(0, middle_key);
  // 注意：全部拷贝
  recipient->CopyNFrom(array, sz, buffer_pool_manager);
  SetSize(0);  // 当前为空
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                      BufferPoolManager *buffer_pool_manager) {
  // 将第一个移动到 recipient 的末尾
  auto first_pair = array[0];
  recipient->CopyLastFrom(first_pair, buffer_pool_manager);
  SetKeyAt(0, middle_key);

  // 第一个 pair 被拷贝后，记得删除
  std::move(array + 1, array + GetSize(), array);
  IncreaseSize(-1);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  // 将 pair 拷贝到末尾
  int sz = GetSize();
  *(array + sz) = pair;
  IncreaseSize(1);

  // 由于是内部节点发生更新，所以子节点的父节点指针应该发生改变
  auto page = buffer_pool_manager->FetchPage(pair.second);
  BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  node->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(page->GetPageId(), true);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                       BufferPoolManager *buffer_pool_manager) {
  auto last_pair = array[GetSize() - 1];
  // 注意：此处有区别，recipient 现在是右孩子
  recipient->CopyFirstFrom(last_pair, buffer_pool_manager);
  recipient->SetKeyAt(0, middle_key);
  // 直接 -1，无需移动
  IncreaseSize(-1);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  int sz = GetSize();
  std::move_backward(array, array + sz, array + sz + 1);
  array[0] = pair;
  IncreaseSize(1);

  // 注意：更新子节点的父指针
  auto page = buffer_pool_manager->FetchPage(pair.second);
  BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  node->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(page->GetPageId(), true);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
