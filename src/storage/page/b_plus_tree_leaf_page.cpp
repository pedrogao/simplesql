//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <iterator>
#include <sstream>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);  // 叶子节点
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/**
 * Helper method to find the first index i so that array[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const {
  int sz = GetSize();
  assert(sz >= 0);
  int st = 0;
  int ed = sz - 1;
  while (st <= ed) {  // find the last key in array <= input
    int mid = (ed - st) / 2 + st;
    if (comparator(array[mid].first, key) >= 0) {
      ed = mid - 1;
    } else {
      st = mid + 1;
    }
  }
  return ed + 1;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  assert(index >= 0);
  if (index > 0) {
    assert(index < GetSize());
  }
  KeyType key = array[index].first;
  return key;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
  // replace with your own code
  // assert(index >= 0 && index < GetSize());
  return array[index];
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return  page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) {
  // 向叶子节点中插入数据
  // LOG_DEBUG("try Insert key: %lld.", key.ToString());
  int sz = GetSize();
  int gtIdx = KeyIndex(key, comparator);
  // 不支持重复 key，相等则不插入
  if (comparator(array[gtIdx].first, key) == 0) {
    return sz;
  }
  // 末位
  if (gtIdx == sz) {
    array[gtIdx].first = key;
    array[gtIdx].second = value;
    IncreaseSize(1);
    // LOG_DEBUG("Insert key: %lld, index: %d .", key.ToString(), gtIdx);
    return sz + 1;
  }

  // link: https://cloud.tencent.com/developer/section/1009822
  // 移动 [s_first, s_last) 到 d_last)
  // 即将 index~sz 的元素整体向后移动 1 位
  std::move_backward(array + gtIdx, array + sz, array + sz + 1);
  // 然后 new_index 就空出来了
  array[gtIdx].first = key;
  array[gtIdx].second = value;
  IncreaseSize(1);
  // LOG_DEBUG("Insert key: %lld, index: %d .", key.ToString(), gtIdx);
  return sz + 1;
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient) {
  auto start_index = GetMinSize();  // 得到一般的大小，保留前面 [0, start_index)
  // 将 [start_index, size) 拷贝到 recipient 中
  recipient->CopyNFrom(array + start_index, GetSize() - start_index);
  SetSize(start_index);  // 更新大小
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType *items, int size) {
  // 将 items 拷贝到 array 中
  int sz = GetSize();
  // copy(src_start, src_end, target_start)
  // [src_start, src_end) => target_start
  std::copy(items, items + size, array + sz);
  IncreaseSize(size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType *value, const KeyComparator &comparator) const {
  // 寻找 key，如果存在，设置 value，返回 true；不存在返回 false
  int sz = GetSize();
  int foundIdx = KeyIndex(key, comparator);
  // 未找到
  if (foundIdx >= sz || comparator(key, KeyAt(foundIdx)) != 0) {
    return false;
  }
  // 找到了且赋值
  *value = array[foundIdx].second;
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return   page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator) {
  // 查询是否存在，如果不存在，直接返回
  int sz = GetSize();
  int foundIdx = KeyIndex(key, comparator);
  // 未找到
  if (foundIdx >= sz || comparator(key, KeyAt(foundIdx)) != 0) {
    return sz;
  }
  // LOG_DEBUG("RemoveAndDeleteRecord key: %lld, found key: %lld", key.ToString(), p->first.ToString());
  // 存在，所以删除，向前移动
  std::move(array + foundIdx + 1, array + sz, array + foundIdx);
  IncreaseSize(-1);
  // page size after deletion
  return sz - 1;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient) {
  // 将当前叶子结点中的所有对移动到 recipient 中
  int sz = GetSize();
  recipient->CopyNFrom(array, sz);
  SetSize(0);
  // Don't forget to update the next_page id in the sibling page
  recipient->SetNextPageId(GetNextPageId());  // 继承我的 next_page_id
  SetNextPageId(INVALID_PAGE_ID);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient) {
  auto first_pair = array[0];
  // 向前移动
  std::move(array + 1, array + GetSize(), array);
  IncreaseSize(-1);
  recipient->CopyLastFrom(first_pair);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
  // 添加到末尾，注意从 0 开始
  array[GetSize()] = item;
  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient) {
  int sz = GetSize();
  auto last_pair = array[sz - 1];
  IncreaseSize(-1);

  recipient->CopyFirstFrom(last_pair);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item) {
  int sz = GetSize();
  std::move_backward(array, array + sz, array + sz + 1);
  array[0] = item;
  IncreaseSize(1);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
