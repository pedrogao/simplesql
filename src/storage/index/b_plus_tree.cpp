//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const {
  // 如果根节点 id == INVALID_PAGE_ID，则表示没有任何数据，也调用 StartNewTree 函数
  return root_page_id_ == INVALID_PAGE_ID;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  // 获取对应的叶子节点，无需找到最左边的元素
  root_page_id_mutex_.lock();
  if (IsEmpty()) {
    root_page_id_mutex_.unlock();
    return false;
  }
  root_page_id_mutex_.unlock();
  auto page = FindLeafPageByOperation(key, false, false, OperationType::SEARCH, transaction).first;
  if (page == nullptr) {
    return false;
  }
  // 在叶子节点上找到 val
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(page->GetData());
  // LOG_DEBUG("key: %lld try to GetValue from page: %d size is: %d.", key.ToString(), leaf_node->GetPageId(),
  //           leaf_node->GetSize());
  ValueType val;
  // 在叶子结点上通过二分查找 key
  bool existed = leaf_node->Lookup(key, &val, comparator_);
  page->RUnlatch();
  // 只要一个页使用完毕后，那么一定要 Unpin 一下
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  if (!existed) {
    return false;
  }
  result->push_back(val);
  return true;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  // 如果是空树，那么调用 StartNewTree
  //  LOG_DEBUG("txid: %d, thread_id: %zu try to insert key: %lld ", transaction->GetTransactionId(),
  //            std::hash<std::thread::id>{}(transaction->GetThreadId()), key.ToString());
  // 加锁，避免多个线程同时 StartNewTree
  root_page_id_mutex_.lock();
  if (IsEmpty()) {
    StartNewTree(key, value);
    root_page_id_mutex_.unlock();
    return true;
  }
  root_page_id_mutex_.unlock();
  // 非空，插入至叶子节点
  return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  // 开始一个新 B+树
  // LOG_DEBUG("StartNewTree key: %lld.", key.ToString());
  Page *root_page = buffer_pool_manager_->NewPage(&root_page_id_);  // 新建 page
  UpdateRootPageId(1);  // 第一个更新 root_page_id，所以设置为 true
  // 根节点也是叶子节点
  LeafPage *root_node = reinterpret_cast<LeafPage *>(root_page->GetData());
  // 初始化根节点
  root_node->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
  // 向根节点中插入数据
  root_node->Insert(key, value, comparator_);
  // Unpin，设置为 dirty
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  // 寻找 key 所在的叶子节点
  auto [leaf_page, is_root_page_id_locked] =
      FindLeafPageByOperation(key, false, false, OperationType::INSERT, transaction);
  // 得到叶子结点
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());

  // 此时 leaf_page 已经加锁，插入数据
  int old_sz = leaf_node->GetSize();
  int sz = leaf_node->Insert(key, value, comparator_);
  // 相等表示重复，插入失败
  if (sz == old_sz) {
    if (is_root_page_id_locked) {  // 如果是根节点插入，那么 root_page_id 还未解锁
      root_page_id_mutex_.unlock();
    }
    // 将 page set 中的页全部解锁
    ClearTransactionPageSetAndUnpin(transaction);
    leaf_page->WUnlatch();  // 叶子结点解锁
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    return false;
  }

  // 无需 split，所以节点是安全的
  if (sz < leaf_max_size_) {
    if (is_root_page_id_locked) {
      root_page_id_mutex_.unlock();
    }
    ClearTransactionPageSetAndUnpin(transaction);
    leaf_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);  // leaf_node 数据发生了更改
    return true;
  }
  // 需要分裂，由于 new_node 是新建的节点，所以无需加锁
  // you should correctly perform split if insertion triggers current number of key/value pairs after insertion equals
  LeafPage *new_node = Split<LeafPage>(leaf_node);  // 将 page 分裂，new_page 是右边，page 是左边
  // 分裂后，leaf_node 是左孩子，new_node 是右孩子，右孩子的第一个 key 拷贝至父节点作为 key
  // InsertIntoParent 中只能对新 fetch 的 page 做 Unpin
  // 注意：此处暂时无法解锁 root_page_id，在 InsertIntoParent 中插入完毕后再解锁
  InsertIntoParent(leaf_node, new_node->KeyAt(0), new_node, transaction, is_root_page_id_locked);
  // 只解锁自己，其它的在 InsertIntoParent 中解锁
  leaf_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);  // leaf_node 数据发生了更改
  // new_node 压根没有被锁，因此直接 Unpin 即可
  buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);  // new_node 数据发生了更改
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  // 新建一个页，node 在上层 Unpin
  page_id_t page_id;
  auto *page = buffer_pool_manager_->NewPage(&page_id);
  assert(page != nullptr);
  // 与 node 节点类型相同
  N *new_node = reinterpret_cast<N *>(page->GetData());
  new_node->SetPageType(node->GetPageType());
  // 判断节点类型
  if (node->IsLeafPage()) {  // 分离叶子节点
    LeafPage *leaf = reinterpret_cast<LeafPage *>(node);
    // 新的叶子节点
    LeafPage *new_leaf = reinterpret_cast<LeafPage *>(new_node);
    new_leaf->Init(page_id, node->GetParentPageId(), leaf_max_size_);
    // 将 leaf 中的数据移动一半到 new_leaf
    leaf->MoveHalfTo(new_leaf);
    // 注意分离后，leaf 是左边兄弟节点，因此需要设置 next_page_id
    // 判断 leaf 节点有无 next_page_id，如果有，那么将其转交给 new_leaf
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      new_leaf->SetNextPageId(leaf->GetNextPageId());
    }
    leaf->SetNextPageId(page_id);
  } else {
    // 分离内部节点
    InternalPage *internal = reinterpret_cast<InternalPage *>(node);
    InternalPage *new_internal = reinterpret_cast<InternalPage *>(new_node);
    new_internal->Init(page_id, node->GetParentPageId(), internal_max_size_);
    internal->MoveHalfTo(new_internal, buffer_pool_manager_);
  }

  return new_node;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction, bool is_root_page_id_locked) {
  // old_node 是左半部分，且在上层 Unpin，new_node 是右半部分
  if (old_node->IsRootPage()) {  // 如果 old_node 是根节点，那么 root_page_id 一定被加锁了
    // 那么重新申请一个页作为新的根节点，也作为 old_node 和 new_node 的父节点
    Page *new_page = buffer_pool_manager_->NewPage(&root_page_id_);
    if (new_page == nullptr) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "can't allocate new page.");
    }
    // new_page 作为新的根节点，同时也是内部节点
    InternalPage *new_root_node = reinterpret_cast<InternalPage *>(new_page->GetData());
    // 初始化新的根节点
    new_root_node->Init(new_page->GetPageId(), INVALID_PAGE_ID, internal_max_size_);
    // page_id 就是 ValueType，设置 new_root_node 中的值，记住内部节点的第一个 key 是 invalid
    new_root_node->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());  // 填充根节点数据
    // 更新 new_node 和 old_node 的父节点
    old_node->SetParentPageId(new_page->GetPageId());
    new_node->SetParentPageId(new_page->GetPageId());
    // Unpin，new_page 不再使用
    buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true);

    UpdateRootPageId(0);  // root_page_id_ 发生了变化
    if (is_root_page_id_locked) {
      root_page_id_mutex_.unlock();  // 现在才能解锁 root_page_id
    }
    // 更新完毕后，记得解锁
    ClearTransactionPageSetAndUnpin(transaction);
    return;
  }
  // 不是根节点，得到 old_node 的父节点
  auto parent_id = old_node->GetParentPageId();
  // 得到父节点对应的 page
  auto *parent_page = buffer_pool_manager_->FetchPage(parent_id);
  assert(parent_page != nullptr);
  // 拿到父节点，父节点一定是内部节点
  InternalPage *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  new_node->SetParentPageId(parent_id);  // new_node 设置父节点
  // 将 new_node 的 page_id 插入父节点，一定要插在 old_node 位置的后面
  int sz = parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
  if (sz < internal_max_size_) {
    if (is_root_page_id_locked) {
      root_page_id_mutex_.unlock();  // 现在才能解锁 root_page_id
    }
    // 无需再次分裂，直接 clear 即可，同样无需 Unpin，交给上层函数负责
    ClearTransactionPageSetAndUnpin(transaction);
    buffer_pool_manager_->UnpinPage(parent_id, true);
    return;
  }
  // 分裂 parent 节点，因为刚刚插入了 new_old，可能要发生分裂
  // LOG_DEBUG("内部节点分裂: max_size: %d, size: %d", parent_node->GetMaxSize(), parent_node->GetSize());
  InternalPage *parent_sibling = Split<InternalPage>(parent_node);  // 分裂
  // InsertIntoParent 函数递归时，只负责 Unpin 自己当前函数栈 fetch 的页，穿进去的页一律不准 Unpin
  InsertIntoParent(parent_node, parent_sibling->KeyAt(0), parent_sibling, transaction, is_root_page_id_locked);
  if (is_root_page_id_locked) {
    root_page_id_mutex_.unlock();  // 现在才能解锁 root_page_id
  }
  // Unpin
  buffer_pool_manager_->UnpinPage(parent_id, true);
  buffer_pool_manager_->UnpinPage(parent_sibling->GetPageId(), true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  // 删除节点
  // 1. 如果是一棵空树，直接返回
  root_page_id_mutex_.lock();
  if (IsEmpty()) {
    root_page_id_mutex_.unlock();
    return;
  }
  root_page_id_mutex_.unlock();
  //  LOG_DEBUG("txid: %d, thread_id: %zu, key: %lld", transaction->GetTransactionId(),
  //            std::hash<std::thread::id>{}(transaction->GetThreadId()), key.ToString());
  // 2. 找到含有 key 的叶子节点，然后删除该项，注意一定要注意 redistribute 和 merge
  // merge：当前叶子节点删除 entry 后，所剩的 entry 个数与兄弟节点 entry 个数加起来 < MaxSize，则二者合并
  // redistribute：当前叶子节点删除 entry 后，所剩 entry 个数与兄弟节点 entry 个数加起来 >= MaxSize，则进行重组
  auto [page, is_root_page_id_locked] = FindLeafPageByOperation(key, false, false, OperationType::DELETE, transaction);
  assert(page != nullptr);
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(page->GetData());
  int old_sz = leaf_node->GetSize();
  // 删除 entry
  // 得到删除后的叶子节点 entry 个数
  int sz = leaf_node->RemoveAndDeleteRecord(key, comparator_);
  if (sz == old_sz) {  // 删除失败
    if (is_root_page_id_locked) {
      root_page_id_mutex_.unlock();
    }
    ClearTransactionPageSetAndUnpin(transaction);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    // LOG_DEBUG("txid: %d, thread_id: %zu, key: %lld delete failed", transaction->GetTransactionId(),
    //           std::hash<std::thread::id>{}(transaction->GetThreadId()), key.ToString());
    return;
  }

  // 需要合并或者重组，注意：里面无需对 leaf_node Unpin
  // coalesce 或者 redistribute
  bool should_delete = CoalesceOrRedistribute(leaf_node, transaction, is_root_page_id_locked);
  if (should_delete) {
    // LOG_DEBUG("page : %d AddIntoDeletedPageSet. ", leaf_node->GetPageId());
    transaction->AddIntoDeletedPageSet(leaf_node->GetPageId());
  }
  // 先解锁，后 Unpin
  page->WUnlatch();
  // 使用完该页后，记得 Unpin，必须 Unpin 后才能删除
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  // 删除页
  std::for_each(transaction->GetDeletedPageSet()->begin(), transaction->GetDeletedPageSet()->end(),
                [&bpm = buffer_pool_manager_](const page_id_t page_id) { bpm->DeletePage(page_id); });
  transaction->GetDeletedPageSet()->clear();
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction, bool is_root_page_id_locked) {
  // 如果是 root 节点，做 AjustRoot
  // 为什么 is_root_page_id_locked 需要一直向下传？
  // 因为 CoalesceOrRedistribute 是递归调用的，不知道是否需要在 AdjustRoot 中调整根节点，所以就一直向下
  if (node->IsRootPage()) {
    bool root_should_delete = AdjustRoot(node, is_root_page_id_locked);
    ClearTransactionPageSetAndUnpin(transaction);  // 清除 page set，全部解锁
    return root_should_delete;
  }

  // 判断一下
  if (node->GetSize() >= node->GetMinSize()) {
    ClearTransactionPageSetAndUnpin(transaction);
    return false;
  }

  // 如果不是，判断兄弟节点的个数，做 Coalesce 还是 Redistribute
  // 注意：CoalesceOrRedistribute 既可以对叶子结点操作，也可以对内部节点操作，所以必须使用 N 类型
  // 找到兄弟节点
  Page *parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  // 注意：当前节点处于 unsafe 的状态下，那么父节点一定已经加锁了，因此此处无需再次加锁
  // 父节点一定是内部节点
  InternalPage *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());
  // 找到当前节点在父节点中的序号
  // Let K′ be the value between pointers N and N′ in parent(N)
  // K 应该是右边孩子节点的第一个元素
  int idx = parent->ValueIndex(node->GetPageId());
  Page *sibling_page;
  if (idx == 0) {
    // 如果 node 在 0 号，sliding 在右边
    // 此时 sliding 是 node 的后继节点
    sibling_page = buffer_pool_manager_->FetchPage(parent->ValueAt(1));
  } else {
    // 否则，sliding 在左边
    // sliding 是 node 的前驱节点
    sibling_page = buffer_pool_manager_->FetchPage(parent->ValueAt(idx - 1));
  }
  sibling_page->WLatch();  // 兄弟节点也需要加锁
  N *sibling_node = reinterpret_cast<N *>(sibling_page->GetData());
  // If sibling's size + input* page's size > page's max size, then redistribute. Otherwise, merge.
  // 如果 leaf 和 sibling entry 个数 >= MaxSize，Coalesce
  if (node->GetSize() + sibling_node->GetSize() > node->GetMaxSize()) {
    // Redistribute，就不会有删除发生
    Redistribute(sibling_node, node, parent, idx, is_root_page_id_locked);
    ClearTransactionPageSetAndUnpin(transaction);
    sibling_page->WUnlatch();
    // 重组完毕后，需要更新 parent
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);
    return false;  // no deletion happens
  }
  // 合并
  bool should_parent_delete = Coalesce(&sibling_node, &node, &parent, idx, transaction, is_root_page_id_locked);
  // 注意，parent_page 在 page set 中，因此无需此处来解锁
  if (should_parent_delete) {
    // LOG_DEBUG("page : %d AddIntoDeletedPageSet. ", parent->GetPageId());
    transaction->AddIntoDeletedPageSet(parent->GetPageId());
  }
  sibling_page->WUnlatch();
  // 但是此处 Fetch 了一次 parent_page，那么我们就必须 Unpin 一次
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
  // 当前函数只能 Unpin 自己产生的 page
  buffer_pool_manager_->UnpinPage(sibling_node->GetPageId(), true);
  // 返回 true 表示 node 应该被删除
  return false;  // true means target leaf page should be deleted
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction, bool is_root_page_id_locked) {
  // Remember to deal with coalesce or redistribute recursively if necessary
  // 合并以后可能还需要合并或者重组
  // if (N is a predecessor of N′) then swap variables(N, N′)
  // if (N is not a leaf)
  // append K′ and all pointers and values in N to N′
  // else append all (Ki, Pi) pairs in N to N′; set N′.Pn = N.Pn
  // delete entry(parent(N), K′, N); delete node N
  // 默认认为 index 是 right_index 右孩子的序号，如果 index == 0，那么只能当作左孩子，然后交换，他还是右孩子
  // 所以要删除的节点仍旧是右孩子，要删除的指针也是右孩子的指针
  int right_index = index;
  if (index == 0) {
    right_index = 1;
    std::swap(node, neighbor_node);
  }
  KeyType right_key = (*parent)->KeyAt(right_index);
  if (!(*node)->IsLeafPage()) {
    // 如果 node 是内部结点，直接将 index 和 node 中的所有项，以及 index 拷贝到 neighbor_node 中
    InternalPage *internal_node = reinterpret_cast<InternalPage *>((*node));
    InternalPage *neighbor_internal_node = reinterpret_cast<InternalPage *>((*neighbor_node));
    internal_node->MoveAllTo(neighbor_internal_node, right_key, buffer_pool_manager_);
  } else {
    // 如果是叶子结点，那么将 node 中的所有项拷贝到 neighbor_node 中
    LeafPage *leaf_node = reinterpret_cast<LeafPage *>((*node));
    LeafPage *neighbor_leaf_node = reinterpret_cast<LeafPage *>((*neighbor_node));
    leaf_node->MoveAllTo(neighbor_leaf_node);
  }
  // 从 parent 中删除 right_key
  (*parent)->Remove(right_index);
  // this->Print(buffer_pool_manager_);
  // true means parent node should be deleted, false means no deletion
  return CoalesceOrRedistribute(*parent, transaction, is_root_page_id_locked);
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node,
                                  BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent, int index,
                                  bool is_root_page_id_locked) {
  // 重组
  if (is_root_page_id_locked) {
    root_page_id_mutex_.unlock();  // 重组不可能删除根节点，也不会改变根节点的id，因此直接解锁
  }
  // index 是 node 在父节点上的序号
  // 如果 index == 0，将 neighbor_node 中的第一个项移动到 node 的尾部
  // 否则将 neighbor_node 的最后一个项移动到 node 的头部
  if (node->IsLeafPage()) {
    LeafPage *leaf_node = reinterpret_cast<LeafPage *>((node));
    LeafPage *neighbor_leaf_node = reinterpret_cast<LeafPage *>(neighbor_node);
    if (index == 0) {
      neighbor_leaf_node->MoveFirstToEndOf(leaf_node);
      // index == 0 时，node 在 0 处，那么 neighbor_node 的 key 就必须在 1 处
      parent->SetKeyAt(1, neighbor_leaf_node->KeyAt(0));
    } else {
      neighbor_leaf_node->MoveLastToFrontOf(leaf_node);
      // node 是后继节点
      parent->SetKeyAt(index, leaf_node->KeyAt(0));
    }
  } else {
    InternalPage *internal_node = reinterpret_cast<InternalPage *>(node);
    InternalPage *neighbor_internal_node = reinterpret_cast<InternalPage *>(neighbor_node);
    if (index == 0) {
      // neighbor_internal_node 在右边
      neighbor_internal_node->MoveFirstToEndOf(internal_node, parent->KeyAt(1), buffer_pool_manager_);
      parent->SetKeyAt(1, neighbor_internal_node->KeyAt(0));
    } else {
      // neighbor_internal_node 在左边，如果在左边就把最后一个移动到最前面
      // internal_node 的第一个 pair 中的 key 来自父节点，val(孩子指针)来自兄弟节点
      neighbor_internal_node->MoveLastToFrontOf(internal_node, parent->KeyAt(index), buffer_pool_manager_);
      parent->SetKeyAt(index, internal_node->KeyAt(0));
    }
  }
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node, bool is_root_page_id_locked) {
  // if (N is the root and N has only one remaining child)
  // then make the child of N the new root of the tree and delete N
  // case 1: when you delete the last element in root page, but root page still has one last child
  // 根节点还不是最后一个节点，仍然是内部节点，且有一个孩子节点
  if (old_root_node->GetSize() == 1 && !old_root_node->IsLeafPage()) {
    // 孩子节点成为新的根节点，然后删除旧的根节点
    InternalPage *internal_node = reinterpret_cast<InternalPage *>(old_root_node);
    Page *child_page = buffer_pool_manager_->FetchPage(internal_node->ValueAt(0));
    // 孩子节点成为新的根结点
    BPlusTreePage *new_root_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
    new_root_node->SetParentPageId(INVALID_PAGE_ID);
    root_page_id_ = new_root_node->GetPageId();
    UpdateRootPageId(0);
    if (is_root_page_id_locked) {
      root_page_id_mutex_.unlock();
    }
    // 注意：child_page 在此处 Fetch，也必须在此处 Unpin
    buffer_pool_manager_->UnpinPage(child_page->GetPageId(), true);
    return true;  // root_page should be deleted
  }

  // case 2: when you delete the last element in whole b+ tree
  // 只剩下根节点了，且已经没有子节点了
  // true means root page should be deleted, false means no deletion happend
  bool should_delete = old_root_node->IsLeafPage() && old_root_node->GetSize() == 0;
  if (should_delete) {  // 删除根节点
    root_page_id_ = INVALID_PAGE_ID;
  }
  if (is_root_page_id_locked) {
    root_page_id_mutex_.unlock();
  }
  return should_delete;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() {
  assert(!IsEmpty());
  auto left_most_page = FindLeafPageByOperation(KeyType(), true).first;
  return INDEXITERATOR_TYPE(buffer_pool_manager_, left_most_page, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  assert(!IsEmpty());
  auto page = FindLeafPageByOperation(key, false).first;
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(page->GetData());
  auto idx = leaf_node->KeyIndex(key, comparator_);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, page, idx);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() {
  assert(!IsEmpty());
  auto right_most_page = FindLeafPageByOperation(KeyType(), false, true).first;
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(right_most_page->GetData());
  auto sz = leaf_node->GetSize();
  return INDEXITERATOR_TYPE(buffer_pool_manager_, right_most_page, sz);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * 寻找包含 key 的叶子节点，如果 leftMost 为 true，寻找最左边的叶子节点
 * op_type 默认是 search
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  return FindLeafPageByOperation(key, leftMost, false, OperationType::SEARCH).first;
}

INDEX_TEMPLATE_ARGUMENTS
std::pair<Page *, bool> BPLUSTREE_TYPE::FindLeafPageByOperation(const KeyType &key, bool leftMost, bool rightMost,
                                                                OperationType op_type, Transaction *transaction) {
  root_page_id_mutex_.lock();
  bool is_root_page_id_latched = true;
  assert(root_page_id_ != INVALID_PAGE_ID);
  auto page = buffer_pool_manager_->FetchPage(root_page_id_);
  BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  if (op_type == OperationType::SEARCH) {
    page->RLatch();
    is_root_page_id_latched = false;
    root_page_id_mutex_.unlock();
  } else {
    page->WLatch();
    if (IsSafety(node, op_type)) {
      is_root_page_id_latched = false;
      root_page_id_mutex_.unlock();
    }
  }
  while (!node->IsLeafPage()) {
    InternalPage *i_node = reinterpret_cast<InternalPage *>(node);
    page_id_t child_node_page_id;
    if (leftMost) {
      child_node_page_id = i_node->ValueAt(0);
    } else if (rightMost) {
      child_node_page_id = i_node->ValueAt(i_node->GetSize() - 1);
    } else {
      child_node_page_id = i_node->Lookup(key, comparator_);
    }
    assert(child_node_page_id > 0);

    auto child_page = buffer_pool_manager_->FetchPage(child_node_page_id);
    auto child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
    if (op_type == OperationType::SEARCH) {
      child_page->RLatch();
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    } else {
      child_page->WLatch();
      transaction->AddIntoPageSet(page);
      if (IsSafety(child_node, op_type)) {
        if (is_root_page_id_latched) {
          is_root_page_id_latched = false;
          root_page_id_mutex_.unlock();
        }
        ClearTransactionPageSetAndUnpin(transaction);
      }
    }
    page = child_page;
    node = child_node;
  }
  return std::make_pair(page, is_root_page_id_latched);
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  Page *page = buffer_pool_manager_->FetchPage(HEADER_PAGE_ID);
  HeaderPage *header_page = static_cast<HeaderPage *>(page);
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  // 每次更新 header_page，顺便 Unpin
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * 判断当前节点是否并发安全
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsSafety(BPlusTreePage *node, OperationType op_type) {
  // 对于插入操作，只有当前 size 小于 GetMaxSize() - 1 才是安全的
  // 对于删除操作，只有当前 size 大于 GetMinSize() 才是安全的
  if (op_type == OperationType::INSERT) {
    return node->GetSize() < node->GetMaxSize() - 1;
  }
  // 删除操作
  // 根节点无需 half
  if (node->IsRootPage()) {
    return node->GetSize() > 2;
  }
  return node->GetSize() > node->GetMinSize();
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ClearTransactionPageSet(Transaction *transaction) {
  assert(transaction != nullptr);
  std::for_each(transaction->GetPageSet()->begin(), transaction->GetPageSet()->end(),
                [](Page *page) { page->WUnlatch(); });
  transaction->GetPageSet()->clear();
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ClearTransactionPageSetAndUnpin(Transaction *transaction) {
  assert(transaction != nullptr);
  std::for_each(transaction->GetPageSet()->begin(), transaction->GetPageSet()->end(),
                [&bpm = buffer_pool_manager_](Page *page) {
                  page->WUnlatch();
                  bpm->UnpinPage(page->GetPageId(), false);
                });
  transaction->GetPageSet()->clear();
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/***************************************************************************
 *  Check integrity of B+ tree data structure.
 ***************************************************************************/

// INDEX_TEMPLATE_ARGUMENTS
// int BPLUSTREE_TYPE::isBalanced(page_id_t pid) {
//   if (IsEmpty()) {
//     return 1;
//   }
//   auto node = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(pid));
//   assert(node != nullptr);
//   int ret = 0;
//   if (!node->IsLeafPage()) {
//     auto page = reinterpret_cast<InternalPage *>(node);
//     int last = -2;
//     for (int i = 0; i < page->GetSize(); i++) {
//       int cur = isBalanced(page->ValueAt(i));
//       if (cur >= 0 && last == -2) {
//         last = cur;
//         ret = last + 1;
//       } else if (last != cur) {
//         ret = -1;
//         break;
//       }
//     }
//   }
//   buffer_pool_manager_->UnpinPage(pid, false);
//   return ret;
// }
//
// INDEX_TEMPLATE_ARGUMENTS
// bool BPLUSTREE_TYPE::isPageCorr(page_id_t pid, std::pair<KeyType, KeyType> &out) {
//   if (IsEmpty()) {
//     return true;
//   }
//   auto node = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(pid));
//   assert(node != nullptr);
//   bool ret = true;
//   if (node->IsLeafPage()) {
//     auto page = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(node);
//     int size = page->GetSize();
//     if (page->IsRootPage()) {
//       ret = ret && (size <= node->GetMaxSize());
//     } else {
//       ret = ret && (size >= node->GetMinSize() && size <= node->GetMaxSize());
//     }
//     for (int i = 1; i < size; i++) {
//       if (comparator_(page->KeyAt(i - 1), page->KeyAt(i)) > 0) {
//         ret = false;
//         break;
//       }
//     }
//     out = std::pair<KeyType, KeyType>{page->KeyAt(0), page->KeyAt(size - 1)};
//   } else {
//     auto page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(node);
//     int size = page->GetSize();
//     if (page->IsRootPage()) {
//       ret = ret && (size <= node->GetMaxSize());
//     } else {
//       ret = ret && (size >= node->GetMinSize() && size <= node->GetMaxSize());
//     }
//     std::pair<KeyType, KeyType> left;
//     std::pair<KeyType, KeyType> right;
//     for (int i = 1; i < size; i++) {
//       if (i == 1) {
//         ret = ret && isPageCorr(page->ValueAt(0), left);
//       }
//       ret = ret && isPageCorr(page->ValueAt(i), right);
//       ret = ret && (comparator_(page->KeyAt(i), left.second) > 0 && comparator_(page->KeyAt(i), right.first) <= 0);
//       ret = ret && (i == 1 || comparator_(page->KeyAt(i - 1), page->KeyAt(i)) < 0);
//       if (!ret) {
//         break;
//       }
//       left = right;
//     }
//     out = std::pair<KeyType, KeyType>{page->KeyAt(0), page->KeyAt(size - 1)};
//   }
//   buffer_pool_manager_->UnpinPage(pid, false);
//   return ret;
// }
//
// INDEX_TEMPLATE_ARGUMENTS
// bool BPLUSTREE_TYPE::Check(bool forceCheck) {
//   if (!forceCheck) {
//     return true;
//   }
//   std::pair<KeyType, KeyType> in;
//   bool isPageInOrderAndSizeCorr = isPageCorr(root_page_id_, in);
//   bool isBal = (isBalanced(root_page_id_) >= 0);
//   bool isAllUnpin = buffer_pool_manager_->CheckAllUnpined();
//   if (!isPageInOrderAndSizeCorr) {
//     LOG_DEBUG("problem in page order or page size.");
//   }
//   if (!isBal) {
//     LOG_DEBUG("problem in balance.");
//   }
//   if (!isAllUnpin) {
//     LOG_DEBUG("problem in page unpin.");
//   }
//   return isPageInOrderAndSizeCorr && isBal && isAllUnpin;
// }

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
