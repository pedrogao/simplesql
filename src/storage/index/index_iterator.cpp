/**
 * index_iterator.cpp
 */

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, Page *page, int idx)
    : buffer_pool_manager_(bpm), page_(page), idx_(idx) {
  leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  page_->RUnlatch();
  buffer_pool_manager_->UnpinPage(page_->GetPageId(), false);
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() { return leaf->GetNextPageId() == INVALID_PAGE_ID && idx_ == leaf->GetSize(); }

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() { return leaf->GetItem(idx_); }

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  if (idx_ == leaf->GetSize() - 1 && leaf->GetNextPageId() != INVALID_PAGE_ID) {
    auto next_page = buffer_pool_manager_->FetchPage(leaf->GetNextPageId());

    next_page->RLatch();
    page_->RUnlatch();
    buffer_pool_manager_->UnpinPage(page_->GetPageId(), false);
    page_ = next_page;
    leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page_->GetData());
    idx_ = 0;
  } else {
    idx_++;
  }

  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const {
  return leaf->GetPageId() == itr.leaf->GetPageId() && idx_ == itr.idx_;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const {
  return !(leaf->GetPageId() == itr.leaf->GetPageId() && idx_ == itr.idx_);
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
