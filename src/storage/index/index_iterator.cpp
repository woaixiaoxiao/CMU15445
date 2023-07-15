/**
 * index_iterator.cpp
 */
#include <cassert>

#include "common/config.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page.h"
#include "type/numeric_type.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE *cur_leaf_ptr, page_id_t cur_page_id, int index,
                                  BufferPoolManager *buffer_pool_manager) {
  cur_leaf_ptr_ = cur_leaf_ptr;
  cur_page_id_ = cur_page_id;
  index_ = index;
  buffer_pool_manager_ = buffer_pool_manager;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  // 当前迭代器失效的时候，那么要把这个迭代器代表的页面给unpin掉
  if (cur_page_id_ != INVALID_PAGE_ID) {
    buffer_pool_manager_->UnpinPage(cur_page_id_, false);
  }
}  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return cur_page_id_ == INVALID_PAGE_ID; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { return cur_leaf_ptr_->PairAt(index_); }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::FetchLeafPtr(page_id_t next_page_id) -> B_PLUS_TREE_LEAF_PAGE_TYPE * {
  if (next_page_id == INVALID_PAGE_ID) {
    return nullptr;
  }
  Page *page_ptr = buffer_pool_manager_->FetchPage(next_page_id);
  auto tree_page = reinterpret_cast<BPlusTreePage *>(page_ptr->GetData());
  return reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(tree_page);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  // 如果当前页面已经是end，该怎么处理呢？
  if (cur_page_id_ == INVALID_PAGE_ID) {
    return *this;
  }
  // 先给index+1
  index_++;
  // 如果index没有达到限制，则返回当前页面的新的index对应的迭代器
  if (index_ < cur_leaf_ptr_->GetSize()) {
    return *this;
  }
  // 如果index超过了限制，则取出下一个页面，重置index的值，并返回对应迭代器。记得unpin当前页面
  // 首先根据next_page_id取出下一个页面的指针，注意，有可能下一个页面没有了，也就是接下来就是end，需要特判
  page_id_t next_page_id = cur_leaf_ptr_->GetNextPageId();
  if (next_page_id == INVALID_PAGE_ID) {
    buffer_pool_manager_->UnpinPage(cur_page_id_, false);
    cur_leaf_ptr_ = nullptr;
    index_ = -1;
    cur_page_id_ = INVALID_PAGE_ID;
    return *this;
  }
  B_PLUS_TREE_LEAF_PAGE_TYPE *next_page_ptr = FetchLeafPtr(next_page_id);
  // unpin当前页面
  buffer_pool_manager_->UnpinPage(cur_page_id_, false);
  // 更新当前迭代器的属性为新的页面
  cur_leaf_ptr_ = next_page_ptr;
  cur_page_id_ = next_page_id;
  index_ = 0;
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
