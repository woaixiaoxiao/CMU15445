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
#include <cstddef>
#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator();
  IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE *cur_leaf_ptr, page_id_t cur_page_id, int index,
                BufferPoolManager *buffer_pool_manager);
  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool {
    return (cur_page_id_ == itr.cur_page_id_) && (index_ == itr.index_);
  }
  auto operator!=(const IndexIterator &itr) const -> bool { return !operator==(itr); }

  auto FetchLeafPtr(page_id_t next_page_id) -> B_PLUS_TREE_LEAF_PAGE_TYPE *;

  void PrintPageId() { printf("%d %d\n", cur_page_id_,cur_leaf_ptr_->GetNextPageId()); }

 private:
  // add your own private member variables here
  // 代表位置的leaf_page_ptr和index
  B_PLUS_TREE_LEAF_PAGE_TYPE *cur_leaf_ptr_;
  int index_;
  page_id_t cur_page_id_;
  // 一个缓存池管理器，用来执行++操作
  BufferPoolManager *buffer_pool_manager_;
};

}  // namespace bustub
