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
#include <sstream>

#include "common/exception.h"
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
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetNextPageId(INVALID_PAGE_ID);
  SetMaxSize(max_size);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  KeyType key{};
  key = array_[index].first;
  return key;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::SearchKeyPos(const KeyType &key, KeyComparator &comparator) const -> int {
  int left = 0;
  int right = GetSize() - 1;
  while (left < right) {
    int mid = (left + right) >> 1;
    auto cmp_result = comparator(KeyAt(mid), key);
    if (cmp_result == 0) {
      return mid;
    }
    if (cmp_result < 0) {
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }
  if (comparator(KeyAt(left), key) == 0) {
    return left;
  }
  return -1;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, KeyComparator &comparator) -> bool {
  // 如果当前叶节点是空的，直接插入即可
  if (GetSize() == 0) {
    SetKeyAt(0, key);
    SetValueAt(0, value);
    IncreaseSize(1);
    return true;
  }
  // 二分查找
  int left = 0;
  int right = GetSize() - 1;
  while (left < right) {
    int mid = (left + right) >> 1;
    auto cmpans = comparator(KeyAt(mid), key);
    if (cmpans == 0) {
      return false;
    }
    if (cmpans > 0) {
      right = mid;
    } else {
      left = mid + 1;
    }
  }
  if (comparator(KeyAt(left), key) == 0) {
    return false;
  }
  // 如果没有比key大的，那么key紧接着放在最后面就行了
  if (comparator(KeyAt(left), key) < 0) {
    SetKeyAt(left + 1, key);
    SetValueAt(left + 1, value);
  } else {
    // 如果有比key大的，那么从left开始后面的每个元素都要往后挪一个
    std::copy_backward(array_ + left, array_ + GetSize(), array_ + GetSize() + 1);
    SetKeyAt(left, key);
    SetValueAt(left, value);
  }
  // 最后更新长度，因为前面的所有操作都要用到长度
  IncreaseSize(1);
  // 成功插入
  return true;
}

// 将自己的后半部分都放入新的结点
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLatterHalfTo(BPlusTreeLeafPage *new_leaf_node_ptr) {
  // 首先，分别计算出应该留下的，和需要挪出去的key的数量，这里采取将中间一个key留在当前结点的做法
  int remain_count = GetMaxSize() / 2 + (GetMaxSize() % 2 != 0);
  int move_count = GetMaxSize() - remain_count;
  // 将需要拷贝的key拷贝过去
  std::copy(&array_[remain_count], &array_[GetMaxSize()], new_leaf_node_ptr->array_);
  // 重置两个叶节点的大小
  SetSize(remain_count);
  new_leaf_node_ptr->SetSize(move_count);
  // 更新新节点的parent_page+id
  new_leaf_node_ptr->SetParentPageId(GetParentPageId());
  // 重置两个叶节点的next_page_id
  new_leaf_node_ptr->SetNextPageId(GetNextPageId());
  SetNextPageId(new_leaf_node_ptr->GetPageId());
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::PairAt(int index) -> MappingType & { return array_[index]; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::SearchBigOrEqualPos(KeyType key, KeyComparator &comparator) -> int {
  int left = 0;
  int right = GetSize() - 1;
  while (left < right) {
    int mid = (left + right) >> 1;
    // std::cout<<KeyAt(mid)<<" "<<key<<std::endl;
    if (comparator(KeyAt(mid), key) >= 0) {
      right = mid;
    } else {
      left = mid + 1;
    }
    // std::cout<<left<<" "<<right<<std::endl;
  }

  if (comparator(KeyAt(left), key) >= 0) {
    return left;
  }
  return -1;
}

// hehe
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(const KeyType &key, KeyComparator &comparator) -> bool {
  // 先查找当前叶节点是否有这个key，如果没有，直接return false
  int index = SearchKeyPos(key, comparator);
  if (index == -1) {
    return false;
  }
  // 如果有这个key，则将后面的所有元素都向前移，并减小size
  std::copy(array_ + index + 1, array_ + GetSize(), array_ + index);
  DecreaseSize(1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndof(BPlusTreeLeafPage *receive_ptr) {
  int size = receive_ptr->GetSize();
  receive_ptr->SetKeyAt(size, KeyAt(0));
  receive_ptr->SetValueAt(size, ValueAt(0));
  receive_ptr->IncreaseSize(1);
  std::copy(array_ + 1, array_ + GetSize(), array_);
  DecreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveBackOne() {
  std::copy_backward(array_, array_ + GetSize(), array_ + GetSize() + 1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFirstof(BPlusTreeLeafPage *receive_ptr) {
  // 首先把receive的所有值都往后一个位置
  receive_ptr->MoveBackOne();
  receive_ptr->IncreaseSize(1);
  // 然后将当前的最后一个放到receive的第一个
  int size = GetSize();
  receive_ptr->SetKeyAt(0, KeyAt(size - 1));
  receive_ptr->SetValueAt(0, ValueAt(size - 1));
  // 最后更新size
  DecreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *receive_ptr) {
  int receive_size = receive_ptr->GetSize();
  int send_size = GetSize();
  std::copy(&array_[0], &array_[send_size], &receive_ptr->array_[receive_size]);
  SetSize(0);
  receive_ptr->IncreaseSize(send_size);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
