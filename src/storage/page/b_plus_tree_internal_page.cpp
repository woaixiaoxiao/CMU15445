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
#include <cstddef>
#include <iostream>
#include <sstream>

#include "common/config.h"
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
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(1);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  KeyType key{};
  key = array_[index].first;
  return key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }
/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::SearchNextChildPageID(const KeyType &key, KeyComparator &comparator) const
    -> page_id_t {
  // 二分查找，从索引为1的地方开始，第i个的所指向的孩子都是>=第i个key的
  // 我要找的其实也是小于等于key的最后一个
  // 开始二分查找
  size_t start = 1;
  size_t end = GetSize() - 1;
  while (start < end) {
    size_t mid = (start + end + 1) >> 1;
    // 这个比较的操作符很神奇，留坑
    if (comparator(KeyAt(mid), key) <= 0) {
      start = mid;
    } else {
      end = mid - 1;
    }
  }
  //如果没有小于等于key的，也就是说需要取index为0的孩子
  if (comparator(KeyAt(start), key) > 0) {
    return ValueAt(0);
  }
  return ValueAt(start);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, page_id_t page_id, KeyComparator &comparator) {
  // 找到当前这个KV应该插到哪里，使用二分查找，找到第一个大于这个key的位置
  int left = 1;
  int right = GetSize() - 1;
  while (left < right) {
    int mid = (left + right) >> 1;
    if (comparator(KeyAt(mid), key) > 0) {
      right = mid;
    } else {
      left = mid + 1;
    }
  }
  // 如果没有比这个key大的，那么直接放在后面
  if (comparator(KeyAt(left), key) <= 0) {
    SetKeyAt(left + 1, key);
    SetValueAt(left + 1, page_id);
  } else {
    // 如果有比这个key大的，那么挪挪位置
    std::copy_backward(array_ + left, array_ + GetSize(), array_ + GetSize() + 1);
    SetKeyAt(left, key);
    SetValueAt(left, page_id);
  }
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLatterHalfTo(BPlusTreeInternalPage *new_brother, const KeyType &key,
                                                      page_id_t page_id, KeyComparator &comparator) {
  // 先将这个KV插入原结点
  Insert(key, page_id, comparator);
  // 分别计算移动之后，当前节点和新的兄弟分别占多少个
  int retain_size = (GetSize() + 1) / 2;
  int move_size = GetSize() - retain_size;
  // 将键值对移动到兄弟
  std::copy(&array_[retain_size], &array_[GetSize()], new_brother->array_);
  // 更新当前节点和兄弟的size
  SetSize(retain_size);
  new_brother->SetSize(move_size);
  // 修改兄弟结点的father为当前节点的father
  new_brother->SetParentPageId(GetParentPageId());
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::SearchKeyPos(const KeyType &key, KeyComparator &comparator) const -> int {
  int left = 1;
  int right = GetSize() - 1;
  while (left < right) {
    int mid = (left + right) >> 1;
    auto cmpres = comparator(KeyAt(mid), key);
    if (cmpres > 0) {
      right = mid - 1;
    } else if (cmpres < 0) {
      left = mid + 1;
    } else {
      return mid;
    }
  }
  if (comparator(KeyAt(left), key) == 0) {
    return left;
  }
  return -1;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(const KeyType &key, KeyComparator &comparator) -> bool {
  int index = SearchKeyPos(key, comparator);
  if (index == -1) {
    return false;
  }
  std::copy(array_ + index + 1, array_ + GetSize(), array_ + index);
  DecreaseSize(1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::SearchIndex(const KeyType &key, KeyComparator &comparator) const -> int {
  size_t start = 1;
  size_t end = GetSize() - 1;
  while (start < end) {
    size_t mid = (start + end + 1) >> 1;
    // 这个比较的操作符很神奇，留坑
    if (comparator(KeyAt(mid), key) <= 0) {
      start = mid;
    } else {
      end = mid - 1;
    }
  }
  //如果没有小于等于key的，也就是说需要取index为0的孩子
  if (comparator(KeyAt(start), key) > 0) {
    return 0;
  }
  return start;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndof(BPlusTreeInternalPage* receive_ptr)
{
  int size=receive_ptr->GetSize();
  receive_ptr->SetKeyAt(size, KeyAt(0));
  receive_ptr->SetValueAt(size, ValueAt(0));
  receive_ptr->IncreaseSize(1);
  std::copy(array_ +1, array_ + GetSize(), array_);
  DecreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveBackOne(){
  std::copy_backward(array_, array_ + GetSize(), array_ + GetSize() + 1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFirstof(BPlusTreeInternalPage* receive_ptr)
{
  // 首先把receive的所有值都往后一个位置
  receive_ptr->MoveBackOne();
  receive_ptr->IncreaseSize(1);
  // 然后将当前的最后一个放到receive的第一个
  int size=GetSize();
  receive_ptr->SetKeyAt(0, KeyAt(size-1));
  receive_ptr->SetValueAt(0,ValueAt(size-1));
  // 最后更新size
  DecreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage* receive_ptr){
  int receive_size=receive_ptr->GetSize();
  int size=GetSize();
  std::copy(&array_[0],&array_[size],&receive_ptr->array_[receive_size]);
  SetSize(0);
  receive_ptr->IncreaseSize(size);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
