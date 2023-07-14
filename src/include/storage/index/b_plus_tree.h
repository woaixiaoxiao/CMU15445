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

#include "common/config.h"
#include "concurrency/transaction.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/page.h"

namespace bustub {

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

/**
 * Main class providing the API for the Interactive B+ Tree.
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
  using BasicPage = BPlusTreePage;
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  enum class LatchMode { READ, INSERT, DELETE, OPTIMIZE };

  explicit BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                     int leaf_max_size = LEAF_PAGE_SIZE, int internal_max_size = INTERNAL_PAGE_SIZE);

  void PrintSize() { printf("%d %d\n", leaf_max_size_, internal_max_size_); }

  // Returns true if this B+ tree has no keys and values.
  auto IsEmpty() const -> bool;

  // 创造一个叶子结点
  auto CreateLeafNode() -> LeafPage *;

  // 创造一个内部结点
  auto CreateInternalNode() -> InternalPage *;

  // 初始化一个B+树
  void InitBPlusTree(const KeyType &key, const ValueType &value);

  // 辅助Insert操作
  auto InsertHelper(const KeyType &key, const ValueType &value, Transaction *transaction, LatchMode mode) -> bool;

  // 将新得到的内部结点的所有孩子的parent指向这个节点
  void SetChildParentID(InternalPage *internal_page_ptr);

  // 分裂操作，向父节点中插入一个key
  void InsertInParent(BPlusTreePage *left_page, BPlusTreePage *right_page, KeyType &key);

  // Insert a key-value pair into this B+ tree.
  auto Insert(const KeyType &key, const ValueType &value, Transaction *transaction = nullptr) -> bool;

  auto RemoveImplement(BasicPage *page_ptr, const KeyType &key) -> bool;

  // 将该节点中的key删掉，并进行后续可能的重新分配，合并工作
  void RemoveEntry(BasicPage *page_ptr, const KeyType &key);

  // 辅助Remove操作
  void RemoveHelper(const KeyType &key, Transaction *transaction, LatchMode mode);

  // 完成merge操作
  void Merge(InternalPage *parent_page_ptr, BasicPage *base_page, BasicPage *brother_page, int index,
             bool brother_on_left);

  // 尝试去和兄弟结点合并，如果成功了，则返回true，否则false
  auto TryMerge(BasicPage *page_ptr, const KeyType &key) -> bool;

  // 将所有孩子的父节点置为自己
  void ReFreshAllChildParent(InternalPage *base_ptr);

  // 将当前页面的第index个孩子的parent置为自己
  void ReFreshParent(InternalPage *base_ptr, int index);

  // 从右兄弟借一个结点给自己
  void Redistribute(InternalPage *parent_page_ptr, BasicPage *base_page, BasicPage *brother_page, int index,
                    bool brother_on_left);

  // 尝试去向兄弟结点借，如果成功了，则返回true，否则false
  auto TryRedistribute(BasicPage *page_ptr, const KeyType &key) -> bool;

  // Remove a key and its value from this B+ tree.
  void Remove(const KeyType &key, Transaction *transaction = nullptr);

  // 将基本的页面类型，强制转为对应的页面类型
  auto ReInterpretAsInternalPage(BasicPage *basicpage) -> InternalPage * {
    return reinterpret_cast<InternalPage *>(basicpage);
  }
  auto ReInterpretAsLeafPage(BasicPage *basicpage) -> LeafPage * { return reinterpret_cast<LeafPage *>(basicpage); }

  auto IsSafe(BasicPage *tree_page_ptr, LatchMode mode) -> bool;

  // 根据页面id，从缓存池管理器中，取出B+树的结点（这个结点的载体是一个B+树类型的页面）
  auto FetchBPlusTreePage(page_id_t page_id) -> std::pair<Page *, BasicPage *>;

  // 寻找key对应的叶子结点所在
  auto FindLeafPage(const KeyType &key, Transaction *transaction = nullptr, LatchMode mode = LatchMode::READ)
      -> std::pair<Page *, LeafPage *>;

  // return the value associated with a given key
  auto GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction = nullptr) -> bool;

  // return the page id of the root node
  auto GetRootPageId() -> page_id_t;

  // index iterator
  auto Begin() -> INDEXITERATOR_TYPE;
  auto Begin(const KeyType &key) -> INDEXITERATOR_TYPE;
  auto End() -> INDEXITERATOR_TYPE;

  void LatchRootPageID(Transaction *transaction, LatchMode mode);
  void RealseAllLatches(Transaction *transaction, LatchMode mode, int dirty_height = 0);

  // print the B+ tree
  void Print(BufferPoolManager *bpm);

  // draw the B+ tree
  void Draw(BufferPoolManager *bpm, const std::string &outf);

  // read data from file and insert one by one
  void InsertFromFile(const std::string &file_name, Transaction *transaction = nullptr);

  // read data from file and remove one by one
  void RemoveFromFile(const std::string &file_name, Transaction *transaction = nullptr);

 private:
  void UpdateRootPageId(int insert_record = 0);

  /* Debug Routines for FREE!! */
  void ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const;

  void ToString(BPlusTreePage *page, BufferPoolManager *bpm) const;

  // member variable
  std::string index_name_;
  page_id_t root_page_id_;
  BufferPoolManager *buffer_pool_manager_;
  KeyComparator comparator_;
  int leaf_max_size_;
  int internal_max_size_;
  bool no_root_{true};
  ReaderWriterLatch root_id_rwlatch_;
};

}  // namespace bustub
