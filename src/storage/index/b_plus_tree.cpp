#include <iostream>
#include <ostream>
#include <string>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
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
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FetchBPlusTreePage(page_id_t page_id) -> BasicPage * {
  Page *raw_root_page_ptr = buffer_pool_manager_->FetchPage(page_id);
  return reinterpret_cast<BasicPage *>(raw_root_page_ptr->GetData());
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key) -> LeafPage * {
  // 首先得到B+树的根节点
  BasicPage *cur_page_ptr = FetchBPlusTreePage(root_page_id_);
  //从根节点往下找
  // 如果当前节点不是叶节点，则不断地去向下搜索
  while (!cur_page_ptr->IsLeafPage()) {
    // 先将当前节点转为一个内部结点
    auto cur_page_internal = ReInterpretAsInternalPage(cur_page_ptr);
    // 去这个内部结点里找到对应的位置，从而找到该去哪个儿子结点
    page_id_t next_page_id = cur_page_internal->SearchNextChildPageID(key, comparator_);
    // 当前结点页面已经不需要了，记得记得unpin
    buffer_pool_manager_->UnpinPage(cur_page_ptr->GetPageId(), false);
    // 根据页面id，取对应的页面指针
    cur_page_ptr = FetchBPlusTreePage(next_page_id);
  }
  // 将基本的页面指正强转为leaf并返回该叶节点
  return ReInterpretAsLeafPage(cur_page_ptr);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  // 不能是空树
  if (IsEmpty()) {
    return false;
  }
  // 找到对应的叶子结点
  LeafPage *leaf_node_ptr = FindLeafPage(key);
  // std::cout<<leaf_node_ptr->GetPageId()<<std::endl;
  // 在对应的叶子结点里查找是否存在key
  int index = leaf_node_ptr->SearchKeyPos(key, comparator_);
  // index==-1代表没有找到
  if (index == -1) {
    buffer_pool_manager_->UnpinPage(leaf_node_ptr->GetPageId(), false);
    return false;
  }
  result->push_back(leaf_node_ptr->ValueAt(index));
  buffer_pool_manager_->UnpinPage(leaf_node_ptr->GetPageId(), false);
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
auto BPLUSTREE_TYPE::CreateInternalNode() -> InternalPage * {
  page_id_t internal_node_page_id;
  Page *page_ptr = buffer_pool_manager_->NewPage(&internal_node_page_id);
  auto internal_node_page_ptr = reinterpret_cast<InternalPage *>(page_ptr->GetData());
  internal_node_page_ptr->Init(internal_node_page_id, INVALID_PAGE_ID, internal_max_size_);
  return internal_node_page_ptr;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::CreateLeafNode() -> LeafPage * {
  // 首先去缓存池申请一块新的页面
  page_id_t leaf_node_page_id;
  Page *page_ptr = buffer_pool_manager_->NewPage(&leaf_node_page_id);
  // 将这个页面变成叶节点页面
  auto leaf_node_page_ptr = reinterpret_cast<LeafPage *>(page_ptr->GetData());
  // 初始化这个叶节点
  leaf_node_page_ptr->Init(leaf_node_page_id, INVALID_PAGE_ID, leaf_max_size_);
  // 返回这个叶结点指针
  return leaf_node_page_ptr;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InitBPlusTree(const KeyType &key, const ValueType &value) {
  // 先获得一个叶节点，这个叶节点还没有任何实际的数据
  LeafPage *leaf_node = CreateLeafNode();
  root_page_id_ = leaf_node->GetPageId();
  // 系统要求的，要使用系统API去更新叶节点
  UpdateRootPageId(no_root_);
  no_root_ = false;
  // 向叶节点中插入实际的数据
  leaf_node->Insert(key, value, comparator_);
  // 页面操作完了，需要unpin
  buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SetChildParentID(InternalPage *internal_page_ptr) {
  for (int i = 0; i < internal_page_ptr->GetSize(); i++) {
    BasicPage *child_ptr = FetchBPlusTreePage(internal_page_ptr->ValueAt(i));
    child_ptr->SetParentPageId(internal_page_ptr->GetPageId());
    buffer_pool_manager_->UnpinPage(child_ptr->GetPageId(), true);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInParent(BPlusTreePage *left_page, BPlusTreePage *right_page, KeyType &key) {
  // 如果left_page是根节点
  if (left_page->GetPageId() == root_page_id_) {
    // 首先新建一个内部结点，用这个内部结点当做新的根节点
    InternalPage *new_root_internal_node_ptr = CreateInternalNode();
    root_page_id_ = new_root_internal_node_ptr->GetPageId();
    // 调用系统要求的那个api，更新根节点的值
    UpdateRootPageId(false);
    // 设置内部结点的值，主要是一个key，和左右两边的child_page_id
    new_root_internal_node_ptr->SetKeyAt(1, key);
    // std::cout<<key<<std::endl;
    new_root_internal_node_ptr->SetValueAt(0, left_page->GetPageId());
    new_root_internal_node_ptr->SetValueAt(1, right_page->GetPageId());
    new_root_internal_node_ptr->IncreaseSize(1);
    // 设置left_page和right_page的父节点的值
    left_page->SetParentPageId(root_page_id_);
    right_page->SetParentPageId(root_page_id_);
    // unpin这个父节点，返回
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    return;
  }
  // 如果left_page不是根节点，则取出父节点的ptr
  InternalPage *parent_internal_node_ptr = ReInterpretAsInternalPage(FetchBPlusTreePage(left_page->GetParentPageId()));
  // 如果父节点没有满，则直接插入
  if (parent_internal_node_ptr->GetSize() != parent_internal_node_ptr->GetMaxSize()) {
    parent_internal_node_ptr->Insert(key, right_page->GetPageId(), comparator_);
  } else {
    // 如果父节点满了，则先创建一个内部结点
    InternalPage *new_parent_internal_node_ptr = CreateInternalNode();
    // 将父节点后一半的内容都挪到新的内部兄弟结点中
    parent_internal_node_ptr->MoveLatterHalfTo(new_parent_internal_node_ptr, key, right_page->GetPageId(), comparator_);
    // 更新新的内部结点的所有child_page的parent
    SetChildParentID(new_parent_internal_node_ptr);
    // 将新的内部结点的第一个key继续推上去，即递归地往上推
    KeyType new_pushup_key = new_parent_internal_node_ptr->KeyAt(0);
    InsertInParent(parent_internal_node_ptr, new_parent_internal_node_ptr, new_pushup_key);
    // unpin新的内部结点
    buffer_pool_manager_->UnpinPage(new_parent_internal_node_ptr->GetPageId(), true);
  }
  // unpin父节点
  buffer_pool_manager_->UnpinPage(parent_internal_node_ptr->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertHelper(const KeyType &key, const ValueType &value) -> bool {
  // 如果当前的B+树为空
  if (IsEmpty()) {
    InitBPlusTree(key, value);
    return true;
  }
  // 如果当前的B+树不为空
  // 先找到这个key应该存在的叶节点
  LeafPage *leaf_node_ptr = FindLeafPage(key);
  // 插入之后，如果这个key已经重复了，则直接return false
  bool no_duplicate = leaf_node_ptr->Insert(key, value, comparator_);
  if (!no_duplicate) {
    buffer_pool_manager_->UnpinPage(leaf_node_ptr->GetPageId(), false);
    return false;
  }
  // 插入之后，如果已经满了，那就要进行分裂操作
  if (leaf_node_ptr->GetSize() == leaf_node_ptr->GetMaxSize()) {
    // 创建一个新的叶节点
    LeafPage *new_leaf_node_ptr = CreateLeafNode();
    // 将当前节点的右半部分的值都放到新的叶节点中，并设置新节点的父节点
    leaf_node_ptr->MoveLatterHalfTo(new_leaf_node_ptr);
    // std::cout<<leaf_node_ptr->GetSize()<<std::endl;
    // std::cout<<new_leaf_node_ptr->GetSize()<<std::endl;
    // 将新节点的第一个key推上父节点
    KeyType insertkey = new_leaf_node_ptr->KeyAt(0);
    InsertInParent(leaf_node_ptr, new_leaf_node_ptr, insertkey);
    // unpin新节点
    buffer_pool_manager_->UnpinPage(new_leaf_node_ptr->GetPageId(), true);
  }
  buffer_pool_manager_->UnpinPage(leaf_node_ptr->GetPageId(), true);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  return InsertHelper(key, value);
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
auto BPLUSTREE_TYPE::RemoveImplement(BasicPage *page_ptr, const KeyType &key) -> bool {
  // 如果当前节点是叶节点，那么首先需要删除叶节点对应的key
  if (page_ptr->IsLeafPage()) {
    return ReInterpretAsLeafPage(page_ptr)->Remove(key, comparator_);
  }
  // 如果不是叶节点，则删除内部节点中的key
  return ReInterpretAsInternalPage(page_ptr)->Remove(key, comparator_);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReFreshParent(InternalPage *base_ptr, int index) {
  page_id_t child_page_id = base_ptr->ValueAt(index);
  BasicPage *child_ptr = FetchBPlusTreePage(child_page_id);
  child_ptr->SetParentPageId(base_ptr->GetPageId());
  buffer_pool_manager_->UnpinPage(child_page_id, true);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Redistribute(InternalPage *parent_page_ptr, BasicPage *base_page, BasicPage *brother_page,
                                  int index, bool brother_on_left) {
  // 如果当前节点是叶节点
  if (base_page->IsLeafPage()) {
    LeafPage *base_leaf_page = ReInterpretAsLeafPage(base_page);
    LeafPage *brother_leaf_page = ReInterpretAsLeafPage(brother_page);
    // 如果当前的兄弟是在左边
    if (brother_on_left) {
      brother_leaf_page->MoveLastToFirstof(base_leaf_page);
      parent_page_ptr->SetKeyAt(index, base_leaf_page->KeyAt(0));
    } else {
      // 如果当前的兄弟是在右边
      // 先把右边兄弟的第一个KV挪到自己的最后面，并更新索引的值
      brother_leaf_page->MoveFirstToEndof(base_leaf_page);
      parent_page_ptr->SetKeyAt(index + 1, brother_leaf_page->KeyAt(0));
    }
  } else {
    //如果当前节点不是叶节点
    InternalPage *base_internal_page = ReInterpretAsInternalPage(base_page);
    InternalPage *brother_internal_page = ReInterpretAsInternalPage(brother_page);
    // 如果当前的兄弟是在左边
    if (brother_on_left) {
      brother_internal_page->MoveLastToFirstof(base_internal_page);
      ReFreshParent(base_internal_page, 0);
      parent_page_ptr->SetKeyAt(index, base_internal_page->KeyAt(0));
    } else {
      // 如果当前的兄弟是在右边
      // 首先把右边兄弟的第一个挪到左边
      brother_internal_page->MoveFirstToEndof(base_internal_page);
      // 更新移过来的page的parent_id
      ReFreshParent(base_internal_page, base_internal_page->GetSize() - 1);
      // 更新当前节点的key
      base_internal_page->SetKeyAt(base_internal_page->GetSize() - 1, parent_page_ptr->KeyAt(index + 1));
      // 更新父节点的key
      parent_page_ptr->SetKeyAt(index + 1, brother_internal_page->KeyAt(0));
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::TryRedistribute(BasicPage *page_ptr, const KeyType &key) -> bool {
  // 如果右侧还有叶节点，并且可以借，则从右边借
  // 首先需要知道当前这个page_ptr在父节点的哪个位置，因为这个page包含了key，所以直接在父节点中查找key的index
  page_id_t parent_page_id = page_ptr->GetParentPageId();
  InternalPage *parent_page_ptr = ReInterpretAsInternalPage(FetchBPlusTreePage(parent_page_id));
  int index = parent_page_ptr->SearchIndex(key, comparator_);
  // 如果这个index<size-1，那么说明有右兄弟，取出右兄弟看看右兄弟能不能借
  if (index < parent_page_ptr->GetSize() - 1) {
    page_id_t right_brother_page_id = parent_page_ptr->ValueAt(index + 1);
    BasicPage *right_brother_page_ptr = FetchBPlusTreePage(right_brother_page_id);
    // 如果右兄弟的数量足够，可以借
    if (right_brother_page_ptr->GetSize() > right_brother_page_ptr->GetMinSize()) {
      Redistribute(parent_page_ptr, page_ptr, right_brother_page_ptr, index, false);
      buffer_pool_manager_->UnpinPage(parent_page_id, true);
      buffer_pool_manager_->UnpinPage(right_brother_page_id, true);
      return true;
    }
  }
  // 如果左侧还有叶节点，并且可以借，则从左边借
  if (index > 0) {
    page_id_t left_brother_page_id = parent_page_ptr->ValueAt(index - 1);
    BasicPage *left_brother_page_ptr = FetchBPlusTreePage(left_brother_page_id);
    if (left_brother_page_ptr->GetSize() > left_brother_page_ptr->GetMinSize()) {
      Redistribute(parent_page_ptr, page_ptr, left_brother_page_ptr, index, true);
      buffer_pool_manager_->UnpinPage(parent_page_id, true);
      buffer_pool_manager_->UnpinPage(left_brother_page_id, true);
      return true;
    }
  }
  buffer_pool_manager_->UnpinPage(parent_page_id, false);
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReFreshAllChildParent(InternalPage *base_ptr)
{
  for(int i=0;i<base_ptr->GetSize();i++){
    page_id_t child_id=base_ptr->ValueAt(i);
    InternalPage* child_ptr=ReInterpretAsInternalPage(FetchBPlusTreePage(child_id));
    child_ptr->SetParentPageId(base_ptr->GetPageId());
    buffer_pool_manager_->UnpinPage(child_id, true);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Merge(InternalPage *parent_page_ptr, BasicPage *base_page, BasicPage *brother_page, int index,
                           bool brother_on_left) {
  // 如果当前节点是叶节点
  if (base_page->IsLeafPage()) {
    LeafPage *base_leaf_page = ReInterpretAsLeafPage(base_page);
    LeafPage *brother_leaf_page = ReInterpretAsLeafPage(brother_page);
    // 如果兄弟结点在左边
    if (brother_on_left) {
      KeyType need_remove_key = parent_page_ptr->KeyAt(index);
      base_leaf_page->MoveAllTo(brother_leaf_page);
      brother_leaf_page->SetNextPageId(base_leaf_page->GetNextPageId());
      base_leaf_page->SetParentPageId(INVALID_PAGE_ID);
      RemoveEntry(parent_page_ptr, need_remove_key);
    } else {
      // 如果兄弟结点在右边
      // 先挪东西
      KeyType need_remove_key = parent_page_ptr->KeyAt(index + 1);
      brother_leaf_page->MoveAllTo(base_leaf_page);
      // 更新当前节点的next_page
      base_leaf_page->SetNextPageId(brother_leaf_page->GetNextPageId());
      brother_leaf_page->SetParentPageId(INVALID_PAGE_ID);
      // 在parent中删除brother对应的那个KV
      RemoveEntry(parent_page_ptr, need_remove_key);
    }
  } else {
    // 如果当前节点是内部节点
    InternalPage* base_internal_page=ReInterpretAsInternalPage(base_page);
    InternalPage* brother_internal_page=ReInterpretAsInternalPage(brother_page);
    // 如果兄弟结点在左边
    if (brother_on_left) {
      KeyType need_remove_key=parent_page_ptr->KeyAt(index);
      int old_size=brother_internal_page->GetSize();
      base_internal_page->MoveAllTo(brother_internal_page);
      ReFreshAllChildParent(brother_internal_page);
      brother_internal_page->SetKeyAt(old_size, need_remove_key);
      base_internal_page->SetParentPageId(INVALID_PAGE_ID);
      RemoveEntry(parent_page_ptr, need_remove_key);
    } else {
      // 如果兄弟结点在右边
      // 先挪东西
      KeyType need_remove_key=parent_page_ptr->KeyAt(index+1);
      int old_size=base_internal_page->GetSize();
      brother_internal_page->MoveAllTo(base_internal_page);
      // 更新新孩子的指针（暴力做法，更新所有孩子的）
      ReFreshAllChildParent(base_internal_page);
      // 将当前节点新增的第一节结点的key置为原来的key，因为有的第一个KV值是没有key的
      base_internal_page->SetKeyAt(old_size, need_remove_key);
      // 将brother页面丢弃
      brother_internal_page->SetParentPageId(INVALID_PAGE_ID);
      // 移出父节点指向brother页面的KV
      RemoveEntry(parent_page_ptr, need_remove_key);
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::TryMerge(BasicPage *page_ptr, const KeyType &key) -> bool {
  // 首先取出parent的id和ptr
  page_id_t parent_page_id = page_ptr->GetParentPageId();
  InternalPage *parent_page_ptr = ReInterpretAsInternalPage(FetchBPlusTreePage(parent_page_id));
  // 找出当前页面在父节点中的index
  int index = parent_page_ptr->SearchIndex(key, comparator_);
  // 如果当前节点有右兄弟，则尝试向右兄弟去合并
  if (index < parent_page_ptr->GetSize() - 1) {
    page_id_t right_brother_id = parent_page_ptr->ValueAt(index + 1);
    BasicPage *right_brother_ptr = FetchBPlusTreePage(right_brother_id);
    Merge(parent_page_ptr, page_ptr, right_brother_ptr, index, false);
    buffer_pool_manager_->UnpinPage(parent_page_id, true);
    buffer_pool_manager_->UnpinPage(right_brother_id, true);
    return true;
  }
  // 没有右兄弟，只能向做兄弟去合并
  if (index > 0) {
    page_id_t left_brother_id = parent_page_ptr->ValueAt(index - 1);
    BasicPage *left_brother_ptr = FetchBPlusTreePage(left_brother_id);
    Merge(parent_page_ptr, page_ptr, left_brother_ptr, index, true);
    buffer_pool_manager_->UnpinPage(parent_page_id, true);
    buffer_pool_manager_->UnpinPage(left_brother_id, true);
    return true;
  }
  buffer_pool_manager_->UnpinPage(parent_page_id, false);
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveEntry(BasicPage *page_ptr, const KeyType &key) {
  // 留坑：这可能是一种偷懒的写法，因为并没有删除内部节点中与叶节点对应的结点，
  // 首先，尝试删除，如果删除成功，则返回true，否则false
  bool remove_success = RemoveImplement(page_ptr, key);
  if (!remove_success) {
    return;
  }
  // 如果删除之后，这个叶节点的没有到达最低临界值，则直接返回
  if (page_ptr->GetSize() >= page_ptr->GetMinSize()) {
    return;
  }
  // 如果删除之后，这个叶节点到达最低临界值，需要向兄弟借，或者和兄弟合并。需要分类讨论。
  // 如果这个节点是根节点，说明它没得借，它既没有parent，也没有brother
  if (page_ptr->IsRootPage()) {
    // 如果这个节点是一个叶节点，只要没有被删光，那就没事，如果被删光了，说明这棵树就没了
    if (page_ptr->IsLeafPage()) {
      if (page_ptr->GetSize() == 0) {
        root_page_id_ = INVALID_PAGE_ID;
        UpdateRootPageId(false);
      }
    } else {
      // 如果这个节点是一个内部结点，除非被删到了一个key都没有，也就是size==1了，否则都不用管
      if (page_ptr->GetSize() == 1) {
        // 代表着当前这棵树的根节点，只有一个child_page了，那么其实直接将这个child_page置为根节点就行
        // 首先将当前的ptr转为内部节点ptr
        InternalPage *internal_page_ptr = ReInterpretAsInternalPage(page_ptr);
        // 取出child_page_id
        page_id_t child_page_id = internal_page_ptr->ValueAt(0);
        // 将child_page置为根节点
        root_page_id_ = child_page_id;
        UpdateRootPageId(false);
        // 将child_page的父节点置为invalid，注意了，更改内部节点的entry时，要记得更新这个entry的parent_id
        BasicPage *child_ptr = FetchBPlusTreePage(root_page_id_);
        child_ptr->SetParentPageId(INVALID_PAGE_ID);
        buffer_pool_manager_->UnpinPage(child_ptr->GetPageId(), true);
      }
    }
  } else {
    // 如果这个节点不是根节点，则说明可能可以去借
    // 尝试去向兄弟借
    bool try_distrubute = TryRedistribute(page_ptr, key);
    // 如果借不到，那么尝试和兄弟去合并
    if (!try_distrubute) {
      // 如果执行了try_merge，那么肯定是要成功的
      TryMerge(page_ptr, key);
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveHelper(const KeyType &key, Transaction *transaction) {
  // 如果当前这棵树为空，则直接返回
  if (IsEmpty()) {
    return;
  }
  // 如果这棵树不空，则找到对应的叶节点，调用函数将其删除
  LeafPage *leaf_page_ptr = FindLeafPage(key);
  RemoveEntry(leaf_page_ptr, key);
  // 留坑：大佬好像忘记unpin了
  buffer_pool_manager_->UnpinPage(leaf_page_ptr->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) { RemoveHelper(key, transaction); }

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  // 如果当前是空的，需要特判
  if (IsEmpty()) {
    return End();
  }
  // 取出对应的叶节点页面的指针
  LeafPage *cur_page_ptr = FindLeafPage(KeyType{});
  // 构造一个迭代器，并返回
  return INDEXITERATOR_TYPE(cur_page_ptr, cur_page_ptr->GetPageId(), 0, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  // 首先取到这个key对应的叶子结点
  LeafPage *cur_page_ptr = FindLeafPage(KeyType{});
  // 在叶子结点中，找到这个key对应的index，这个index大于等于当前的key就行了
  int index = cur_page_ptr->SearchBigOrEqualPos(key, comparator_);
  // 如果这个index存在，则直接返回答案
  if (index != -1) {
    // for(int i=0;i<cur_page_ptr->GetSize();i++)
    // {
    //   std::cout<<cur_page_ptr->KeyAt(i)<<std::endl;
    // }
    // std::cout<<index<<std::endl;

    return INDEXITERATOR_TYPE(cur_page_ptr, cur_page_ptr->GetPageId(), index, buffer_pool_manager_);
  }
  // 如果这个index不存在，则需要取下一个page
  page_id_t next_page_id = cur_page_ptr->GetNextPageId();
  buffer_pool_manager_->UnpinPage(cur_page_ptr->GetPageId(), false);
  if (next_page_id == INVALID_PAGE_ID) {
    return End();
  }
  LeafPage *next_page_ptr = ReInterpretAsLeafPage(FetchBPlusTreePage(next_page_id));
  return INDEXITERATOR_TYPE(next_page_ptr, next_page_id, 0, buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  return INDEXITERATOR_TYPE(nullptr, INVALID_PAGE_ID, -1, buffer_pool_manager_);
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
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
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
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
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
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
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
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
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
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
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
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

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
