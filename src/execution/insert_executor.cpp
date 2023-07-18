//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdint>
#include <memory>
#include <vector>

#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "execution/executors/insert_executor.h"
#include "execution/executors/values_executor.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type.h"
#include "type/type_id.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  insert_finished_ = false;
  child_executor_->Init();
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  // 如果插入已经结束了，则直接return false
  if (insert_finished_) {
    return false;
  }
  Transaction *transaction = exec_ctx_->GetTransaction();
  // 创建一个tuple对象，用于从child算子接受tuple
  Tuple child_tuple{};
  // 创建一个count，记录插入了多少条数据
  int64_t count = 0;
  // 先获得当前需要操作的表的指针，方便后续的操作
  TableHeap *table_ptr = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)->table_.get();
  // 先将指向索引的东西取出来，省的后面麻烦
  std::string table_name = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)->name_;
  std::vector<IndexInfo *> table_index = exec_ctx_->GetCatalog()->GetTableIndexes(table_name);
  // 不断接受tuple对象，直到接受失败，则跳出循环
  while (true) {
    // 取出下一个tuple
    bool fetch_res = child_executor_->Next(&child_tuple, rid);
    // 若取出失败，则说明插入全部完成了
    if (!fetch_res) {
      break;
    }
    // 插入
    RID new_insert_rid;
    bool insert_res = table_ptr->InsertTuple(child_tuple, &new_insert_rid, transaction);
    if (insert_res) {
      count++;
    }
    // 更新这个表的所有索引
    for (const auto &it : table_index) {
      // 取出这个索引的模式和属性
      auto key_schema = it->index_->GetKeySchema();
      auto key_attrs = it->index_->GetKeyAttrs();
      // 生成新的tuple的索引key
      auto new_key = child_tuple.KeyFromTuple(child_executor_->GetOutputSchema(), *key_schema, key_attrs);
      // 将新的索引key值插入
      it->index_->InsertEntry(new_key, new_insert_rid, transaction);
    }
  }
  // 做出返回值，包括记录当前插入已经结束
  auto return_value = std::vector<Value>{{TypeId::BIGINT, count}};
  auto return_schema = Schema(std::vector<Column>{{"success_insert_count", TypeId::BIGINT}});
  *tuple = Tuple(return_value, &return_schema);
  insert_finished_ = true;
  return true;
}

}  // namespace bustub
