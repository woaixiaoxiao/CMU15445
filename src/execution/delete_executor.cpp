//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdint>
#include <memory>
#include <vector>

#include "catalog/column.h"
#include "catalog/schema.h"
#include "concurrency/transaction.h"
#include "execution/executors/delete_executor.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type.h"
#include "type/type_id.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  delete_finished_ = false;
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (delete_finished_) {
    return false;
  }
  // 提前取出事务
  Transaction *txn = exec_ctx_->GetTransaction();
  if (!txn->IsTableIntentionExclusiveLocked(plan_->table_oid_)) {
    bool succ =
        exec_ctx_->GetLockManager()->LockTable(txn, LockManager::LockMode::INTENTION_EXCLUSIVE, plan_->table_oid_);
    if (!succ) {
      txn->SetState(TransactionState::ABORTED);
      throw bustub::Exception(ExceptionType::EXECUTION, "InsertExecutor cannot get IX lock on table");
    }
  }
  // 提前记录下当前这个表的指针，方便
  std::string table_name = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)->name_;
  TableHeap *table_ptr = exec_ctx_->GetCatalog()->GetTable(table_name)->table_.get();
  // 取出所有的索引
  auto table_index = exec_ctx_->GetCatalog()->GetTableIndexes(table_name);
  // 子tuple
  Tuple child_tuple;
  // 已经删除的数量
  int64_t count = 0;
  // 不断去取tuple，然后删除
  while (true) {
    // 首先从child算子中取出一个tuple
    bool fetch_success = child_executor_->Next(&child_tuple, rid);
    if (!fetch_success) {
      break;
    }
    // 获取删除的权限
    if (!txn->IsRowExclusiveLocked(plan_->TableOid(), child_tuple.GetRid())) {
      exec_ctx_->GetLockManager()->LockRow(txn, LockManager::LockMode::EXCLUSIVE, plan_->TableOid(),
                                           child_tuple.GetRid());
    }
    // 尝试去删除
    bool delete_success = table_ptr->MarkDelete(child_tuple.GetRid(), txn);
    if (delete_success) {
      count++;
    }
    // 更新所有的索引
    for (auto &it : table_index) {
      auto key_schema = it->index_->GetKeySchema();
      auto key_attrs = it->index_->GetKeyAttrs();
      auto new_key = child_tuple.KeyFromTuple(child_executor_->GetOutputSchema(), *key_schema, key_attrs);
      it->index_->DeleteEntry(new_key, child_tuple.GetRid(), txn);
    }
  }
  // 构造返回值
  auto return_value = std::vector<Value>{{TypeId::BIGINT, count}};
  auto return_schema = Schema(std::vector<Column>{{"success_delete_count", TypeId::BIGINT}});
  *tuple = Tuple(return_value, &return_schema);
  delete_finished_ = true;
  return true;
}

}  // namespace bustub
