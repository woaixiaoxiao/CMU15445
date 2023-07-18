//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "catalog/column.h"
#include "type/type.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      inner_schema_(plan_->InnerTableSchema()),
      outer_schema_(plan_->GetChildPlan()->OutputSchema()),
      key_schema_(std::vector<Column>{{"index_key", plan->KeyPredicate()->GetReturnType()}}) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestIndexJoinExecutor::Init() {
  child_executor_->Init();
  index_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  match_rids_.clear();
  inner_table_ptr_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid());
}

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (true) {
    // 首先取一个outer表的tuple
    bool status = child_executor_->Next(&outer_tuple_, &outer_rid_);
    // outer表已经被取完了
    if (!status) {
      return false;
    }
    // 构建出index，然后去根据index去查找
    std::vector<Value> key_value{plan_->KeyPredicate()->Evaluate(&outer_tuple_, outer_schema_)};
    Tuple key_index = Tuple(key_value, &key_schema_);
    index_->index_->ScanKey(key_index, &match_rids_, exec_ctx_->GetTransaction());
    // 如果查找成功
    if (!match_rids_.empty()) {
      // 只拿最后一个
      inner_rid_ = match_rids_.back();
      match_rids_.clear();
      inner_table_ptr_->table_->GetTuple(inner_rid_, &inner_tuple_, exec_ctx_->GetTransaction());
      std::vector<Value> value;
      MergeValueFromTuple(value, false);
      *tuple = Tuple(value, &GetOutputSchema());
      return true;
    }
    // 查找失败，但是如果是left joint，则可以返回null
    if (plan_->GetJoinType() == JoinType::LEFT) {
      std::vector<Value> value;
      MergeValueFromTuple(value, true);
      *tuple = Tuple(value, &GetOutputSchema());
      return true;
    }
  }
  return false;
}

}  // namespace bustub
