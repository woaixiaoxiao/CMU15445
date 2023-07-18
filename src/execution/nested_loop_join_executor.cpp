//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/type.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  // 逐个初始化成员变量
  left_executor_->Init();
  right_executor_->Init();
  left_tuple_ = Tuple{};
  right_tuple_ = Tuple{};
  RID rid_holder;
  left_status_ = left_executor_->Next(&left_tuple_, &rid_holder);
  right_status_ = false;
  left_join_found_ = false;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  RID rid_holder;
  while (true) {
    // 首先取得一个右边的tuple
    right_status_ = right_executor_->Next(&right_tuple_, &rid_holder);
    // 如果右边已经到了end
    if (!right_status_) {
      // 如果是左连接，并且没有找到的话
      if (plan_->GetJoinType() == JoinType::LEFT && !left_join_found_) {
        std::vector<Value> value;
        MergeValueFromTuple(value, true);
        *tuple = Tuple(value, &plan_->OutputSchema());
        left_join_found_ = true;
        return true;
      }
      // 如果之前不是左链接，或者说是左连接但是已经找到了，那么就要更新左边的tuple了
      left_status_ = left_executor_->Next(&left_tuple_, &rid_holder);
      left_join_found_ = false;
      // 已经到了左边表的end了，直接return
      if (!left_status_) {
        return false;
      }
      // 左边的tuple更新之后，右边的tuple就要从头开始访问了
      right_executor_->Init();
      right_status_ = right_executor_->Next(&right_tuple_, &rid_holder);
      // 右边是空表，但是如果是左连接的话，还是需要访问，即左连接一定会出现左边的表的所有tuple
      if (!right_status_) {
        continue;
      }
    }
    // 获取匹配的结果
    Value pred_value = plan_->Predicate().EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple_,
                                                       right_executor_->GetOutputSchema());
    // 如果匹配成功
    if (!pred_value.IsNull() && pred_value.GetAs<bool>()) {
      // 将这两个tuple给放入一个vector，再用这个vector初始化返回的tuple
      std::vector<Value> value;
      MergeValueFromTuple(value, false);
      *tuple = Tuple(value, &plan_->OutputSchema());
      left_join_found_ = true;
      return true;
    }
  }
  return false;
}

}  // namespace bustub
