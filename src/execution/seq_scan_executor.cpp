//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include "concurrency/transaction.h"

namespace bustub {
// 初始化plan
// 初始化两个指针
// 初始化谓词
SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      end_(exec_ctx->GetCatalog()->GetTable(plan->table_oid_)->table_->End()),
      cursor_(end_),
      fileter_predicate_(plan->filter_predicate_) {}

void SeqScanExecutor::Init() {
  tsn_ = exec_ctx_->GetTransaction();
  cur_table_id_ = plan_->GetTableOid();
  cursor_ = exec_ctx_->GetCatalog()->GetTable(cur_table_id_)->table_->Begin(tsn_);
  table_name_ = exec_ctx_->GetCatalog()->GetTable(cur_table_id_)->name_;
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // 如果已经没有next了
  if (cursor_ == end_) {
    return false;
  }
  while (cursor_ != end_) {
    // 如果没有筛选谓词
    *rid = cursor_->GetRid();
    if (fileter_predicate_ == nullptr) {
      *tuple = *cursor_;
      cursor_++;
      return true;
    }
    // 如果有筛选谓词
    auto evaluate_res = fileter_predicate_->Evaluate(&(*(cursor_)), plan_->OutputSchema());
    if (!evaluate_res.IsNull() && evaluate_res.GetAs<bool>()) {
      *tuple = *cursor_;
      cursor_++;
      return true;
    }
    cursor_++;
  }
  return false;
}

}  // namespace bustub
