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
#include "common/exception.h"
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
  // 需要对行加上读锁，那么只有在commited或者repeated的情况下才需要
  bool suc1 = (tsn_->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) ||
              (tsn_->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ);
  // 对行加上读锁，只有在当前表还没有对这个行加上意向锁时，才需要去申请意向锁，可以是读意向锁，也可以是写意向锁
  bool suc2 =
      (!tsn_->IsTableIntentionSharedLocked(cur_table_id_)) && (!tsn_->IsTableIntentionExclusiveLocked(cur_table_id_));
  // 只有两个条件都成立时，才需要去申请这个表的意向读锁
  if (suc1 && suc2) {
    // 尝试加锁
    bool table_success =
        exec_ctx_->GetLockManager()->LockTable(tsn_, LockManager::LockMode::INTENTION_SHARED, cur_table_id_);
    // 如果加锁失败，则将事务置为aborted，并抛出异常
    if (!table_success) {
      tsn_->SetState(TransactionState::ABORTED);
      throw bustub::Exception(ExceptionType::EXECUTION, "SeqScan cannot get IS lock on table");
    }
  }
  cursor_ = exec_ctx_->GetCatalog()->GetTable(cur_table_id_)->table_->Begin(tsn_);
  table_name_ = exec_ctx_->GetCatalog()->GetTable(cur_table_id_)->name_;
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // 如果已经没有next了
  if (cursor_ == end_) {
    return false;
  }
  bool obtain_lock = false;
  while (cursor_ != end_) {
    // 如果没有筛选谓词
    *rid = cursor_->GetRid();
    if (fileter_predicate_ == nullptr) {
      // 在去读这一行之前，先获取锁
      if (((tsn_->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) ||
           (tsn_->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ)) &&
          (!tsn_->IsRowSharedLocked(cur_table_id_, *rid) && !tsn_->IsRowExclusiveLocked(cur_table_id_, *rid))) {
        // 如果满足条件，那么就获得锁
        exec_ctx_->GetLockManager()->LockRow(tsn_, LockManager::LockMode::SHARED, cur_table_id_, *rid);
        obtain_lock = true;
      }
      *tuple = *cursor_;
      cursor_++;
      if (obtain_lock && tsn_->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
        exec_ctx_->GetLockManager()->UnlockRow(tsn_, cur_table_id_, *rid);
      }
      // obtain_lock_ = false;
      return true;
    }
    // 如果有筛选谓词
    auto evaluate_res = fileter_predicate_->Evaluate(&(*(cursor_)), plan_->OutputSchema());
    if (!evaluate_res.IsNull() && evaluate_res.GetAs<bool>()) {
      if (((tsn_->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) ||
           (tsn_->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ)) &&
          (!tsn_->IsRowSharedLocked(cur_table_id_, *rid) && !tsn_->IsRowExclusiveLocked(cur_table_id_, *rid))) {
        // 如果满足条件，那么就获得锁
        exec_ctx_->GetLockManager()->LockRow(tsn_, LockManager::LockMode::SHARED, cur_table_id_, *rid);
        obtain_lock = true;
      }
      *tuple = *cursor_;
      cursor_++;
      // if (table_name_ == "nft") {
      //   cursor_ = end_;
      // }
      if (obtain_lock && tsn_->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
        exec_ctx_->GetLockManager()->UnlockRow(tsn_, cur_table_id_, *rid);
      }
      // obtain_lock_ = false;
      return true;
    }
    cursor_++;
  }
  return false;
}

}  // namespace bustub
