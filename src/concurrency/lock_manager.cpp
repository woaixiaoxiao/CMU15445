//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include <algorithm>
#include <deque>
#include <iostream>
#include <memory>
#include <utility>

#include "common/config.h"
#include "common/exception.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::IsLockRequestValid(Transaction *txn, LockMode lock_mode, bool is_table, const table_oid_t &oid,
                                     LockMode &pre_lock_mode, bool &is_upgrade, const RID &rid,
                                     std::shared_ptr<LockRequestQueue> &queue, AbortReason &reason) -> bool {
  // 检查lock_mode是否符合规定，表无所谓，主要是行不能有意向锁
  if (!is_table) {
    if (IsISLock(lock_mode) || IsIXLock(lock_mode) || IsSIXLock(lock_mode)) {
      reason = AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW;
      return false;
    }
  }
  // 隔离级别的检查，即lock_mode和隔离级别是否匹配的上
  auto isolation_level = txn->GetIsolationLevel();
  auto state = txn->GetState();
  if (isolation_level == IsolationLevel::REPEATABLE_READ) {
    if (state == TransactionState::SHRINKING) {
      reason = AbortReason::LOCK_ON_SHRINKING;
      return false;
    }
  } else if (isolation_level == IsolationLevel::READ_COMMITTED) {
    if (state == TransactionState::SHRINKING) {
      if (!IsISLock(lock_mode) && !IsSLock(lock_mode)) {
        reason = AbortReason::LOCK_ON_SHRINKING;
        return false;
      }
    }
  } else if (isolation_level == IsolationLevel::READ_UNCOMMITTED) {
    if (!IsXLock(lock_mode) && !IsIXLock(lock_mode)) {
      reason = AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED;
      return false;
    }
    if (state == TransactionState::SHRINKING) {
      reason = AbortReason::LOCK_ON_SHRINKING;
      return false;
    }
  }
  // 多级检查，即如果当前是行，那么表的lock_mode是否符合规定
  if (!is_table) {
    if (IsXLock(lock_mode)) {
      if (!txn->IsTableExclusiveLocked(oid) && !txn->IsTableIntentionExclusiveLocked(oid) &&
          !txn->IsTableSharedIntentionExclusiveLocked(oid)) {
        reason = AbortReason::TABLE_LOCK_NOT_PRESENT;
        return false;
      }
    } else if (IsSLock(lock_mode)) {
      if (!txn->IsTableExclusiveLocked(oid) && !txn->IsTableIntentionExclusiveLocked(oid) &&
          !txn->IsTableIntentionSharedLocked(oid) && !txn->IsTableSharedIntentionExclusiveLocked(oid) &&
          !txn->IsTableSharedLocked(oid)) {
        reason = AbortReason::TABLE_LOCK_NOT_PRESENT;
        return false;
      }
    }
  }
  // 升级检查
  // 先找出之前的锁的级别
  is_upgrade = false;
  if (is_table) {
    if (txn->IsTableSharedLocked(oid)) {
      pre_lock_mode = LockMode::SHARED;
      is_upgrade = true;
    } else if (txn->IsTableExclusiveLocked(oid)) {
      pre_lock_mode = LockMode::EXCLUSIVE;
      is_upgrade = true;
    } else if (txn->IsTableIntentionSharedLocked(oid)) {
      pre_lock_mode = LockMode::INTENTION_SHARED;
      is_upgrade = true;
    } else if (txn->IsTableIntentionExclusiveLocked(oid)) {
      pre_lock_mode = LockMode::INTENTION_EXCLUSIVE;
      is_upgrade = true;
    } else if (txn->IsTableSharedIntentionExclusiveLocked(oid)) {
      pre_lock_mode = LockMode::SHARED_INTENTION_EXCLUSIVE;
      is_upgrade = true;
    }
  } else {
    if (txn->IsRowSharedLocked(oid, rid)) {
      pre_lock_mode = LockMode::SHARED;
      is_upgrade = true;
    } else if (txn->IsRowExclusiveLocked(oid, rid)) {
      pre_lock_mode = LockMode::EXCLUSIVE;
      is_upgrade = true;
    }
  }
  // 然后去判断这个锁是否可以升级
  if (is_upgrade && pre_lock_mode != lock_mode) {
    // 首先判断现在是否已经有一个在升级了
    if (queue->upgrading_ != INVALID_TXN_ID) {
      reason = AbortReason::UPGRADE_CONFLICT;
      return false;
    }
    // 然后判断前后的锁是否可以兼容
    bool suc1 = IsISLock(pre_lock_mode) &&
                (IsSLock(lock_mode) || IsXLock(lock_mode) || IsIXLock(lock_mode) || IsSIXLock(lock_mode));
    bool suc2 = IsSLock(pre_lock_mode) && (IsXLock(lock_mode) || IsSIXLock(lock_mode));
    bool suc3 = IsIXLock(pre_lock_mode) && (IsXLock(lock_mode) || IsSIXLock(lock_mode));
    bool suc4 = IsSIXLock(pre_lock_mode) && (IsXLock(lock_mode));
    if (!(suc1 || suc2 || suc3 || suc4)) {
      reason = AbortReason::INCOMPATIBLE_UPGRADE;
      return false;
    }
  }
  // 可以加锁或者更新锁
  return true;
}

auto LockManager::CouldLockRequestProceed(std::shared_ptr<LockRequest> &request, Transaction *txn,
                                          std::shared_ptr<LockRequestQueue> &queue, bool &is_upgrade, bool &is_abort)
    -> bool {
  // 先给事务挂个锁，因为在加入queue之后，事务的锁已经被解开了
  txn->LockTxn();
  // 检查事务的状态，如果已经aborted，则直接返回
  is_abort = false;
  if (txn->GetState() == TransactionState::ABORTED) {
    is_abort = true;
    txn->UnlockTxn();
    return true;
  }

  // 检查这个事务是否是第一个被授权的
  auto cur_it = std::find(queue->request_queue_.begin(), queue->request_queue_.end(), request);
  auto find_first_ungranted_lambda = [](std::shared_ptr<LockRequest> &req) { return req->granted_; };
  auto first_ungranted_it =
      std::find_if_not(queue->request_queue_.begin(), queue->request_queue_.end(), find_first_ungranted_lambda);
  // 还没轮到这个事务去上锁

  if (cur_it != first_ungranted_it) {
    txn->UnlockTxn();
    return false;
  }
  // 如果轮到了这个事务去上锁，先检查这个事务是否和已有的锁冲突了

  bool is_compatible = queue->IsAllCompatible(cur_it);
  // cout<<is_compatible<<endl;
  // 和以前的锁冲突了，直接去世
  if (!is_compatible) {
    txn->UnlockTxn();
    return false;
  }

  // 可以上锁，先更新事务中记录每种类型的锁的set
  auto lock_mode = request->lock_mode_;
  if (request->is_table_) {
    if (IsSLock(lock_mode)) {
      txn->GetSharedTableLockSet()->insert(request->oid_);
    } else if (IsXLock(lock_mode)) {
      txn->GetExclusiveTableLockSet()->insert(request->oid_);
    } else if (IsISLock(lock_mode)) {
      txn->GetIntentionSharedTableLockSet()->insert(request->oid_);
    } else if (IsIXLock(lock_mode)) {
      txn->GetIntentionExclusiveTableLockSet()->insert(request->oid_);
    } else if (IsSIXLock(lock_mode)) {
      txn->GetSharedIntentionExclusiveTableLockSet()->insert(request->oid_);
    }
  } else {
    if (IsSLock(lock_mode)) {
      txn->GetSharedRowLockSet()->operator[](request->oid_).insert(request->rid_);
    } else if (IsXLock(lock_mode)) {
      txn->GetExclusiveRowLockSet()->operator[](request->oid_).insert(request->rid_);
    }
  }
  // 再更新这个请求为允许
  request->granted_ = true;
  if (is_upgrade) {
    queue->upgrading_ = INVALID_TXN_ID;
  }
  txn->UnlockTxn();
  return true;
}

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  // 首先获得这个表维护的那个队列
  auto queue = GetTableQueue(oid);
  // 然后获得这个队列的锁
  std::unique_lock<std::mutex> lock(queue->latch_);
  // 获取事务的锁，这个事务的状态等变量都是可能被多个线程并发访问和修改的
  txn->LockTxn();

  // 先判断是否可以添加这个锁
  LockMode pre_lock_mode;
  bool is_upgrade;
  RID rid;
  AbortReason reason;
  bool is_lock_valid = IsLockRequestValid(txn, lock_mode, true, oid, pre_lock_mode, is_upgrade, rid, queue, reason);
  // 如果不可以加这个锁，则终止这个事务，释放锁，并扔出异常
  if (!is_lock_valid) {
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    throw TransactionAbortException(txn->GetTransactionId(), reason);
  }

  // 如果要进行的操作是升级
  if (is_upgrade) {
    // 如果前后的锁的类型一致
    if (lock_mode == pre_lock_mode) {
      txn->UnlockTxn();
      return true;
    }
    // 如果不一致，那么就先释放这个锁
    queue->upgrading_ = txn->GetTransactionId();
    UnlockTableHelper(txn, oid, true);
  }

  // 将这个锁的记录插入queue
  auto request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
  queue->InsertIntoQueue(request, is_upgrade);

  // 当前事务的加锁请求已经加入对应的queue了，可以释放当前事务的锁
  txn->UnlockTxn();

  // 等待这个queue不断处理，直到这个事务执行，或者提前被终止了
  bool already_abort = false;
  auto wait_lambda = [&]() { return CouldLockRequestProceed(request, txn, queue, is_upgrade, already_abort); };
  queue->cv_.wait(lock, wait_lambda);

  // 如果这个事务已经被终止了
  if (already_abort) {
    // 记得更新这个upgrading
    if (is_upgrade) {
      queue->upgrading_ = INVALID_TXN_ID;
    }
    // 找到这个事务在queue中位置，删除
    auto find_lambda = [&](std::shared_ptr<LockRequest> &req) {
      return req->txn_id_ == txn->GetTransactionId() && req->oid_ == oid;
    };
    auto it = std::find_if(queue->request_queue_.begin(), queue->request_queue_.end(), find_lambda);
    queue->request_queue_.erase(it);
    // 解锁，返回答案
    lock.unlock();
    queue->cv_.notify_all();
    return false;
  }

  // 如果这个事务正常，并且lock请求已经被接受
  lock.unlock();
  queue->cv_.notify_all();
  return true;
}

auto LockManager::IsUnLockRequestValid(Transaction *txn, LockMode &lock_mode, bool is_table, const table_oid_t &oid,
                                       const RID &rid, std::shared_ptr<LockRequestQueue> &queue, AbortReason &reason)
    -> bool {
  // 确保这个事务在这个表上存在某种锁，如果不存在，return false，否则记录下这种锁
  if (is_table) {
    bool s_lock = txn->IsTableSharedLocked(oid);
    bool x_lock = txn->IsTableExclusiveLocked(oid);
    bool is_lock = txn->IsTableIntentionSharedLocked(oid);
    bool ix_lock = txn->IsTableIntentionExclusiveLocked(oid);
    bool six_lock = txn->IsTableSharedIntentionExclusiveLocked(oid);
    if (!(s_lock || x_lock || is_lock || ix_lock || six_lock)) {
      reason = AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD;
      return false;
    }
    if (s_lock) {
      lock_mode = LockMode::SHARED;
    } else if (x_lock) {
      lock_mode = LockMode::EXCLUSIVE;
    } else if (is_lock) {
      lock_mode = LockMode::INTENTION_SHARED;
    } else if (ix_lock) {
      lock_mode = LockMode::INTENTION_EXCLUSIVE;
    } else if (six_lock) {
      lock_mode = LockMode::SHARED_INTENTION_EXCLUSIVE;
    }
  } else {
    bool s_lock = txn->IsRowSharedLocked(oid, rid);
    bool x_lock = txn->IsRowExclusiveLocked(oid, rid);
    if (!(s_lock || x_lock)) {
      reason = AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD;
      return false;
    }
    if (s_lock) {
      lock_mode = LockMode::SHARED;
    } else if (x_lock) {
      lock_mode = LockMode::EXCLUSIVE;
    }
  }
  // 确认这个事务在这个表的所有行上的锁都除干净了
  if (is_table) {
    bool not_upgrade = txn->GetTransactionId() != queue->upgrading_;
    bool s_not_empty = !txn->GetSharedRowLockSet()->operator[](oid).empty();
    bool x_not_empty = !txn->GetExclusiveRowLockSet()->operator[](oid).empty();
    if (not_upgrade && (s_not_empty || x_not_empty)) {
      reason = AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS;
      return false;
    }
  }
  return true;
}

void LockManager::TryUpdateTxnState(Transaction *txn, LockMode unlock_mode) {
  // 如果隔离级别是重复读，并且要解锁的是读或者写
  auto isolation_level = txn->GetIsolationLevel();
  if (isolation_level == IsolationLevel::REPEATABLE_READ) {
    if (unlock_mode == LockMode::SHARED || unlock_mode == LockMode::EXCLUSIVE) {
      txn->SetState(TransactionState::SHRINKING);
    }
  } else if (isolation_level == IsolationLevel::READ_COMMITTED || isolation_level == IsolationLevel::READ_UNCOMMITTED) {
    if (unlock_mode == LockMode::EXCLUSIVE) {
      txn->SetState(TransactionState::SHRINKING);
    }
  }
}

auto LockManager::UnlockTableHelper(Transaction *txn, const table_oid_t &oid, bool is_upgrade) -> bool {
  // 首先取出queue，并在不是upgrade的情况下试图获取锁
  auto queue = GetTableQueue(oid);
  std::unique_lock<std::mutex> lock;
  if (!is_upgrade) {
    lock = std::unique_lock<std::mutex>(queue->latch_);
    txn->LockTxn();
  }
  // 检查unlock的合法性
  AbortReason reason;
  LockMode lock_mode;
  RID rid;
  bool is_unlock_valid = IsUnLockRequestValid(txn, lock_mode, true, oid, rid, queue, reason);
  if (!is_unlock_valid) {
    txn->SetState(TransactionState::ABORTED);
    if (!is_upgrade) {
      txn->UnlockTxn();
    }
    throw TransactionAbortException(txn->GetTransactionId(), reason);
  }
  // 因为现在是unlock，尝试更新状态位shrinking，如果现在不是在upgrade期间，并且现在也没有到后面两个阶段的话
  if (!is_upgrade && txn->GetState() != TransactionState::COMMITTED && txn->GetState() != TransactionState::ABORTED) {
    TryUpdateTxnState(txn, lock_mode);
  }
  // 先去queue中删除这个request
  auto find_lambda = [&](std::shared_ptr<LockRequest> &req) {
    return req->txn_id_ == txn->GetTransactionId() && req->oid_ == oid;
  };
  auto it = std::find_if(queue->request_queue_.begin(), queue->request_queue_.end(), find_lambda);
  queue->request_queue_.erase(it);
  // 再去这个事务里删除这个表的记录
  if (IsSLock(lock_mode)) {
    txn->GetSharedTableLockSet()->erase(oid);
  } else if (IsXLock(lock_mode)) {
    txn->GetExclusiveTableLockSet()->erase(oid);
  } else if (IsISLock(lock_mode)) {
    txn->GetIntentionSharedTableLockSet()->erase(oid);
  } else if (IsIXLock(lock_mode)) {
    txn->GetIntentionExclusiveTableLockSet()->erase(oid);
  } else if (IsSIXLock(lock_mode)) {
    txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
  }

  // 如果不是更新的部分，那就开锁，并叫醒
  if (!is_upgrade) {
    txn->UnlockTxn();
    lock.unlock();
    queue->cv_.notify_all();
  }

  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  return UnlockTableHelper(txn, oid, false);
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  // 首先获得queue，并上锁
  auto queue = GetRowQueue(rid);
  std::unique_lock<std::mutex> lock(queue->latch_);
  txn->LockTxn();
  // 检查这个上锁是否合理
  AbortReason reason;
  bool is_upgrade;
  LockMode pre_lock_mode;
  bool lock_req_valid = IsLockRequestValid(txn, lock_mode, false, oid, pre_lock_mode, is_upgrade, rid, queue, reason);
  // 如果这个上锁的要求不合理，那么解锁，并抛出异常，并且只要有一个请求不合理，那么这个txn就要abort
  if (!lock_req_valid) {
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    throw TransactionAbortException(txn->GetTransactionId(), reason);
  }
  // 请求合理，那么可以给它加锁
  // 先判断是否是升级
  if (is_upgrade) {
    // 如果前后的mode一致，则不用管，解锁，返回
    if (pre_lock_mode == lock_mode) {
      txn->UnlockTxn();
      return true;
    }
    // 前后不一致，那么就要进行更新，先将queue的更新txnid置为当前id，并删除queue中的请求
    queue->upgrading_ = txn->GetTransactionId();
    UnlockRowHelper(txn, oid, rid, is_upgrade);
  }
  // 将当前这个上锁请求加入queue中
  auto request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
  queue->InsertIntoQueue(request, is_upgrade);
  // 这个事务的请求已经放入queue了，事务可以先解锁了
  txn->UnlockTxn();
  // 等待这个锁被加上
  bool is_abort = false;
  auto wait_lambda = [&]() { return CouldLockRequestProceed(request, txn, queue, is_upgrade, is_abort); };
  queue->cv_.wait(lock, wait_lambda);
  // 等待结束，如果这个事务已经被终止了
  if (is_abort) {
    // 如果是更新状态，那么要修改queue的ungrade txn id
    if (is_upgrade) {
      queue->upgrading_ = INVALID_TXN_ID;
    }
    // 将这个request从queue中除去
    auto find_lambda = [&](std::shared_ptr<LockRequest> &req) {
      return req->txn_id_ == txn->GetTransactionId() && req->oid_ == oid && req->rid_ == rid;
    };
    auto it = std::find_if(queue->request_queue_.begin(), queue->request_queue_.end(), find_lambda);
    queue->request_queue_.erase(it);
    lock.unlock();
    queue->cv_.notify_all();
    return false;
  }
  // 如果这个上锁请求被正常上锁了，那么就正常返回
  lock.unlock();
  queue->cv_.notify_all();
  return true;
}

auto LockManager::UnlockRowHelper(Transaction *txn, const table_oid_t &oid, const RID &rid, bool is_upgrade) -> bool {
  auto queue = GetRowQueue(rid);
  std::unique_lock<std::mutex> lock;
  if (!is_upgrade) {
    lock = std::unique_lock<std::mutex>(queue->latch_);
    txn->LockTxn();
  }
  // 检查合法性
  LockMode lock_mode;
  AbortReason reason;
  bool is_valid = IsUnLockRequestValid(txn, lock_mode, false, oid, rid, queue, reason);
  // 如果不合法
  if (!is_valid) {
    txn->SetState(TransactionState::ABORTED);
    if (!is_upgrade) {
      txn->UnlockTxn();
    }
    throw TransactionAbortException(txn->GetTransactionId(), reason);
  }
  // 如果取消锁的请求是合法的，先看看是否可以更新事务的状态
  if (!is_upgrade && txn->GetState() != TransactionState::COMMITTED && txn->GetState() != TransactionState::ABORTED) {
    TryUpdateTxnState(txn, lock_mode);
  }
  // 先去queue中删除这个请求
  auto find_lambda = [&](std::shared_ptr<LockRequest> &req) {
    return req->txn_id_ == txn->GetTransactionId() && req->oid_ == oid && req->rid_ == rid;
  };
  auto it = std::find_if(queue->request_queue_.begin(), queue->request_queue_.end(), find_lambda);
  queue->request_queue_.erase(it);
  // 再将事务中记录的这个锁给删了
  if (lock_mode == LockMode::SHARED) {
    txn->GetSharedRowLockSet()->at(oid).erase(rid);
  } else if (lock_mode == LockMode::EXCLUSIVE) {
    txn->GetExclusiveRowLockSet()->at(oid).erase(rid);
  }
  // 如果不是在upgrad，那么就解锁，并通知其他人
  if (!is_upgrade) {
    txn->UnlockTxn();
    lock.unlock();
    queue->cv_.notify_all();
  }
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  return UnlockRowHelper(txn, oid, rid, false);
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) { waits_for_[t1].emplace(t2); }

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) { waits_for_[t1].erase(t2); }

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  std::set<txn_id_t> visited;
  std::deque<txn_id_t> path;
  for (auto &[start, end_points] : waits_for_) {
    // 如果这个事务结点还没有被碰到过
    if (visited.find(start) == visited.end()) {
      // 从这个事务结点去搜索
      txn_id_t first_cycle_point = DepthFirstSearchToFindCycle(start, visited, path);
      // 如果没有搜到，则直接进入下一轮循环
      // 如果找到了环，则进行处理
      if (first_cycle_point != -1) {
        // 因为这个环的入口不一定是这个环里最大的那个，所以需要找出这个环里最大的
        auto it = std::find(path.begin(), path.end(), first_cycle_point);
        path.erase(path.begin(), it);
        std::sort(path.begin(), path.end());
        *txn_id = path.back();
        return true;
      }
    }
  }
  return false;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges;
  for (auto &edge_set : waits_for_) {
    txn_id_t start = edge_set.first;
    for (auto &end : edge_set.second) {
      edges.emplace_back(std::make_pair(start, end));
    }
  }
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
      // 因为要对所有的表和行操作，所以先获得两个锁
      std::unique_lock<std::mutex> lock_table(table_lock_map_latch_);
      std::unique_lock<std::mutex> lock_tuple(row_lock_map_latch_);
      // 然后根据依赖关系，建图
      MakeGraph();
      // 不断循环，若有死锁，则删除这个事务，并修改依赖图
      txn_id_t cycle_id = -1;
      while (HasCycle(&cycle_id)) {
        // 根据这个cycle_id去删除这个事务
        auto txn = TransactionManager::GetTransaction(cycle_id);
        txn->SetState(TransactionState::ABORTED);
        // 修改依赖图
        TrimGraph(cycle_id);
      }
      // 如果处理了死锁，则最后提醒所有等待的txn试着去唤醒
      if (cycle_id != -1) {
        NotifyAllTxn();
      }
    }
  }
}

}  // namespace bustub
