//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.h
//
// Identification: src/include/concurrency/lock_manager.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <condition_variable>  // NOLINT
#include <deque>
#include <list>
#include <map>
#include <memory>
#include <mutex>  // NOLINT
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/rid.h"
#include "concurrency/transaction.h"

namespace bustub {

using std::cout;
using std::endl;

class TransactionManager;

/**
 * LockManager handles transactions asking for locks on records.
 */
class LockManager {
 public:
  enum class LockMode { SHARED, EXCLUSIVE, INTENTION_SHARED, INTENTION_EXCLUSIVE, SHARED_INTENTION_EXCLUSIVE };

  /**
   * Structure to hold a lock request.
   * This could be a lock request on a table OR a row.
   * For table lock requests, the rid_ attribute would be unused.
   */
  class LockRequest {
   public:
    LockRequest(txn_id_t txn_id, LockMode lock_mode, table_oid_t oid) /** Table lock request */
        : txn_id_(txn_id), lock_mode_(lock_mode), oid_(oid), is_table_(true) {}
    LockRequest(txn_id_t txn_id, LockMode lock_mode, table_oid_t oid, RID rid) /** Row lock request */
        : txn_id_(txn_id), lock_mode_(lock_mode), oid_(oid), rid_(rid), is_table_(false) {}

    /** Txn_id of the txn requesting the lock */
    txn_id_t txn_id_;
    /** Locking mode of the requested lock */
    LockMode lock_mode_;
    /** Oid of the table for a table lock; oid of the table the row belong to for a row lock */
    table_oid_t oid_;
    /** Rid of the row for a row lock; unused for table locks */
    RID rid_;
    // 对表的还是对行的
    bool is_table_;
    /** Whether the lock has been granted or not */
    bool granted_{false};
  };

  class LockRequestQueue {
   public:
    void InsertIntoQueue(std::shared_ptr<LockRequest> &request, bool is_upgrade) {
      // 如果不是更新的
      if (!is_upgrade) {
        request_queue_.push_back(request);
        return;
      }
      // 如果是更新的，那么需要找到未授权的第一个位置，再插入
      auto find_lambda = [](std::shared_ptr<LockRequest> &req) { return req->granted_; };
      auto it = std::find_if_not(request_queue_.begin(), request_queue_.end(), find_lambda);
      request_queue_.insert(it, request);
    }
    auto IsSLock(LockMode lock_mode) { return lock_mode == LockMode::SHARED; }
    auto IsXLock(LockMode lock_mode) { return lock_mode == LockMode::EXCLUSIVE; }
    auto IsISLock(LockMode lock_mode) { return lock_mode == LockMode::INTENTION_SHARED; }
    auto IsIXLock(LockMode lock_mode) { return lock_mode == LockMode::INTENTION_EXCLUSIVE; }
    auto IsSIXLock(LockMode lock_mode) { return lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE; }

    auto LockModeOk(LockMode a, LockMode b) -> bool {
      bool suc1 = (IsSLock(a)) && (IsSLock(b) || IsISLock(b));
      bool suc2 = (IsISLock(a)) && (IsSLock(b) || IsISLock(b) || IsIXLock(b) || IsSIXLock(b));
      bool suc3 = (IsIXLock(a)) && (IsISLock(b) || IsIXLock(b));
      bool suc4 = (IsSIXLock(a)) && (IsISLock(b));

      return suc1 || suc2 || suc3 || suc4;
    }

    auto IsAllCompatible(std::list<std::shared_ptr<LockRequest>>::iterator new_lock_it) -> bool {
      auto new_lock_mode = (*new_lock_it)->lock_mode_;
      for (auto it = request_queue_.begin(); it != new_lock_it; it++) {
        auto old_lock_mode = (*it)->lock_mode_;
        if (!LockModeOk(old_lock_mode, new_lock_mode)) {
          return false;
        }
      }
      return true;
    }
    /** List of lock requests for the same resource (table or row) */
    std::list<std::shared_ptr<LockRequest>> request_queue_;
    /** For notifying blocked transactions on this rid */
    std::condition_variable cv_;
    /** txn_id of an upgrading transaction (if any) */
    txn_id_t upgrading_ = INVALID_TXN_ID;
    /** coordination */
    std::mutex latch_;
  };

  /**
   * Creates a new lock manager configured for the deadlock detection policy.
   */
  LockManager() {
    enable_cycle_detection_ = true;
    cycle_detection_thread_ = new std::thread(&LockManager::RunCycleDetection, this);
  }

  ~LockManager() {
    enable_cycle_detection_ = false;
    cycle_detection_thread_->join();
    delete cycle_detection_thread_;
  }

  /**
   * [LOCK_NOTE]
   *
   * GENERAL BEHAVIOUR:
   *    Both LockTable() and LockRow() are blocking methods; they should wait till the lock is granted and then return.
   *    If the transaction was aborted in the meantime, do not grant the lock and return false.
   *
   *
   * MULTIPLE TRANSACTIONS:
   *    LockManager should maintain a queue for each resource; locks should be granted to transactions in a FIFO manner.
   *    If there are multiple compatible lock requests, all should be granted at the same time
   *    as long as FIFO is honoured.
   *
   * SUPPORTED LOCK MODES:
   *    Table locking should support all lock modes.
   *    Row locking should not support Intention locks. Attempting this should set the TransactionState as
   *    ABORTED and throw a TransactionAbortException (ATTEMPTED_INTENTION_LOCK_ON_ROW)
   *
   *
   * ISOLATION LEVEL:
   *    Depending on the ISOLATION LEVEL, a transaction should attempt to take locks:
   *    - Only if required, AND
   *    - Only if allowed
   *
   *    For instance S/IS/SIX locks are not required under READ_UNCOMMITTED, and any such attempt should set the
   *    TransactionState as ABORTED and throw a TransactionAbortException (LOCK_SHARED_ON_READ_UNCOMMITTED).
   *
   *    Similarly, X/IX locks on rows are not allowed if the the Transaction State is SHRINKING, and any such attempt
   *    should set the TransactionState as ABORTED and throw a TransactionAbortException (LOCK_ON_SHRINKING).
   *
   *    REPEATABLE_READ:
   *        The transaction is required to take all locks.
   *        All locks are allowed in the GROWING state
   *        No locks are allowed in the SHRINKING state
   *
   *    READ_COMMITTED:
   *        The transaction is required to take all locks.
   *        All locks are allowed in the GROWING state
   *        Only IS, S locks are allowed in the SHRINKING state
   *
   *    READ_UNCOMMITTED:
   *        The transaction is required to take only IX, X locks.
   *        X, IX locks are allowed in the GROWING state.
   *        S, IS, SIX locks are never allowed
   *
   *
   * MULTILEVEL LOCKING:
   *    While locking rows, Lock() should ensure that the transaction has an appropriate lock on the table which the row
   *    belongs to. For instance, if an exclusive lock is attempted on a row, the transaction must hold either
   *    X, IX, or SIX on the table. If such a lock does not exist on the table, Lock() should set the TransactionState
   *    as ABORTED and throw a TransactionAbortException (TABLE_LOCK_NOT_PRESENT)
   *
   *
   * LOCK UPGRADE:
   *    Calling Lock() on a resource that is already locked should have the following behaviour:
   *    - If requested lock mode is the same as that of the lock presently held,
   *      Lock() should return true since it already has the lock.
   *    - If requested lock mode is different, Lock() should upgrade the lock held by the transaction.
   *
   *    A lock request being upgraded should be prioritised over other waiting lock requests on the same resource.
   *
   *    While upgrading, only the following transitions should be allowed:
   *        IS -> [S, X, IX, SIX]
   *        S -> [X, SIX]
   *        IX -> [X, SIX]
   *        SIX -> [X]
   *    Any other upgrade is considered incompatible, and such an attempt should set the TransactionState as ABORTED
   *    and throw a TransactionAbortException (INCOMPATIBLE_UPGRADE)
   *
   *    Furthermore, only one transaction should be allowed to upgrade its lock on a given resource.
   *    Multiple concurrent lock upgrades on the same resource should set the TransactionState as
   *    ABORTED and throw a TransactionAbortException (UPGRADE_CONFLICT).
   *
   *
   * BOOK KEEPING:
   *    If a lock is granted to a transaction, lock manager should update its
   *    lock sets appropriately (check transaction.h)
   */

  /**
   * [UNLOCK_NOTE]
   *
   * GENERAL BEHAVIOUR:
   *    Both UnlockTable() and UnlockRow() should release the lock on the resource and return.
   *    Both should ensure that the transaction currently holds a lock on the resource it is attempting to unlock.
   *    If not, LockManager should set the TransactionState as ABORTED and throw
   *    a TransactionAbortException (ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD)
   *
   *    Additionally, unlocking a table should only be allowed if the transaction does not hold locks on any
   *    row on that table. If the transaction holds locks on rows of the table, Unlock should set the Transaction State
   *    as ABORTED and throw a TransactionAbortException (TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS).
   *
   *    Finally, unlocking a resource should also grant any new lock requests for the resource (if possible).
   *
   * TRANSACTION STATE UPDATE
   *    Unlock should update the transaction state appropriately (depending upon the ISOLATION LEVEL)
   *    Only unlocking S or X locks changes transaction state.
   *
   *    REPEATABLE_READ:
   *        Unlocking S/X locks should set the transaction state to SHRINKING
   *
   *    READ_COMMITTED:
   *        Unlocking X locks should set the transaction state to SHRINKING.
   *        Unlocking S locks does not affect transaction state.
   *
   *   READ_UNCOMMITTED:
   *        Unlocking X locks should set the transaction state to SHRINKING.
   *        S locks are not permitted under READ_UNCOMMITTED.
   *            The behaviour upon unlocking an S lock under this isolation level is undefined.
   *
   *
   * BOOK KEEPING:
   *    After a resource is unlocked, lock manager should update the transaction's lock sets
   *    appropriately (check transaction.h)
   */

  /**
   * Acquire a lock on table_oid_t in the given lock_mode.
   * If the transaction already holds a lock on the table, upgrade the lock
   * to the specified lock_mode (if possible).
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [LOCK_NOTE] in header file.
   *
   * @param txn the transaction requesting the lock upgrade
   * @param lock_mode the lock mode for the requested lock
   * @param oid the table_oid_t of the table to be locked in lock_mode
   * @return true if the upgrade is successful, false otherwise
   */

  auto GetTableQueue(table_oid_t table_oid) -> std::shared_ptr<LockRequestQueue> {
    // 给维护表的映射关系的map加锁
    std::lock_guard<std::mutex> lock(table_lock_map_latch_);
    // 若map里没有，则插入
    if (table_lock_map_.find(table_oid) == table_lock_map_.end()) {
      table_lock_map_.insert(std::make_pair(table_oid, std::make_shared<LockRequestQueue>()));
    }
    // 返回这个表对应的map
    return table_lock_map_[table_oid];
  }

  auto GetRowQueue(RID rid) -> std::shared_ptr<LockRequestQueue> {
    std::lock_guard<std::mutex> lock(row_lock_map_latch_);
    if (row_lock_map_.find(rid) == row_lock_map_.end()) {
      row_lock_map_.insert(std::make_pair(rid, std::make_shared<LockRequestQueue>()));
    }
    return row_lock_map_[rid];
  }

  auto IsSLock(LockMode lock_mode) { return lock_mode == LockMode::SHARED; }
  auto IsXLock(LockMode lock_mode) { return lock_mode == LockMode::EXCLUSIVE; }
  auto IsISLock(LockMode lock_mode) { return lock_mode == LockMode::INTENTION_SHARED; }
  auto IsIXLock(LockMode lock_mode) { return lock_mode == LockMode::INTENTION_EXCLUSIVE; }
  auto IsSIXLock(LockMode lock_mode) { return lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE; }

  auto IsLockRequestValid(Transaction *txn, LockMode lock_mode, bool is_table, const table_oid_t &oid,
                          LockMode &pre_lock_mode, bool &is_upgrade, const RID &rid,
                          std::shared_ptr<LockRequestQueue> &queue, AbortReason &reason) -> bool;

  auto CouldLockRequestProceed(std::shared_ptr<LockRequest> &request, Transaction *txn,
                               std::shared_ptr<LockRequestQueue> &queue, bool &is_upgrade, bool &is_abort) -> bool;

  auto LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) noexcept(false) -> bool;

  /**
   * Release the lock held on a table by the transaction.
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [UNLOCK_NOTE] in header file.
   *
   * @param txn the transaction releasing the lock
   * @param oid the table_oid_t of the table to be unlocked
   * @return true if the unlock is successful, false otherwise
   */
  auto IsUnLockRequestValid(Transaction *txn, LockMode &lock_mode, bool is_table, const table_oid_t &oid,
                            const RID &rid, std::shared_ptr<LockRequestQueue> &queue, AbortReason &reason) -> bool;
  auto UnlockTableHelper(Transaction *txn, const table_oid_t &oid, bool is_upgrade) -> bool;
  void TryUpdateTxnState(Transaction *txn, LockMode unlock_mode);
  auto UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool;

  /**
   * Acquire a lock on rid in the given lock_mode.
   * If the transaction already holds a lock on the row, upgrade the lock
   * to the specified lock_mode (if possible).
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [LOCK_NOTE] in header file.
   *
   * @param txn the transaction requesting the lock upgrade
   * @param lock_mode the lock mode for the requested lock
   * @param oid the table_oid_t of the table the row belongs to
   * @param rid the RID of the row to be locked
   * @return true if the upgrade is successful, false otherwise
   */
  auto LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool;

  /**
   * Release the lock held on a row by the transaction.
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [UNLOCK_NOTE] in header file.
   *
   * @param txn the transaction releasing the lock
   * @param rid the RID that is locked by the transaction
   * @param oid the table_oid_t of the table the row belongs to
   * @param rid the RID of the row to be unlocked
   * @return true if the unlock is successful, false otherwise
   */
  auto UnlockRowHelper(Transaction *txn, const table_oid_t &oid, const RID &rid, bool is_upgrade) -> bool;
  auto UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool;

  /*** Graph API ***/

  /**
   * Adds an edge from t1 -> t2 from waits for graph.
   * @param t1 transaction waiting for a lock
   * @param t2 transaction being waited for
   */
  auto AddEdge(txn_id_t t1, txn_id_t t2) -> void;

  /**
   * Removes an edge from t1 -> t2 from waits for graph.
   * @param t1 transaction waiting for a lock
   * @param t2 transaction being waited for
   */
  auto RemoveEdge(txn_id_t t1, txn_id_t t2) -> void;

  /**
   * Checks if the graph has a cycle, returning the newest transaction ID in the cycle if so.
   * @param[out] txn_id if the graph has a cycle, will contain the newest transaction ID
   * @return false if the graph has no cycle, otherwise stores the newest transaction ID in the cycle to txn_id
   */
  auto DepthFirstSearchToFindCycle(txn_id_t cur, std::set<txn_id_t> &visited, std::deque<txn_id_t> &path) -> txn_id_t {
    // 记录当前节点
    visited.emplace(cur);
    path.emplace_back(cur);
    // 开始尝试去搜索
    for (auto &next_txn : waits_for_[cur]) {
      // 如果下一个没有被访问过
      if (visited.find(next_txn) == visited.end()) {
        auto next_res = DepthFirstSearchToFindCycle(next_txn, visited, path);
        // 如果在下一个那里发现了环
        if (next_res != -1) {
          return next_res;
        }
      }
      // 如果下一个就在path里面，即当前就是一个环
      if (std::find(path.begin(), path.end(), next_txn) != path.end()) {
        return next_txn;
      }
    }
    path.pop_back();
    return -1;
  }
  auto HasCycle(txn_id_t *txn_id) -> bool;

  /**
   * @return all edges in current waits_for graph
   */
  auto GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>>;

  /**
   * Runs cycle detection in the background.
   */
  void NotifyAllTxn() {
    for (auto &[oid, queue] : table_lock_map_) {
      queue->cv_.notify_all();
    }
    for (auto &[rid, queue] : row_lock_map_) {
      queue->cv_.notify_all();
    }
  }
  void TrimGraph(txn_id_t &id) {
    waits_for_.erase(id);
    for (auto &[start, end_point] : waits_for_) {
      end_point.erase(id);
    }
  }
  void MakeGraph() {
    for (auto &[oid, queue] : table_lock_map_) {
      std::set<txn_id_t> txn_has_lock;
      for (auto &req : queue->request_queue_) {
        if (req->granted_) {
          txn_has_lock.emplace(req->txn_id_);
        } else {
          for (auto &ne : txn_has_lock) {
            AddEdge(req->txn_id_, ne);
          }
        }
      }
    }
    for (auto &[rid, queue] : row_lock_map_) {
      std::set<txn_id_t> txn_has_lock;
      for (auto &req : queue->request_queue_) {
        if (req->granted_) {
          txn_has_lock.emplace(req->txn_id_);
        } else {
          for (auto &ne : txn_has_lock) {
            AddEdge(req->txn_id_, ne);
          }
        }
      }
    }
  }
  auto RunCycleDetection() -> void;

 private:
  /** Fall 2022 */
  /** Structure that holds lock requests for a given table oid */
  std::unordered_map<table_oid_t, std::shared_ptr<LockRequestQueue>> table_lock_map_;
  /** Coordination */
  std::mutex table_lock_map_latch_;

  /** Structure that holds lock requests for a given RID */
  std::unordered_map<RID, std::shared_ptr<LockRequestQueue>> row_lock_map_;
  /** Coordination */
  std::mutex row_lock_map_latch_;

  std::atomic<bool> enable_cycle_detection_;
  std::thread *cycle_detection_thread_;
  /** Waits-for graph representation. */
  std::map<txn_id_t, std::set<txn_id_t>> waits_for_;
  std::mutex waits_for_latch_;

  //
};

}  // namespace bustub
