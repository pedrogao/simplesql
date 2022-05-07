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
#include <list>
#include <memory>
#include <mutex>  // NOLINT
#include <stack>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/rid.h"
#include "concurrency/transaction.h"

namespace bustub {

class TransactionManager;

/**
 * LockManager handles transactions asking for locks on records.
 *
 * 实现一个严格的二阶段提交协议；注意，LockManager 是对 RID 的并发控制，而 B+树 是对 index 的并发控制
 */
class LockManager {
  enum class LockMode { SHARED, EXCLUSIVE };                  // 锁类型，共享还是独占
  enum class VisitedType { NOT_VISITED, IN_STACK, VISITED };  // 访问访问

  // 锁请求
  class LockRequest {
   public:
    LockRequest(txn_id_t txn_id, LockMode lock_mode) : txn_id_(txn_id), lock_mode_(lock_mode), granted_(false) {}

    txn_id_t txn_id_;     // 事务id
    LockMode lock_mode_;  // 锁类型
    bool granted_;        // 是否颁发
  };

  // 锁请求队列
  class LockRequestQueue {
   public:
    std::list<LockRequest> request_queue_;
    std::condition_variable cv_;  // for notifying blocked transactions on this rid
    bool upgrading_ = false;      // 是否升级
    std::mutex latch_;            // 锁
  };

 public:
  /**
   * Creates a new lock manager configured for the deadlock detection policy.
   */
  LockManager() {
    enable_cycle_detection_ = true;                                                    // 是否死锁检测
    cycle_detection_thread_ = new std::thread(&LockManager::RunCycleDetection, this);  // 新建线程检查死锁
    LOG_INFO("Cycle detection thread launched");
  }

  ~LockManager() {
    enable_cycle_detection_ = false;
    cycle_detection_thread_->join();
    delete cycle_detection_thread_;
    LOG_INFO("Cycle detection thread stopped");
  }

  /*
   * [LOCK_NOTE]: For all locking functions, we:
   * 1. return false if the transaction is aborted; and
   * 2. block on wait, return true when the lock request is granted; and
   * 3. it is undefined behavior to try locking an already locked RID in the same transaction, i.e. the transaction
   *    is responsible for keeping track of its current locks.
   */

  /**
   * Acquire a lock on RID in shared mode. See [LOCK_NOTE] in header file.
   * @param txn the transaction requesting the shared lock
   * @param rid the RID to be locked in shared mode
   * @return true if the lock is granted, false otherwise
   */
  bool LockShared(Transaction *txn, const RID &rid);

  /**
   * Acquire a lock on RID in exclusive mode. See [LOCK_NOTE] in header file.
   * @param txn the transaction requesting the exclusive lock
   * @param rid the RID to be locked in exclusive mode
   * @return true if the lock is granted, false otherwise
   */
  bool LockExclusive(Transaction *txn, const RID &rid);

  /**
   * Upgrade a lock from a shared lock to an exclusive lock.
   * @param txn the transaction requesting the lock upgrade
   * @param rid the RID that should already be locked in shared mode by the requesting transaction
   * @return true if the upgrade is successful, false otherwise
   */
  bool LockUpgrade(Transaction *txn, const RID &rid);

  /**
   * Release the lock held by the transaction.
   * @param txn the transaction releasing the lock, it should actually hold the lock
   * @param rid the RID that is locked by the transaction
   * @return true if the unlock is successful, false otherwise
   */
  bool Unlock(Transaction *txn, const RID &rid);

  /*** Graph API ***/
  /**
   * Adds edge t1->t2
   */

  /** Adds an edge from t1 -> t2. */
  void AddEdge(txn_id_t t1, txn_id_t t2);

  /** Removes an edge from t1 -> t2. */
  void RemoveEdge(txn_id_t t1, txn_id_t t2);

  /**
   * Checks if the graph has a cycle, returning the newest transaction ID in the cycle if so.
   * @param[out] txn_id if the graph has a cycle, will contain the newest transaction ID
   * @return false if the graph has no cycle, otherwise stores the newest transaction ID in the cycle to txn_id
   */
  bool HasCycle(txn_id_t *txn_id);

  /** @return the set of all edges in the graph, used for testing only! */
  std::vector<std::pair<txn_id_t, txn_id_t>> GetEdgeList();

  /** Runs cycle detection in the background. */
  void RunCycleDetection();

  /**
   * Test lock compatibility for a lock request against the lock request queue that it accquires lock on
   * 检测 target_request 是否与队列中的锁兼容
   *
   * Return true if and only if:
   * - queue is empty
   * - compatible with locks that are currently held
   * - all **earlier** requests have been granted already
   * @param lock_request_queue the queue to test compatibility
   * @param lock_request the request to test
   * @return true if compatible, otherwise false
   */
  static bool IsLockCompatible(const LockRequestQueue &lock_request_queue, const LockRequest &target_request) {
    for (auto &&lock_request : lock_request_queue.request_queue_) {
      if (lock_request.txn_id_ == target_request.txn_id_) {  // 如果事务已经在队列中存在
        return true;
      }
      // 这个地方需要遍历队列中的每个锁请求，如果有一个不满足，那么直接返回 false
      // 满足的条件是：所有锁请求，都请求共享锁，且已经被 granted，条件很苛刻
      // 如果当前锁请求已经被 granted，且 lock_request 和 target_request 都不是独占锁请求
      const auto isCompatible = lock_request.granted_ &&  // all **earlier** requests have been granted already
                                lock_request.lock_mode_ != LockMode::EXCLUSIVE &&
                                target_request.lock_mode_ != LockMode::EXCLUSIVE;
      if (!isCompatible) {
        return false;
      }
    }
    return true;
  }

 private:
  // 隐式 abort
  void AbortImplicitly(Transaction *txn, AbortReason abort_reason);

  // DFS
  bool ProcessDFSTree(txn_id_t *txn_id, std::stack<txn_id_t> *stack,
                      std::unordered_map<txn_id_t, VisitedType> *visited);

  // 获取栈中最早的事务
  txn_id_t GetYoungestTransactionInCycle(std::stack<txn_id_t> *stack, txn_id_t vertex);

  // 构建等待图
  void BuildWaitsForGraph();

  std::mutex latch_;                            // lock_manager 全局锁
  std::atomic<bool> enable_cycle_detection_{};  // 是否开启死锁检查
  std::thread *cycle_detection_thread_{};       // 死锁检查线程

  /** Lock table for lock requests. */
  std::unordered_map<RID, LockRequestQueue> lock_table_{};  // RID 加锁队列表
  /** Waits-for graph representation. */
  std::unordered_map<txn_id_t, std::vector<txn_id_t>> waits_for_{};  // 事务等待队列表
};

}  // namespace bustub
