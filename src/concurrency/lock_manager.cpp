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
#include "concurrency/transaction_manager.h"

namespace bustub {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  // 事务共享锁加入 rid
  // Transaction txn tries to take a shared lock on record id rid.
  // This should be blocked on waiting and should return true when granted.
  // Return false if transaction is rolled back (aborts).
  // 如果是读未提交，那么直接 abort
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    AbortImplicitly(txn, AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
    return false;
  }
  // 如果是可重复读，且事务处于 shrinking 状态，那么直接 abort
  // 因为 shrinking 状态下，事务不能再加锁
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && txn->GetState() == TransactionState::SHRINKING) {
    AbortImplicitly(txn, AbortReason::LOCK_ON_SHRINKING);
    return false;
  }
  // 如果 rid 已经被锁了，那么直接返回 true
  if (txn->IsSharedLocked(rid) || txn->IsExclusiveLocked(rid)) {
    return true;
  }
  // latch_ 用来保护 lock_table
  // lock_table 中保存了 rid 对应的锁队列
  std::unique_lock<std::mutex> latch(latch_);
  auto &lock_request_queue = lock_table_[rid];
  latch.unlock();  // 解锁

  // 操作 rid 对应的队列，对队列加锁
  std::unique_lock<std::mutex> queue_latch(lock_request_queue.latch_);
  // 将事务id和加锁模式加入到队列
  auto &lock_request =
      lock_request_queue.request_queue_.emplace_back(txn->GetTransactionId(), LockManager::LockMode::SHARED);

  // 加入后等待
  lock_request_queue.cv_.wait(queue_latch, [&lock_request_queue, &lock_request, &txn] {
    // 如果事务 abort，或者当前锁请求与队列中的其它锁兼容，比如都是共享锁，那么直接停止等待，继续执行
    return LockManager::IsLockCompatible(lock_request_queue, lock_request) ||
           txn->GetState() == TransactionState::ABORTED;
  });
  // abort，直接返回
  if (txn->GetState() == TransactionState::ABORTED) {
    AbortImplicitly(txn, AbortReason::DEADLOCK);
  }
  // 授予锁
  lock_request.granted_ = true;
  txn->GetSharedLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  // 事务独占锁加入 rid
  // shrinking A transaction may release locks, but may not obtain any new locks.
  // 因为 shrinking 状态下，事务不能再加锁
  if (txn->GetState() == TransactionState::SHRINKING) {
    AbortImplicitly(txn, AbortReason::LOCK_ON_SHRINKING);
    return false;
  }
  // 已经处于排他锁
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }
  std::unique_lock<std::mutex> latch(latch_);
  auto &lock_request_queue = lock_table_[rid];
  latch.unlock();

  std::unique_lock<std::mutex> queue_latch(lock_request_queue.latch_);
  auto &lock_request =
      lock_request_queue.request_queue_.emplace_back(txn->GetTransactionId(), LockManager::LockMode::EXCLUSIVE);
  // 在此处等待，直到 lock 被兼容，或者 abort
  lock_request_queue.cv_.wait(queue_latch, [&lock_request_queue, &lock_request, &txn] {
    return LockManager::IsLockCompatible(lock_request_queue, lock_request) ||
           txn->GetState() == TransactionState::ABORTED;
  });
  // 如果 abort，直接返回
  if (txn->GetState() == TransactionState::ABORTED) {
    AbortImplicitly(txn, AbortReason::DEADLOCK);
    return false;
  }
  lock_request.granted_ = true;
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  // 锁升级，就是将 rid 从共享锁中删除，然后加入独占锁
  // 因为 shrinking 状态下，事务不能再加锁
  if (txn->GetState() == TransactionState::SHRINKING) {
    AbortImplicitly(txn, AbortReason::LOCK_ON_SHRINKING);
    return false;
  }
  // 不同升级了
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }

  std::unique_lock<std::mutex> latch(latch_);
  auto &lock_request_queue = lock_table_[rid];
  latch.unlock();

  std::unique_lock<std::mutex> queue_latch(lock_request_queue.latch_);
  // 正在处于升级，就不用再升了吧
  if (lock_request_queue.upgrading_) {
    AbortImplicitly(txn, AbortReason::UPGRADE_CONFLICT);
    return false;
  }
  // 抢到了升级机会
  lock_request_queue.upgrading_ = true;  // 升级成功
  // 找到需要升级锁的事务
  auto lock_rq = std::find_if(
      lock_request_queue.request_queue_.begin(), lock_request_queue.request_queue_.end(),
      [&txn](const LockManager::LockRequest &lock_req) { return txn->GetTransactionId() == lock_req.txn_id_; });
  // 未找到，直接报错
  BUSTUB_ASSERT(lock_rq != lock_request_queue.request_queue_.end(), "Cannot find lock request when upgrade lock");
  // 没有 grant，直接报错
  BUSTUB_ASSERT(lock_rq->granted_, "Lock request has not be granted");
  // 处于 shared 状态下才能升级，否则报错
  BUSTUB_ASSERT(lock_rq->lock_mode_ == LockManager::LockMode::SHARED, "Lock request is not locked in SHARED mode");
  // 如果不在 shared 状态，那么报错
  BUSTUB_ASSERT(txn->IsSharedLocked(rid), "Rid is not shared locked by transaction when upgrade");
  // 如果处在 exclusive，那么报错
  BUSTUB_ASSERT(!txn->IsExclusiveLocked(rid), "Rid is currently exclusive locked by transaction when upgrade");
  // 升级为独占锁
  lock_rq->lock_mode_ = LockManager::LockMode::EXCLUSIVE;
  lock_rq->granted_ = false;  // 暂时还没有 grant

  // 等待兼容或者 abort，在队列中等待，一个 RID 有一个等待队列
  lock_request_queue.cv_.wait(queue_latch, [&lock_request_queue, &lock_req = *lock_rq, &txn] {
    return LockManager::IsLockCompatible(lock_request_queue, lock_req) || txn->GetState() == TransactionState::ABORTED;
  });
  // 如果 abort
  if (txn->GetState() == TransactionState::ABORTED) {
    AbortImplicitly(txn, AbortReason::DEADLOCK);
    return false;
  }
  // 一切成功，那么 grant，且结束升级
  lock_rq->granted_ = true;
  lock_request_queue.upgrading_ = false;

  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  // 从共享锁和独占锁中清除 rid
  std::unique_lock<std::mutex> latch(latch_);
  auto &lock_request_queue = lock_table_[rid];
  latch.unlock();

  std::unique_lock<std::mutex> queue_latch(lock_request_queue.latch_);
  // 如果是可重复读级别，且当前事务状态为 GROWING，那么设置为 SHRINKING
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && txn->GetState() == TransactionState::GROWING) {
    txn->SetState(TransactionState::SHRINKING);
  }
  // 找到队列中的事务
  auto lock_rq = std::find_if(
      lock_request_queue.request_queue_.begin(), lock_request_queue.request_queue_.end(),
      [&txn](const LockManager::LockRequest &lock_req) { return txn->GetTransactionId() == lock_req.txn_id_; });
  // 没有找到直接报错
  BUSTUB_ASSERT(lock_rq != lock_request_queue.request_queue_.end(), "Cannot find lock request when unlock");

  // 从队列中删除 lock_rq
  auto following_it = lock_request_queue.request_queue_.erase(lock_rq);

  // 通知其它在队列中等待的事务
  // 如果删除成功，且 granted 为 false，且与队列中其它锁兼容，那么通知其它队列抢锁
  if (following_it != lock_request_queue.request_queue_.end() && !following_it->granted_ &&
      LockManager::IsLockCompatible(lock_request_queue, *following_it)) {
    lock_request_queue.cv_.notify_all();
  }

  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);
  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  // 获取 t1 上的所有边，列表
  auto &v = waits_for_[t1];
  auto it = std::lower_bound(v.begin(), v.end(), t2);
  // 已经存在 v1->v2 的边，直接返回
  if (it != v.end() && *it == t2) {
    return;
  }
  v.insert(it, t2);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  auto &v = waits_for_[t1];
  auto it = std::find(v.begin(), v.end(), t2);
  if (it != v.end()) {
    v.erase(it);
  }
}

bool LockManager::HasCycle(txn_id_t *txn_id) {
  // Looks for a cycle by using the Depth First Search (DFS) algorithm.
  // If it finds a cycle, HasCycle should store the transaction id of the youngest transaction in the cycle in txn_id
  // and return true. Your function should return the first cycle it finds. If your graph has no cycles, HasCycle should
  // return false.
  // DFS 寻找死锁
  // 得到所有出发点
  std::vector<txn_id_t> vertices;
  std::transform(waits_for_.begin(), waits_for_.end(), std::back_inserter(vertices),
                 [](const auto &pair) { return pair.first; });
  // 根据大小排序
  std::sort(vertices.begin(), vertices.end());

  std::unordered_map<txn_id_t, LockManager::VisitedType> visited;

  for (auto &&v : vertices) {
    // vertex is NOT_VISITED
    if (auto it = visited.find(v); it == visited.end()) {
      std::stack<txn_id_t> stack;
      stack.push(v);  // v->
      visited.emplace(v, LockManager::VisitedType::IN_STACK);
      // 深度优先遍历，检查死锁
      auto has_cycle = ProcessDFSTree(txn_id, &stack, &visited);
      if (has_cycle) {
        return true;
      }
    }
  }
  return false;
}

std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() {
  // Returns a list of tuples representing the edges in your graph. We will use this to test correctness of your graph.
  // A pair (t1,t2) corresponds to an edge from t1 to t2.
  // 获取所有的边，直接遍历并且 make_pair
  std::vector<std::pair<txn_id_t, txn_id_t>> ret{};
  for (const auto &[txn_id, txn_id_v] : waits_for_) {
    // 将 txn_id_v 列表中的 txn 与 txn_id 一一组合，然后推入 ret 中
    std::transform(txn_id_v.begin(), txn_id_v.end(), std::back_inserter(ret),
                   [&t1 = txn_id](const auto &t2) { return std::make_pair(t1, t2); });
  }
  return ret;
}

void LockManager::RunCycleDetection() {
  // 检查是否存在死锁
  // You should not be maintaining a graph, it should be built and destroyed every time the thread wakes up.
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      std::unique_lock<std::mutex> latch(latch_);
      if (!enable_cycle_detection_) {
        break;
      }

      waits_for_.clear();
      BuildWaitsForGraph();

      txn_id_t txn_id;
      while (HasCycle(&txn_id)) {
        // 得到死锁的 txn_id
        auto txn = TransactionManager::GetTransaction(txn_id);
        txn->SetState(TransactionState::ABORTED);  // abort
        // 清除 txn_id 上的锁
        // 得到 txn_id 上所有的边
        for (const auto &wait_on_txn_id : waits_for_[txn_id]) {
          auto wait_on_txn = TransactionManager::GetTransaction(wait_on_txn_id);
          // 得到事务上的所有 share、exclusive 的 RID
          std::unordered_set<RID> lock_set;
          lock_set.insert(wait_on_txn->GetSharedLockSet()->begin(), wait_on_txn->GetSharedLockSet()->end());
          lock_set.insert(wait_on_txn->GetExclusiveLockSet()->begin(), wait_on_txn->GetExclusiveLockSet()->end());
          for (auto locked_rid : lock_set) {
            // 通知
            lock_table_[locked_rid].cv_.notify_all();
          }
        }
        // 重新建立图
        waits_for_.clear();
        BuildWaitsForGraph();
      }
    }
  }
}

void LockManager::AbortImplicitly(Transaction *txn, AbortReason abort_reason) {
  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txn->GetTransactionId(), abort_reason);
}

bool LockManager::ProcessDFSTree(txn_id_t *txn_id, std::stack<txn_id_t> *stack,
                                 std::unordered_map<txn_id_t, VisitedType> *visited) {
  bool has_cycle = false;

  for (auto &&v : waits_for_[stack->top()]) {
    auto it = visited->find(v);

    // find a cycle
    if (it != visited->end() && it->second == LockManager::VisitedType::IN_STACK) {
      *txn_id = GetYoungestTransactionInCycle(stack, v);
      has_cycle = true;
      break;
    }

    // v is NOT_VISITED
    if (it == visited->end()) {
      stack->push(v);
      visited->emplace(v, LockManager::VisitedType::IN_STACK);

      has_cycle = ProcessDFSTree(txn_id, stack, visited);
      if (has_cycle) {
        break;
      }
    }
  }

  visited->insert_or_assign(stack->top(), LockManager::VisitedType::VISITED);
  stack->pop();

  return has_cycle;
}

txn_id_t LockManager::GetYoungestTransactionInCycle(std::stack<txn_id_t> *stack, txn_id_t vertex) {
  txn_id_t max_txn_id = 0;
  std::stack<txn_id_t> tmp;
  tmp.push(stack->top());
  stack->pop();

  while (tmp.top() != vertex) {
    tmp.push(stack->top());
    stack->pop();
  }

  while (!tmp.empty()) {
    max_txn_id = std::max(max_txn_id, tmp.top());
    stack->push(tmp.top());
    tmp.pop();
  }

  return max_txn_id;
}

void LockManager::BuildWaitsForGraph() {
  for (const auto &it : lock_table_) {
    const auto queue = it.second.request_queue_;
    std::vector<txn_id_t> holdings;
    std::vector<txn_id_t> waitings;

    for (const auto &lock_request : queue) {
      const auto txn = TransactionManager::GetTransaction(lock_request.txn_id_);
      if (txn->GetState() == TransactionState::ABORTED) {
        continue;
      }

      if (lock_request.granted_) {
        holdings.push_back(lock_request.txn_id_);
      } else {
        waitings.push_back(lock_request.txn_id_);
      }
    }

    for (auto &&t1 : waitings) {
      for (auto &&t2 : holdings) {
        AddEdge(t1, t2);
      }
    }
  }
}

}  // namespace bustub
