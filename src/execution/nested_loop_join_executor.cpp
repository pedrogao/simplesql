//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple left_tuple;
  RID left_rid;
  Tuple right_tuple;
  RID right_rid;
  // nested loop join 是双重循环
  // foreach l in left
  //   foreach r in right
  //     emit l match r
  bool done = false;
  while (left_executor_->Next(&left_tuple, &left_rid)) {
    while (right_executor_->Next(&right_tuple, &right_rid)) {
      // tuples are returned if predicate(tuple) = true or predicate = nullptr
      if (plan_->Predicate() == nullptr || plan_->Predicate()
                                               ->EvaluateJoin(&left_tuple, left_executor_->GetOutputSchema(),
                                                              &right_tuple, right_executor_->GetOutputSchema())
                                               .GetAs<bool>()) {
        done = true;
        break;
      }
    }
    if (done) {
      break;
    }
    right_executor_->Init();  // 重新初始化
  }
  if (!done) {
    return false;
  }

  // 判断 join 的左右两边
  switch (exec_ctx_->GetTransaction()->GetIsolationLevel()) {
    case IsolationLevel::READ_UNCOMMITTED:
      // 读未提交，直接 break
      break;
    case IsolationLevel::READ_COMMITTED:
      // 读已提交
      // 1.没有加读锁;
      // 2.也没有加写锁;
      // 3.不能加读锁后，立即解锁成功
      if (!exec_ctx_->GetTransaction()->IsSharedLocked(left_rid) &&
          !exec_ctx_->GetTransaction()->IsExclusiveLocked(left_rid) &&
          !(exec_ctx_->GetLockManager()->LockShared(exec_ctx_->GetTransaction(), left_rid) &&
            exec_ctx_->GetLockManager()->Unlock(exec_ctx_->GetTransaction(), left_rid))) {
        return false;
      }
      if (!exec_ctx_->GetTransaction()->IsSharedLocked(right_tuple.GetRid()) &&
          !exec_ctx_->GetTransaction()->IsExclusiveLocked(right_tuple.GetRid()) &&
          !(exec_ctx_->GetLockManager()->LockShared(exec_ctx_->GetTransaction(), right_tuple.GetRid()) &&
            exec_ctx_->GetLockManager()->Unlock(exec_ctx_->GetTransaction(), right_tuple.GetRid()))) {
        return false;
      }
      break;
    case IsolationLevel::REPEATABLE_READ:
      // 可重读读
      // 1. 没有加读锁
      // 2. 没有加写锁
      // 3. 且不能加读锁
      if (!exec_ctx_->GetTransaction()->IsSharedLocked(left_rid) &&
          !exec_ctx_->GetTransaction()->IsExclusiveLocked(left_rid) &&
          !exec_ctx_->GetLockManager()->LockShared(exec_ctx_->GetTransaction(), left_rid)) {
        return false;
      }
      if (!exec_ctx_->GetTransaction()->IsSharedLocked(right_tuple.GetRid()) &&
          !exec_ctx_->GetTransaction()->IsExclusiveLocked(right_tuple.GetRid()) &&
          !exec_ctx_->GetLockManager()->LockShared(exec_ctx_->GetTransaction(), right_tuple.GetRid())) {
        return false;
      }
      break;
    default:
      break;
  }

  std::vector<Value> values;
  std::transform(plan_->OutputSchema()->GetColumns().begin(), plan_->OutputSchema()->GetColumns().end(),
                 std::back_inserter(values),
                 [&left_tuple = left_tuple, &left_executor = left_executor_, &right_tuple = right_tuple,
                  &right_executor = right_executor_](const Column &col) {
                   return col.GetExpr()->EvaluateJoin(&left_tuple, left_executor->GetOutputSchema(), &right_tuple,
                                                      right_executor->GetOutputSchema());
                 });
  // 赋值
  *tuple = Tuple{values, plan_->OutputSchema()};
  return true;
}

}  // namespace bustub
