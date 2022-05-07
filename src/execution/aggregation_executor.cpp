//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

void AggregationExecutor::Init() {
  child_->Init();
  // 将数据从 child 中搬到 aht 中
  Tuple tup;
  RID rid;
  while (child_->Next(&tup, &rid)) {
    switch (exec_ctx_->GetTransaction()->GetIsolationLevel()) {
      case IsolationLevel::READ_UNCOMMITTED:
        break;  // 读未提交，未加任何锁，直接 break
      case IsolationLevel::READ_COMMITTED:
        // 读已提交
        // 1.没有加读锁;
        // 2.也没有加写锁;
        // 3.不能加读锁后，立即解锁成功
        if (!exec_ctx_->GetTransaction()->IsSharedLocked(rid) && !exec_ctx_->GetTransaction()->IsExclusiveLocked(rid) &&
            !(exec_ctx_->GetLockManager()->LockShared(exec_ctx_->GetTransaction(), rid) &&
              exec_ctx_->GetLockManager()->Unlock(exec_ctx_->GetTransaction(), rid))) {
          return;
        }
        break;
      case IsolationLevel::REPEATABLE_READ:
        // 可重读读
        // 1. 没有加读锁
        // 2. 没有加写锁
        // 3. 且不能加读锁
        if (!exec_ctx_->GetTransaction()->IsSharedLocked(rid) && !exec_ctx_->GetTransaction()->IsExclusiveLocked(rid) &&
            !exec_ctx_->GetLockManager()->LockShared(exec_ctx_->GetTransaction(), rid)) {
          return;
        }
        break;
      default:
        break;
    }
    aht_.InsertCombine(MakeKey(&tup), MakeVal(&tup));
  }
  aht_iterator_ = aht_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  // Hint: You will want to aggregate the results and make use of the HAVING for constraints. In particular, take a look
  // at AbstractExpression::EvaluateAggregate, which handles aggregation evaluation for different types of expressions.
  // Note that this returns a Value, which you can GetAs<bool>.
  std::vector<Value> group_bys;
  std::vector<Value> aggregates;

  do {
    if (aht_iterator_ == aht_.End()) {
      return false;
    }
    group_bys = aht_iterator_.Key().group_bys_;
    aggregates = aht_iterator_.Val().aggregates_;
    ++aht_iterator_;
  } while (plan_->GetHaving() != nullptr &&
           !(plan_->GetHaving()->EvaluateAggregate(group_bys, aggregates).GetAs<bool>()));
  // 拷贝数据
  std::vector<Value> values;
  std::transform(plan_->OutputSchema()->GetColumns().begin(), plan_->OutputSchema()->GetColumns().end(),
                 std::back_inserter(values), [&group_bys, &aggregates](const Column &col) {
                   return col.GetExpr()->EvaluateAggregate(group_bys, aggregates);
                 });
  *tuple = Tuple{values, plan_->OutputSchema()};
  return true;
}

}  // namespace bustub
