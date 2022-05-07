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

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  auto table_id = plan_->GetInnerTableOid();
  table_info_ = exec_ctx->GetCatalog()->GetTable(table_id);
  index_info_ = exec_ctx->GetCatalog()->GetIndex(plan_->GetIndexName(), table_info_->name_);
}

void NestIndexJoinExecutor::Init() { child_executor_->Init(); }

bool NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple left_tuple;
  RID left_rid;
  Tuple right_tuple;
  // Hint: You will want to fetch the tuple from the outer table, construct the index probe key by looking up the column
  // value and index key schema, and then look up the RID in the index to retrieve the corresponding tuple for the inner
  // table.
  //
  // Hint: You may assume the inner tuple is always valid (i.e. no predicate)
  bool done = false;
  while (child_executor_->Next(&left_tuple, &left_rid)) {
    // tuples are returned if predicate(tuple) = true or predicate = nullptr
    // child0 是 inner table
    Value val = plan_->Predicate()->GetChildAt(0)->EvaluateJoin(&left_tuple, plan_->OuterTableSchema(), &right_tuple,
                                                                &table_info_->schema_);
    Tuple probe{std::vector<Value>{val}, index_info_->index_->GetKeySchema()};
    auto b_index = dynamic_cast<BPlusTreeIndex<KeyType, ValueType, KeyComparator> *>(index_info_->index_.get());
    std::vector<RID> rids;
    b_index->ScanKey(probe, &rids, exec_ctx_->GetTransaction());
    if (rids.empty()) {
      done = true;
      break;
    }
    bool ok = table_info_->table_->GetTuple(rids[0], &right_tuple, exec_ctx_->GetTransaction());
    if (!ok) {
      done = true;
      break;
    }
    if (plan_->Predicate() == nullptr ||
        plan_->Predicate()
            ->EvaluateJoin(&left_tuple, plan_->OuterTableSchema(), &right_tuple, plan_->InnerTableSchema())
            .GetAs<bool>()) {
      done = true;
      break;
    }
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
                 [&left_tuple = left_tuple, &right_tuple = right_tuple, &plan = plan_](const Column &col) {
                   return col.GetExpr()->EvaluateJoin(&left_tuple, plan->OuterTableSchema(), &right_tuple,
                                                      plan->InnerTableSchema());
                 });
  // 赋值
  *tuple = Tuple{values, plan_->OutputSchema()};
  return true;
}

}  // namespace bustub
