//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  table_metadata_ = exec_ctx->GetCatalog()->GetTable(plan->GetTableOid());
}

void SeqScanExecutor::Init() {
  table_iterator_ = std::make_unique<TableIterator>(table_metadata_->table_->Begin(exec_ctx_->GetTransaction()));
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple tup;
  // 谓词不为空，而且执行ok
  // tuples are returned if predicate(tuple) = true or predicate = nullptr
  do {
    // 到末尾了，直接返回
    if (*table_iterator_ == table_metadata_->table_->End()) {
      return false;
    }
    tup = *(*table_iterator_);  // 得到当前 tuple
    ++(*table_iterator_);       // 下一个
  } while (plan_->GetPredicate() != nullptr &&
           !plan_->GetPredicate()->Evaluate(&tup, &(table_metadata_->schema_)).GetAs<bool>());

  // 判断事务隔离级别
  switch (exec_ctx_->GetTransaction()->GetIsolationLevel()) {
    case IsolationLevel::READ_UNCOMMITTED:
      break;  // 读未提交，未加任何锁，直接 break
    case IsolationLevel::READ_COMMITTED:
      // 读已提交
      // 1.没有加读锁;
      // 2.也没有加写锁;
      // 3.不能加读锁后，立即解锁成功
      if (!exec_ctx_->GetTransaction()->IsSharedLocked(tup.GetRid()) &&
          !exec_ctx_->GetTransaction()->IsExclusiveLocked(tup.GetRid()) &&
          !(exec_ctx_->GetLockManager()->LockShared(exec_ctx_->GetTransaction(), tup.GetRid()) &&
            exec_ctx_->GetLockManager()->Unlock(exec_ctx_->GetTransaction(), tup.GetRid()))) {
        return false;
      }
      break;
    case IsolationLevel::REPEATABLE_READ:
      // 可重读读
      // 1. 没有加读锁
      // 2. 没有加写锁
      // 3. 且不能加读锁
      if (!exec_ctx_->GetTransaction()->IsSharedLocked(tup.GetRid()) &&
          !exec_ctx_->GetTransaction()->IsExclusiveLocked(tup.GetRid()) &&
          !exec_ctx_->GetLockManager()->LockShared(exec_ctx_->GetTransaction(), tup.GetRid())) {
        return false;
      }
      break;
    default:
      break;
  }
  // 一个 Tuple 是一条记录，values 是字段值，schema 是字段名称
  std::vector<Value> values;

  std::transform(plan_->OutputSchema()->GetColumns().begin(), plan_->OutputSchema()->GetColumns().end(),
                 std::back_inserter(values), [&tup, &table_metadata_ = table_metadata_](const Column &col) {
                   // Column 是数据列，即字段的定义，调用 Evaluate 获取列数据
                   return col.GetExpr()->Evaluate(&tup, &(table_metadata_->schema_));
                 });

  // 赋值
  *tuple = Tuple{values, plan_->OutputSchema()};
  *rid = tup.GetRid();
  return true;
}

}  // namespace bustub
