//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  auto index_id = plan->GetIndexOid();
  index_info_ = exec_ctx->GetCatalog()->GetIndex(index_id);
  table_metadata_ = exec_ctx->GetCatalog()->GetTable(index_info_->table_name_);
}

void IndexScanExecutor::Init() {
  auto b_index = dynamic_cast<BPLUSTREE_INDEX_TYPE *>(index_info_->index_.get());
  index_iterator_ = std::make_unique<INDEXITERATOR_TYPE>(b_index->GetBeginIterator());
}

bool IndexScanExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple tup;
  // 迭代
  do {
    auto b_index = dynamic_cast<BPLUSTREE_INDEX_TYPE *>(index_info_->index_.get());
    if (*index_iterator_ == b_index->GetEndIterator()) {
      return false;
    }
    bool found = table_metadata_->table_->GetTuple((*(*index_iterator_)).second, &tup, exec_ctx_->GetTransaction());
    if (!found) {
      return false;
    }
    ++(*index_iterator_);
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
