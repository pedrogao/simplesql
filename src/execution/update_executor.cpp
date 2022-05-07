//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-20, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  table_info_ = exec_ctx->GetCatalog()->GetTable(plan_->TableOid());
}

void UpdateExecutor::Init() {
  child_executor_->Init();
  table_indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  // 从 child_executor_ 执行器中拿到数据然后更新
  Tuple tup;
  if (!child_executor_->Next(&tup, rid)) {
    return false;
  }
  Tuple hit_tup;
  bool found = table_info_->table_->GetTuple(*rid, &hit_tup, exec_ctx_->GetTransaction());
  if (!found) {
    return false;
  }
  *tuple = GenerateUpdatedTuple(hit_tup);

  // 如果处于读锁状态
  if (exec_ctx_->GetTransaction()->IsSharedLocked(*rid)) {
    // 尝试进行锁升级
    if (!exec_ctx_->GetLockManager()->LockUpgrade(exec_ctx_->GetTransaction(), *rid)) {
      // 升级失败，直接返回
      return false;
    }
  } else if (!exec_ctx_->GetTransaction()->IsExclusiveLocked(*rid) &&
             !exec_ctx_->GetLockManager()->LockExclusive(exec_ctx_->GetTransaction(), *rid)) {
    // 没有不在写锁状态，但是加写锁失败，直接返回
    return false;
  }

  bool updated = table_info_->table_->UpdateTuple(*tuple, *rid, exec_ctx_->GetTransaction());
  if (updated) {
    // 更新索引
    std::for_each(
        table_indexes_.begin(), table_indexes_.end(),
        [&hit_tup, &tuple, &rid, &table_info = table_info_, &ctx = exec_ctx_](IndexInfo *index_info) {
          // 没有更新这个操作，因此先删除，后插入
          index_info->index_->DeleteEntry(
              hit_tup.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()),
              *rid, ctx->GetTransaction());
          index_info->index_->InsertEntry(
              tuple->KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()),
              *rid, ctx->GetTransaction());
          ctx->GetTransaction()->GetIndexWriteSet()->emplace_back(*rid, table_info->oid_, WType::UPDATE, *tuple,
                                                                  hit_tup, index_info->index_oid_, ctx->GetCatalog());
        });
  }
  return updated;
}
}  // namespace bustub
