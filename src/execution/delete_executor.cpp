//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  table_info_ = exec_ctx->GetCatalog()->GetTable(plan_->TableOid());
}

void DeleteExecutor::Init() {
  child_executor_->Init();
  table_indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Tuple tup;
  RID emit_rid;
  bool found = child_executor_->Next(&tup, &emit_rid);
  if (!found) {
    return false;
  }
  // 如果处于读锁状态
  if (exec_ctx_->GetTransaction()->IsSharedLocked(emit_rid)) {
    // 尝试进行锁升级
    if (!exec_ctx_->GetLockManager()->LockUpgrade(exec_ctx_->GetTransaction(), emit_rid)) {
      // 升级失败，直接返回
      return false;
    }
  } else if (!exec_ctx_->GetTransaction()->IsExclusiveLocked(emit_rid) &&
             !exec_ctx_->GetLockManager()->LockExclusive(exec_ctx_->GetTransaction(), emit_rid)) {
    // 没有不在写锁状态，但是加写锁失败，直接返回
    return false;
  }

  bool deleted = table_info_->table_->MarkDelete(emit_rid, exec_ctx_->GetTransaction());
  if (deleted) {
    // 删除索引
    std::for_each(table_indexes_.begin(), table_indexes_.end(),
                  [&tup, &emit_rid, &table_info = table_info_, &ctx = exec_ctx_](IndexInfo *index_info) {
                    // 没有更新这个操作，因此先删除，后插入
                    index_info->index_->DeleteEntry(tup.KeyFromTuple(table_info->schema_, index_info->key_schema_,
                                                                     index_info->index_->GetKeyAttrs()),
                                                    emit_rid, ctx->GetTransaction());
                    // 新建索引写记录
                    IndexWriteRecord record{emit_rid, table_info->oid_,       WType::DELETE,    tup,
                                            Tuple{},  index_info->index_oid_, ctx->GetCatalog()};
                    ctx->GetTransaction()->GetIndexWriteSet()->push_back(record);
                  });
  }
  return deleted;
}

}  // namespace bustub
