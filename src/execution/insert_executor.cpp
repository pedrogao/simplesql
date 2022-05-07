//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  table_metadata_ = exec_ctx->GetCatalog()->GetTable(plan->TableOid());
}

void InsertExecutor::Init() {
  if (child_executor_ != nullptr) {
    child_executor_->Init();
  }
  table_indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_metadata_->name_);
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Tuple tup;
  if (plan_->IsRawInsert()) {
    if (next_insert_ >= plan_->RawValues().size()) {
      return false;
    }
    tup = Tuple(plan_->RawValuesAt(next_insert_), &table_metadata_->schema_);
    ++next_insert_;
  } else {
    RID emid_rid;
    if (!child_executor_->Next(&tup, &emid_rid)) {
      return false;
    }
  }
  bool ok = table_metadata_->table_->InsertTuple(tup, rid, exec_ctx_->GetTransaction());
  if (ok) {
    // 锁住新插入的 RID
    exec_ctx_->GetLockManager()->LockExclusive(exec_ctx_->GetTransaction(), *rid);
    // 插入索引数据
    std::for_each(
        table_indexes_.begin(), table_indexes_.end(),
        [&tup, &rid, &table_metadata = table_metadata_, &ctx = exec_ctx_](IndexInfo *index_info) {
          index_info->index_->InsertEntry(
              tup.KeyFromTuple(table_metadata->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()),
              *rid, ctx->GetTransaction());
          // 加入到事务索引写集合中
          ctx->GetTransaction()->GetIndexWriteSet()->emplace_back(*rid, table_metadata->oid_, WType::INSERT, tup,
                                                                  Tuple{}, index_info->index_oid_, ctx->GetCatalog());
        });
  }
  return ok;
}

}  // namespace bustub
