//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void LimitExecutor::Init() {
  offset_ = 0;
  limit_ = 0;
  child_executor_->Init();
}

bool LimitExecutor::Next(Tuple *tuple, RID *rid) {
  size_t limit = plan_->GetLimit();
  size_t offset = plan_->GetOffset();
  while (offset_ < offset) {
    // 小于 offset 的 Tuple 废弃
    bool ok = child_executor_->Next(tuple, rid);
    if (ok) {
      offset_++;
    }
  }
  // 超过了 limit 直接 false
  if (limit_ >= limit) {
    return false;
  }
  bool ok = child_executor_->Next(tuple, rid);
  if (ok) {
    limit_++;
  }
  return ok;
}

}  // namespace bustub
