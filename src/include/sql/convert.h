//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// convert.h
//
// Identification: src/include/sql/convert.h
//
// Copyright (c) 2022-2022, pedrogao
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include "SQLParser.h"

#include "execution/plans/abstract_plan.h"

namespace bustub {

class Convertor {
 public:
  Convertor() = default;
  ~Convertor() = default;

  bool Parse(const std::string &raw_query);
  hsql::SQLParserResult &GetResult();
  AbstractPlanNode *Do();

 private:
  std::string raw_query_;
  hsql::SQLParserResult result_;
  std::unique_ptr<AbstractPlanNode> plan_;
};

}  // namespace bustub