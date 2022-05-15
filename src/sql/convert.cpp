//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// convert.cpp
//
// Identification: src/sql/convert.cpp
//
// Copyright (c) 2022-2022, pedrogao
//
//===----------------------------------------------------------------------===//

#include "sql/convert.h"
#include "common/exception.h"
#include "common/logger.h"

namespace bustub {

bool Convertor::Parse(const std::string &raw_query) {
  if (raw_query.empty()) {
    throw Exception("raw sql can't be empty");
  }

  LOG_INFO("parse sql: %s", raw_query.c_str());
  hsql::SQLParser::parse(raw_query, &result_);
  return result_.isValid();
}

hsql::SQLParserResult &Convertor::GetResult() { return result_; }

AbstractPlanNode *Convertor::Do() {
  // todo
  return plan_.get();
}

}  // namespace bustub