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

namespace bustub {

class Convertor {
 public:
  Convertor() = default;
  ~Convertor() = default;

  bool Parse(const std::string &raw_query);
  hsql::SQLParserResult &GetResult();

 private:
  std::string raw_query_;
  hsql::SQLParserResult result_;
};

}  // namespace bustub