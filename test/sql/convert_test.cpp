//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// convert_test.cpp
//
// Identification: test/include/convert_test.cpp
//
//===----------------------------------------------------------------------===//

#include <iostream>

#include "gtest/gtest.h"
#include "sql/convert.h"
#include "util/sqlhelper.h"

namespace bustub {

TEST(StarterTest, SQLParseTest) {
  std::unique_ptr<Convertor> conv1_ptr{new Convertor()};

  bool ok = conv1_ptr->Parse("select * from test;");
  EXPECT_TRUE(ok);

  auto &result = conv1_ptr->GetResult();
  EXPECT_EQ(result.size(), 1);

  for (auto i = 0U; i < result.size(); ++i) {
    // Print a statement summary.
    hsql::printStatementInfo(result.getStatement(i));
  }

  for (const auto &item : result.getStatements()) {
    switch (item->type()) {
      case hsql::kStmtError:
        break;
      case hsql::kStmtSelect: {
        auto *select_statement = dynamic_cast<hsql::SelectStatement *>(item);
        EXPECT_STREQ(select_statement->fromTable->getName(), "test");
      } break;
      case hsql::kStmtImport:
        break;
      case hsql::kStmtInsert:
        break;
      case hsql::kStmtUpdate:
        break;
      case hsql::kStmtDelete:
        break;
      case hsql::kStmtCreate:
        break;
      case hsql::kStmtDrop:
        break;
      case hsql::kStmtPrepare:
        break;
      case hsql::kStmtExecute:
        break;
      case hsql::kStmtExport:
        break;
      case hsql::kStmtRename:
        break;
      case hsql::kStmtAlter:
        break;
      case hsql::kStmtShow:
        break;
      case hsql::kStmtTransaction:
        break;
    }
  }
}

}  // namespace bustub