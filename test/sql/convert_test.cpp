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

TEST(StarterTest, InsertSQLTest) {
  const std::string query =
      R"(INSERT INTO students(age, grade) VALUES(10, 100); 
      INSERT INTO foods(name, price) VALUES('meat', 99.12);
      INSERT INTO foods VALUES('meat', 99.12);
      )";
  hsql::SQLParserResult result;
  auto ok = hsql::SQLParser::parse(query, &result);
  EXPECT_TRUE(ok);
  EXPECT_EQ(result.size(), 3);

  hsql::SQLStatement *stmt = result.getMutableStatement(0);
  auto *insert_statement = dynamic_cast<hsql::InsertStatement *>(stmt);
  EXPECT_STREQ(insert_statement->tableName, "students");
  EXPECT_EQ(insert_statement->schema, nullptr);
  EXPECT_EQ(insert_statement->columns->size(), 2);
  EXPECT_EQ(insert_statement->values->size(), 2);

  // values & cols
  EXPECT_STREQ(insert_statement->columns->at(0), "age");
  EXPECT_STREQ(insert_statement->columns->at(1), "grade");

  EXPECT_EQ(insert_statement->values->at(0)->isLiteral(), true);
  EXPECT_EQ(insert_statement->values->at(0)->isType(hsql::ExprType::kExprLiteralInt), true);
  EXPECT_EQ(insert_statement->values->at(0)->ival, 10);

  EXPECT_EQ(insert_statement->values->at(1)->isLiteral(), true);
  EXPECT_EQ(insert_statement->values->at(1)->isType(hsql::ExprType::kExprLiteralInt), true);
  EXPECT_EQ(insert_statement->values->at(1)->ival, 100);

  stmt = result.getMutableStatement(1);
  insert_statement = dynamic_cast<hsql::InsertStatement *>(stmt);
  EXPECT_STREQ(insert_statement->tableName, "foods");
  EXPECT_EQ(insert_statement->schema, nullptr);
  EXPECT_EQ(insert_statement->columns->size(), 2);
  EXPECT_EQ(insert_statement->values->size(), 2);

  // values & cols
  EXPECT_STREQ(insert_statement->columns->at(0), "name");
  EXPECT_STREQ(insert_statement->columns->at(1), "price");

  EXPECT_EQ(insert_statement->values->at(0)->isLiteral(), true);
  EXPECT_EQ(insert_statement->values->at(0)->isType(hsql::ExprType::kExprLiteralString), true);
  EXPECT_STREQ(insert_statement->values->at(0)->name, "meat");

  EXPECT_EQ(insert_statement->values->at(1)->isLiteral(), true);
  EXPECT_EQ(insert_statement->values->at(1)->isType(hsql::ExprType::kExprLiteralFloat), true);
  EXPECT_EQ(insert_statement->values->at(1)->fval, 99.12);

  stmt = result.getMutableStatement(2);
  insert_statement = dynamic_cast<hsql::InsertStatement *>(stmt);
  EXPECT_STREQ(insert_statement->tableName, "foods");
  EXPECT_EQ(insert_statement->schema, nullptr);
  EXPECT_EQ(insert_statement->columns, nullptr);  // no columns
  EXPECT_EQ(insert_statement->values->size(), 2);

  EXPECT_EQ(insert_statement->values->at(0)->isLiteral(), true);
  EXPECT_EQ(insert_statement->values->at(0)->isType(hsql::ExprType::kExprLiteralString), true);
  EXPECT_STREQ(insert_statement->values->at(0)->name, "meat");

  EXPECT_EQ(insert_statement->values->at(1)->isLiteral(), true);
  EXPECT_EQ(insert_statement->values->at(1)->isType(hsql::ExprType::kExprLiteralFloat), true);
  EXPECT_EQ(insert_statement->values->at(1)->fval, 99.12);
}

TEST(StarterTest, SelectSQLTest) {
  const std::string query =
      R"(
        SELECT * FROM students; 
        SELECT age, grade FROM students; 
        SELECT name, price FROM foods WHERE name='meat'; 
        SELECT name, price FROM foods WHERE name='meat' AND price > 0 AND price < 100; 
      )";
  hsql::SQLParserResult result;
  auto ok = hsql::SQLParser::parse(query, &result);
  EXPECT_TRUE(ok);
  EXPECT_EQ(result.size(), 4);

  hsql::SQLStatement *stmt = result.getMutableStatement(0);
  auto *select_statement = dynamic_cast<hsql::SelectStatement *>(stmt);
  EXPECT_STREQ(select_statement->fromTable->getName(), "students");
  EXPECT_EQ(select_statement->whereClause, nullptr);
  EXPECT_EQ(select_statement->selectDistinct, false);
  EXPECT_EQ(select_statement->selectList->size(), 1);
  EXPECT_EQ(select_statement->selectList->at(0)->getName(), nullptr);
  EXPECT_EQ(select_statement->selectList->at(0)->isType(hsql::ExprType::kExprStar), true);
  EXPECT_EQ(select_statement->selectList->at(0)->isLiteral(), false);

  stmt = result.getMutableStatement(1);
  select_statement = dynamic_cast<hsql::SelectStatement *>(stmt);
  EXPECT_STREQ(select_statement->fromTable->getName(), "students");
  EXPECT_EQ(select_statement->whereClause, nullptr);
  EXPECT_EQ(select_statement->selectDistinct, false);
  EXPECT_EQ(select_statement->selectList->size(), 2);
  EXPECT_STREQ(select_statement->selectList->at(0)->getName(), "age");
  EXPECT_STREQ(select_statement->selectList->at(1)->getName(), "grade");

  stmt = result.getMutableStatement(2);
  select_statement = dynamic_cast<hsql::SelectStatement *>(stmt);
  EXPECT_STREQ(select_statement->fromTable->getName(), "foods");
  EXPECT_EQ(select_statement->selectDistinct, false);
  EXPECT_EQ(select_statement->selectList->size(), 2);
  EXPECT_STREQ(select_statement->selectList->at(0)->getName(), "name");
  EXPECT_STREQ(select_statement->selectList->at(1)->getName(), "price");

  EXPECT_TRUE(select_statement->whereClause->isType(hsql::ExprType::kExprOperator));
  EXPECT_EQ(select_statement->whereClause->opType, hsql::OperatorType::kOpEquals);
  EXPECT_STREQ(select_statement->whereClause->expr->getName(), "name");
  EXPECT_TRUE(select_statement->whereClause->expr->isType(hsql::ExprType::kExprColumnRef));
  EXPECT_STREQ(select_statement->whereClause->expr2->getName(), "meat");
  EXPECT_TRUE(select_statement->whereClause->expr2->isType(hsql::ExprType::kExprLiteralString));

  stmt = result.getMutableStatement(3);
  select_statement = dynamic_cast<hsql::SelectStatement *>(stmt);
  EXPECT_STREQ(select_statement->fromTable->getName(), "foods");
  EXPECT_EQ(select_statement->selectDistinct, false);
  EXPECT_EQ(select_statement->selectList->size(), 2);
  EXPECT_STREQ(select_statement->selectList->at(0)->getName(), "name");
  EXPECT_STREQ(select_statement->selectList->at(1)->getName(), "price");
  // WHERE name='meat' AND price > 0 AND price < 100;
  EXPECT_TRUE(select_statement->whereClause->isType(hsql::ExprType::kExprOperator));
  EXPECT_EQ(select_statement->whereClause->opType, hsql::OperatorType::kOpAnd);
  // name='meat'
  EXPECT_EQ(select_statement->whereClause->expr->expr->opType, hsql::OperatorType::kOpEquals);
  EXPECT_TRUE(select_statement->whereClause->expr->expr->expr->isType(hsql::ExprType::kExprColumnRef));
  EXPECT_STREQ(select_statement->whereClause->expr->expr->expr->getName(), "name");
  EXPECT_TRUE(select_statement->whereClause->expr->expr->expr2->isType(hsql::ExprType::kExprLiteralString));
  EXPECT_STREQ(select_statement->whereClause->expr->expr->expr2->getName(), "meat");
  // price > 0
  EXPECT_EQ(select_statement->whereClause->expr->expr2->opType, hsql::OperatorType::kOpGreater);
  EXPECT_TRUE(select_statement->whereClause->expr->expr2->expr->isType(hsql::ExprType::kExprColumnRef));
  EXPECT_STREQ(select_statement->whereClause->expr->expr2->expr->getName(), "price");
  EXPECT_TRUE(select_statement->whereClause->expr->expr2->expr2->isType(hsql::ExprType::kExprLiteralInt));
  EXPECT_EQ(select_statement->whereClause->expr->expr2->expr2->ival, 0);
  // price < 100
  EXPECT_EQ(select_statement->whereClause->expr2->opType, hsql::OperatorType::kOpLess);
  EXPECT_STREQ(select_statement->whereClause->expr2->expr->getName(), "price");
  EXPECT_TRUE(select_statement->whereClause->expr2->expr->isType(hsql::ExprType::kExprColumnRef));
  EXPECT_TRUE(select_statement->whereClause->expr2->expr2->isType(hsql::ExprType::kExprLiteralInt));
  EXPECT_EQ(select_statement->whereClause->expr2->expr2->ival, 100);
}

TEST(StarterTest, UpdateSQLTest) {
  const std::string query =
      R"(
        UPDATE students SET age=99, grade='六年级'; 
        UPDATE foods SET price=99.99 WHERE name='meat'; 
        UPDATE foods SET price=99.99 WHERE name='meat' AND price > 0 AND price < 100; 
      )";
  hsql::SQLParserResult result;
  auto ok = hsql::SQLParser::parse(query, &result);
  EXPECT_TRUE(ok);
  EXPECT_EQ(result.size(), 3);

  hsql::SQLStatement *stmt = result.getMutableStatement(0);
  auto *statement = dynamic_cast<hsql::UpdateStatement *>(stmt);
  EXPECT_STREQ(statement->table->getName(), "students");
  EXPECT_EQ(statement->where, nullptr);
  EXPECT_EQ(statement->updates->size(), 2);
  EXPECT_STREQ(statement->updates->at(0)->column, "age");
  EXPECT_EQ(statement->updates->at(0)->value->ival, 99);
  EXPECT_STREQ(statement->updates->at(1)->column, "grade");
  EXPECT_STREQ(statement->updates->at(1)->value->getName(), "六年级");

  stmt = result.getMutableStatement(1);
  statement = dynamic_cast<hsql::UpdateStatement *>(stmt);
  EXPECT_STREQ(statement->table->getName(), "foods");
  EXPECT_EQ(statement->updates->size(), 1);
  EXPECT_STREQ(statement->updates->at(0)->column, "price");
  EXPECT_EQ(statement->updates->at(0)->value->fval, 99.99);
  EXPECT_TRUE(statement->where->isType(hsql::ExprType::kExprOperator));
  EXPECT_EQ(statement->where->opType, hsql::OperatorType::kOpEquals);
  EXPECT_STREQ(statement->where->expr->getName(), "name");
  EXPECT_STREQ(statement->where->expr2->getName(), "meat");

  stmt = result.getMutableStatement(2);
  statement = dynamic_cast<hsql::UpdateStatement *>(stmt);
  EXPECT_STREQ(statement->table->getName(), "foods");
  EXPECT_EQ(statement->updates->size(), 1);
  EXPECT_STREQ(statement->updates->at(0)->column, "price");
  EXPECT_EQ(statement->updates->at(0)->value->fval, 99.99);
  // WHERE name='meat' AND price > 0 AND price < 100;
  EXPECT_TRUE(statement->where->isType(hsql::ExprType::kExprOperator));
  EXPECT_EQ(statement->where->opType, hsql::OperatorType::kOpAnd);
  // name='meat'
  EXPECT_EQ(statement->where->expr->expr->opType, hsql::OperatorType::kOpEquals);
  EXPECT_TRUE(statement->where->expr->expr->expr->isType(hsql::ExprType::kExprColumnRef));
  EXPECT_STREQ(statement->where->expr->expr->expr->getName(), "name");
  EXPECT_TRUE(statement->where->expr->expr->expr2->isType(hsql::ExprType::kExprLiteralString));
  EXPECT_STREQ(statement->where->expr->expr->expr2->getName(), "meat");
  // price > 0
  EXPECT_EQ(statement->where->expr->expr2->opType, hsql::OperatorType::kOpGreater);
  EXPECT_TRUE(statement->where->expr->expr2->expr->isType(hsql::ExprType::kExprColumnRef));
  EXPECT_STREQ(statement->where->expr->expr2->expr->getName(), "price");
  EXPECT_TRUE(statement->where->expr->expr2->expr2->isType(hsql::ExprType::kExprLiteralInt));
  EXPECT_EQ(statement->where->expr->expr2->expr2->ival, 0);
  // price < 100
  EXPECT_EQ(statement->where->expr2->opType, hsql::OperatorType::kOpLess);
  EXPECT_STREQ(statement->where->expr2->expr->getName(), "price");
  EXPECT_TRUE(statement->where->expr2->expr->isType(hsql::ExprType::kExprColumnRef));
  EXPECT_TRUE(statement->where->expr2->expr2->isType(hsql::ExprType::kExprLiteralInt));
  EXPECT_EQ(statement->where->expr2->expr2->ival, 100);
}

TEST(StarterTest, DeleteSQLTest) {
  const std::string query =
      R"(
        DELETE FROM students; 
        DELETE FROM foods WHERE name='meat' AND price > 0 AND price < 100; 
      )";
  hsql::SQLParserResult result;
  auto ok = hsql::SQLParser::parse(query, &result);
  EXPECT_TRUE(ok);
  EXPECT_EQ(result.size(), 2);

  hsql::SQLStatement *stmt = result.getMutableStatement(0);
  auto *statement = dynamic_cast<hsql::DeleteStatement *>(stmt);
  EXPECT_STREQ(statement->tableName, "students");
  EXPECT_EQ(statement->expr, nullptr);

  stmt = result.getMutableStatement(1);
  statement = dynamic_cast<hsql::DeleteStatement *>(stmt);
  EXPECT_STREQ(statement->tableName, "foods");
  // WHERE name='meat' AND price > 0 AND price < 100;
  EXPECT_TRUE(statement->expr->isType(hsql::ExprType::kExprOperator));
  EXPECT_EQ(statement->expr->opType, hsql::OperatorType::kOpAnd);
  // name='meat'
  EXPECT_EQ(statement->expr->expr->expr->opType, hsql::OperatorType::kOpEquals);
  EXPECT_TRUE(statement->expr->expr->expr->expr->isType(hsql::ExprType::kExprColumnRef));
  EXPECT_STREQ(statement->expr->expr->expr->expr->getName(), "name");
  EXPECT_TRUE(statement->expr->expr->expr->expr2->isType(hsql::ExprType::kExprLiteralString));
  EXPECT_STREQ(statement->expr->expr->expr->expr2->getName(), "meat");
  // price > 0
  EXPECT_EQ(statement->expr->expr->expr2->opType, hsql::OperatorType::kOpGreater);
  EXPECT_TRUE(statement->expr->expr->expr2->expr->isType(hsql::ExprType::kExprColumnRef));
  EXPECT_STREQ(statement->expr->expr->expr2->expr->getName(), "price");
  EXPECT_TRUE(statement->expr->expr->expr2->expr2->isType(hsql::ExprType::kExprLiteralInt));
  EXPECT_EQ(statement->expr->expr->expr2->expr2->ival, 0);
  // price < 100
  EXPECT_EQ(statement->expr->expr2->opType, hsql::OperatorType::kOpLess);
  EXPECT_STREQ(statement->expr->expr2->expr->getName(), "price");
  EXPECT_TRUE(statement->expr->expr2->expr->isType(hsql::ExprType::kExprColumnRef));
  EXPECT_TRUE(statement->expr->expr2->expr2->isType(hsql::ExprType::kExprLiteralInt));
  EXPECT_EQ(statement->expr->expr2->expr2->ival, 100);
}

}  // namespace bustub