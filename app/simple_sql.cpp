#include <iostream>
#include <map>
#include <string>

#include "SQLParser.h"
#include "catalog/catalog.h"
#include "common/bustub_instance.h"
#include "common/config.h"
#include "common/logger.h"
#include "execution/execution_engine.h"
#include "execution/executor_context.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/delete_plan.h"
#include "execution/plans/insert_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/update_plan.h"
#include "tuple_util.h"
#include "util/sqlhelper.h"

using namespace bustub;

// ./bin/simple_sql "CREATE TABLE students (name VARCHAR(20), age INTEGER, grade INTEGER);"
// ./bin/simple_sql "CREATE TABLE students (age INTEGER, grade INTEGER); INSERT INTO students(age, grade) VALUES(10,
// 10);"

class SimpleSQL {
 public:
  SimpleSQL(std::string &&db_path) {
    db_ = new BustubInstance(db_path);
    catalog_ = new Catalog(db_->buffer_pool_manager_, db_->lock_manager_, db_->log_manager_);
  }

  ~SimpleSQL() {
    delete catalog_;
    delete db_;
  }

  hsql::SQLParserResult ParseSQL(std::string &query) {
    hsql::SQLParserResult result;
    hsql::SQLParser::parse(query, &result);
    return result;
  }

  Transaction *BeginTransaction() { return db_->transaction_manager_->Begin(); }

  void CommitTransaction(Transaction *txn) { db_->transaction_manager_->Commit(txn); }

  BustubInstance *GetDB() { return db_; }

  Catalog *GetCatalog() { return catalog_; }

 private:
  BustubInstance *db_;
  Catalog *catalog_;
};

Column GetCol(const hsql::ColumnDefinition *def) {
  switch (def->type.data_type) {
    case hsql::DataType::INT:
      return Column{std::string(def->name), TypeId::INTEGER};
    case hsql::DataType::VARCHAR:
      return Column{std::string(def->name), TypeId::VARCHAR, uint32_t(def->type.length)};  // VARCHAR 现在不支持
    case hsql::DataType::SMALLINT:
      return Column{std::string(def->name), TypeId::SMALLINT};
    default:
      return Column{std::string(def->name), TypeId::INVALID};
  }
}

int main(int argc, char *argv[]) {
  if (argc <= 1) {
    std::cerr << "Usage: ./example \"SELECT * FROM test;" << std::endl;
    return -1;
  }

  SimpleSQL db_instance("test.db");

  std::string query = argv[1];
  // parse a given query
  auto result = db_instance.ParseSQL(query);
  // check whether the parsing was successful
  if (!result.isValid()) {
    std::cerr << "Given string is not a valid SQL query" << std::endl;
    std::cerr << result.errorMsg() << ", at (" << result.errorLine() << ":" << result.errorColumn() << ")\n"
              << std::endl;
    return -1;
  }

  std::cout << "Parsed successfully!" << std::endl;
  std::cout << "Number of statements: " << result.size() << std::endl;

  auto txn = db_instance.BeginTransaction();  // start

  for (const auto &stmt : result.getStatements()) {
    hsql::printStatementInfo(stmt);

    switch (stmt->type()) {
      case hsql::kStmtError:
        break;
      case hsql::kStmtSelect: {
        auto *select_statement = dynamic_cast<hsql::SelectStatement *>(stmt);
        std::cout << "select from: " << select_statement->fromTable->getName() << std::endl;
        auto table_meta = db_instance.GetCatalog()->GetTable(select_statement->fromTable->getName());
        std::vector<Column> cols;
        for (const auto expr : *select_statement->selectList) {
          std::cout << "expr: " << expr->isLiteral() << std::endl;
          std::cout << "expr name: " << expr->name << std::endl;
          Column col{std::string(expr->getName()), TypeId::INTEGER};
          cols.push_back(col);
          std::cout << "column def : " << col.ToString() << std::endl;
        }
        // TODO where
        // select_statement->whereClause
        ColumnValueExpression expB(1, 1, TypeId::SMALLINT);
        ConstantValueExpression const5(ValueFactory::GetIntegerValue(9));
        ComparisonExpression predicate(&expB, &const5, ComparisonType::LessThan);
        Schema schema{cols};
        SeqScanPlanNode plan{&schema, &predicate, table_meta->oid_};
        ExecutorContext exec_ctx(txn, db_instance.GetCatalog(), db_instance.GetDB()->buffer_pool_manager_,
                                 db_instance.GetDB()->transaction_manager_, db_instance.GetDB()->lock_manager_);
        ExecutionEngine execution_engine(db_instance.GetDB()->buffer_pool_manager_,
                                         db_instance.GetDB()->transaction_manager_, db_instance.GetCatalog());
        std::vector<Tuple> result_set;
        auto ok = execution_engine.Execute(&plan, &result_set, txn, &exec_ctx);
        std::cout << "result: " << ok << " size: " << result_set.size() << std::endl;
        break;
      }
      case hsql::kStmtImport:
        std::cout << "import statement not support" << std::endl;
        break;
      case hsql::kStmtInsert: {
        auto *insert_statement = dynamic_cast<hsql::InsertStatement *>(stmt);
        std::cout << insert_statement->tableName << std::endl;
        auto table_meta = db_instance.GetCatalog()->GetTable(insert_statement->tableName);
        for (const auto &col : *insert_statement->columns) {
          std::cout << "col: " << col << std::endl;
        }

        std::vector<std::vector<Value>> raw_values;
        std::vector<Value> raw_value;
        for (const auto &val : *insert_statement->values) {
          std::cout << "val: " << val->isLiteral() << std::endl;
          std::cout << "int val: " << val->ival << " (2): " << val->ival2 << std::endl;
          Value value(TypeId::INTEGER, int32_t(val->ival));
          std::cout << "value: " << value.ToString() << std::endl;
          raw_value.push_back(value);
        }
        raw_values.push_back(raw_value);

        // 执行 sql
        InsertPlanNode plan{{std::move(raw_values)}, table_meta->oid_};
        ExecutorContext exec_ctx(txn, db_instance.GetCatalog(), db_instance.GetDB()->buffer_pool_manager_,
                                 db_instance.GetDB()->transaction_manager_, db_instance.GetDB()->lock_manager_);
        ExecutionEngine execution_engine(db_instance.GetDB()->buffer_pool_manager_,
                                         db_instance.GetDB()->transaction_manager_, db_instance.GetCatalog());
        auto ok = execution_engine.Execute(&plan, nullptr, txn, &exec_ctx);
        // execute result
        std::cout << "result: " << ok << std::endl;
        break;
      }
      case hsql::kStmtUpdate: {
        auto *update_stmt = dynamic_cast<hsql::DeleteStatement *>(stmt);
        std::cout << "table name: " << update_stmt->tableName << std::endl;
        auto table_meta = db_instance.GetCatalog()->GetTable(update_stmt->tableName);
        // TODO where
        ColumnValueExpression expB(1, 1, TypeId::SMALLINT);
        ConstantValueExpression const5(ValueFactory::GetIntegerValue(9));
        ComparisonExpression predicate(&expB, &const5, ComparisonType::LessThan);
        SeqScanPlanNode child_plan{&table_meta->schema_, &predicate, table_meta->oid_};
        std::unordered_map<uint32_t, UpdateInfo> update_attrs;
        update_attrs.emplace(0, UpdateInfo{UpdateType::Set, 1});
        UpdatePlanNode plan{&child_plan, table_meta->oid_, update_attrs};
        ExecutorContext exec_ctx(txn, db_instance.GetCatalog(), db_instance.GetDB()->buffer_pool_manager_,
                                 db_instance.GetDB()->transaction_manager_, db_instance.GetDB()->lock_manager_);
        ExecutionEngine execution_engine(db_instance.GetDB()->buffer_pool_manager_,
                                         db_instance.GetDB()->transaction_manager_, db_instance.GetCatalog());
        auto ok = execution_engine.Execute(&plan, nullptr, txn, &exec_ctx);
        // execute result
        std::cout << "result: " << ok << std::endl;
        break;
      }
      case hsql::kStmtDelete: {
        auto *delete_stmt = dynamic_cast<hsql::DeleteStatement *>(stmt);
        std::cout << "table name: " << delete_stmt->tableName << std::endl;
        auto table_meta = db_instance.GetCatalog()->GetTable(delete_stmt->tableName);

        // TODO where
        ColumnValueExpression expB(1, 1, TypeId::SMALLINT);
        ConstantValueExpression const5(ValueFactory::GetIntegerValue(9));
        ComparisonExpression predicate(&expB, &const5, ComparisonType::LessThan);
        SeqScanPlanNode child_plan{&table_meta->schema_, &predicate, table_meta->oid_};
        DeletePlanNode plan{&child_plan, table_meta->oid_};
        ExecutorContext exec_ctx(txn, db_instance.GetCatalog(), db_instance.GetDB()->buffer_pool_manager_,
                                 db_instance.GetDB()->transaction_manager_, db_instance.GetDB()->lock_manager_);
        ExecutionEngine execution_engine(db_instance.GetDB()->buffer_pool_manager_,
                                         db_instance.GetDB()->transaction_manager_, db_instance.GetCatalog());
        auto ok = execution_engine.Execute(&plan, nullptr, txn, &exec_ctx);
        // execute result
        std::cout << "result: " << ok << std::endl;
        break;
      }
      case hsql::kStmtCreate: {
        auto *create_stmt = dynamic_cast<hsql::CreateStatement *>(stmt);
        std::cout << "table name: " << create_stmt->tableName << std::endl;
        std::vector<Column> cols;
        for (const auto column : *create_stmt->columns) {
          std::cout << "column, name: " << column->name << ", type: " << column->type << std::endl;
          auto col = GetCol(column);
          if (col.GetType() == TypeId::INVALID) {
            std::cerr << "column: " << column->type << " not support" << std::endl;
            break;
          }
          cols.push_back(col);
          std::cout << "column def : " << col.ToString() << std::endl;
        }
        std::cout << "column size: " << create_stmt->columns->size() << std::endl;

        Schema schema{cols};
        // create table
        db_instance.GetCatalog()->CreateTable(txn, std::string(create_stmt->tableName), schema);
        break;
      }
      case hsql::kStmtDrop: {
        auto *drop_stmt = dynamic_cast<hsql::DropStatement *>(stmt);
        std::cout << "table name: " << drop_stmt->name << std::endl;
        std::cout << "drop statement not support" << std::endl;
        // drop table
        // db_instance.GetCatalog()->CreateTable(txn, std::string(create_stmt->tableName), schema);
        break;
      }
      case hsql::kStmtPrepare:
        std::cout << "prepare statement not support" << std::endl;
        break;
      case hsql::kStmtExecute:
        std::cout << "execute statement not support" << std::endl;
        break;
      case hsql::kStmtExport:
        std::cout << "export statement not support" << std::endl;
        break;
      case hsql::kStmtRename:
        std::cout << "rename statement not support" << std::endl;
        break;
      case hsql::kStmtAlter:
        std::cout << "alter statement not support" << std::endl;
        break;
      case hsql::kStmtShow:
        std::cout << "show statement not support" << std::endl;
        break;
      case hsql::kStmtTransaction:
        std::cout << "transaction statement not support" << std::endl;
        break;
    }
  }

  db_instance.CommitTransaction(txn);  // commit
  return 0;
}
