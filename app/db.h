#pragma once

#include <string>

#include "SQLParser.h"
#include "catalog/catalog.h"
#include "common/bustub_instance.h"
#include "common/config.h"
#include "common/exception.h"
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
#include "spdlog/spdlog.h"
#include "tuple_util.h"
#include "util/sqlhelper.h"

using namespace bustub;

Column GetCol(const hsql::ColumnDefinition *def);
ComparisonType GetCompareType(hsql::OperatorType &opType);
std::tuple<std::vector<Column>, std::vector<ColumnValueExpression>> GetSelectCols(std::vector<hsql::Expr *> select_list,
                                                                                  const Schema &schema);

class SimpleSQL {
 public:
  SimpleSQL(const std::string &&db_path) {
    db_ = new BustubInstance(db_path);
    catalog_ = new Catalog(db_->buffer_pool_manager_, db_->lock_manager_, db_->log_manager_);
  }

  ~SimpleSQL() {
    delete catalog_;
    delete db_;
  }

  hsql::SQLParserResult ParseSQL(std::string &query);
  void Execute(hsql::SQLParserResult &result);
  void executeSelectStmt(Transaction *txn, hsql::SQLStatement *stmt);
  void executeDeleteStmt(Transaction *txn, hsql::SQLStatement *stmt);
  void executeInsertStmt(Transaction *txn, hsql::SQLStatement *stmt);
  void executeUpdateStmt(Transaction *txn, hsql::SQLStatement *stmt);
  void executeCreateStmt(Transaction *txn, hsql::SQLStatement *stmt);

  Transaction *BeginTransaction() { return db_->transaction_manager_->Begin(); }

  void CommitTransaction(Transaction *txn) { db_->transaction_manager_->Commit(txn); }

  BustubInstance *GetDB() { return db_; }

  Catalog *GetCatalog() { return catalog_; }

 private:
  BustubInstance *db_;
  Catalog *catalog_;
};