#include "db.h"

hsql::SQLParserResult SimpleSQL::ParseSQL(std::string &query) {
  hsql::SQLParserResult result;
  hsql::SQLParser::parse(query, &result);
  return result;
}

void SimpleSQL::Execute(hsql::SQLParserResult &result) {
  auto txn = this->BeginTransaction();  // start
  for (const auto &stmt : result.getStatements()) {
    switch (stmt->type()) {
      case hsql::kStmtSelect: {
        this->executeSelectStmt(txn, stmt);
        break;
      }
      case hsql::kStmtInsert: {
        this->executeInsertStmt(txn, stmt);
        break;
      }
      case hsql::kStmtUpdate: {
        this->executeUpdateStmt(txn, stmt);
        break;
      }
      case hsql::kStmtDelete: {
        this->executeDeleteStmt(txn, stmt);
        break;
      }
      case hsql::kStmtCreate: {
        this->executeCreateStmt(txn, stmt);
        break;
      }
      case hsql::kStmtDrop: {
        // auto *drop_stmt = dynamic_cast<hsql::DropStatement *>(stmt);
        throw NotImplementedException("drop statement not implemented");
      }
      case hsql::kStmtPrepare:
        throw NotImplementedException("prepare statement not implemented");
      case hsql::kStmtExecute:
        throw NotImplementedException("execute statement not implemented");
      case hsql::kStmtExport:
        throw NotImplementedException("export statement not implemented");
      case hsql::kStmtRename:
        throw NotImplementedException("rename statement not implemented");
      case hsql::kStmtAlter:
        throw NotImplementedException("alter statement not implemented");
      case hsql::kStmtShow:
        throw NotImplementedException("show statement not implemented");
      case hsql::kStmtTransaction:
        throw NotImplementedException("transaction statement not implemented");
      case hsql::kStmtImport:
        throw NotImplementedException("import statement not implemented");
      case hsql::kStmtError:
        throw Exception(ExceptionType::SQL_STATEMENT_PARSE, "SQL statement parse error");
    }
  }
  this->CommitTransaction(txn);  // commit
}

void SimpleSQL::executeSelectStmt(Transaction *txn, hsql::SQLStatement *stmt) {
  auto *statement = dynamic_cast<hsql::SelectStatement *>(stmt);
  spdlog::debug("select from table: {0}", statement->fromTable->getName());

  auto table_meta = this->GetCatalog()->GetTable(statement->fromTable->getName());
  auto [cols, exp_list] = GetSelectCols(*statement->selectList, table_meta->schema_);
  spdlog::debug("select from table: {0}, column size: {1}", statement->fromTable->getName(), cols.size());

  Schema schema{cols};
  auto condition_name = std::string{statement->whereClause->expr->getName()};
  ConstantValueExpression condition1(ValueFactory::GetIntegerValue(statement->whereClause->expr->ival));
  ComparisonType comp_type = GetCompareType(statement->whereClause->opType);
  auto exp_b = exp_list[0];
  ComparisonExpression predicate(&exp_b, &condition1, comp_type);
  // FIXME predicate 为啥不能为 nullptr？
  SeqScanPlanNode plan{&schema, &predicate, table_meta->oid_};
  ExecutorContext exec_ctx(txn, this->GetCatalog(), this->GetDB()->buffer_pool_manager_,
                           this->GetDB()->transaction_manager_, this->GetDB()->lock_manager_);
  ExecutionEngine execution_engine(this->GetDB()->buffer_pool_manager_, this->GetDB()->transaction_manager_,
                                   this->GetCatalog());

  std::vector<Tuple> result_set;
  auto ok = execution_engine.Execute(&plan, &result_set, txn, &exec_ctx);
  spdlog::debug("select from table: {0}, result: {1}, result set size: {2}", statement->fromTable->getName(), ok,
                result_set.size());
}

void SimpleSQL::executeDeleteStmt(Transaction *txn, hsql::SQLStatement *stmt) {
  auto *statement = dynamic_cast<hsql::DeleteStatement *>(stmt);
  spdlog::debug("delet from table : {0}", statement->tableName);
  auto table_meta = this->GetCatalog()->GetTable(statement->tableName);

  auto condition_name = std::string{statement->expr->getName()};
  ColumnValueExpression expB(table_meta->schema_.GetColIdx(condition_name),
                             table_meta->schema_.GetColIdx(condition_name),
                             table_meta->schema_.GetColumn(table_meta->schema_.GetColIdx(condition_name)).GetType());
  ConstantValueExpression condition1(ValueFactory::GetIntegerValue(statement->expr->expr->ival));
  ComparisonType comp_type = GetCompareType(statement->expr->opType);
  ComparisonExpression predicate(&expB, &condition1, comp_type);

  SeqScanPlanNode child_plan{&table_meta->schema_, &predicate, table_meta->oid_};
  DeletePlanNode plan{&child_plan, table_meta->oid_};
  ExecutorContext exec_ctx(txn, this->GetCatalog(), this->GetDB()->buffer_pool_manager_,
                           this->GetDB()->transaction_manager_, this->GetDB()->lock_manager_);
  ExecutionEngine execution_engine(this->GetDB()->buffer_pool_manager_, this->GetDB()->transaction_manager_,
                                   this->GetCatalog());
  auto ok = execution_engine.Execute(&plan, nullptr, txn, &exec_ctx);
  spdlog::debug("select from table: {0}, result: {1}", statement->tableName, ok);
}

void SimpleSQL::executeInsertStmt(Transaction *txn, hsql::SQLStatement *stmt) {
  auto *statement = dynamic_cast<hsql::InsertStatement *>(stmt);
  spdlog::debug("insert table: {0}", statement->tableName);
  auto table_meta = this->GetCatalog()->GetTable(statement->tableName);
  // TODO 暂时不支持具名插入
  // auto schema = table_meta->schema_;
  // std::vector<Column> insert_cols{};
  // if (statement->columns == nullptr) {
  //   insert_cols.insert(insert_cols.end(), schema.GetColumns().begin(), schema.GetColumns().end());
  // } else {
  //   for (const auto &col : *statement->columns) {
  //     insert_cols.push_back(schema.GetColumn(schema.GetColIdx(std::string(col))));
  //   }
  // }

  std::vector<std::vector<Value>> raw_values;
  std::vector<Value> raw_value;
  for (const auto &val : *statement->values) {
    if (!val->isLiteral()) {
      spdlog::debug("{0} not literal, not support", val->name);
      continue;
    }
    if (val->isType(hsql::ExprType::kExprLiteralInt)) {
      Value value(TypeId::INTEGER, int32_t(val->ival));
      raw_value.push_back(value);
    }
    if (val->isType(hsql::ExprType::kExprLiteralString)) {
      Value value(TypeId::VARCHAR, std::string(val->name));
      raw_value.push_back(value);
    }
    if (val->isType(hsql::ExprType::kExprLiteralFloat)) {
      Value value(TypeId::DECIMAL, val->fval);
      raw_value.push_back(value);
    }
  }
  raw_values.push_back(raw_value);

  InsertPlanNode plan{{std::move(raw_values)}, table_meta->oid_};
  ExecutorContext exec_ctx(txn, this->GetCatalog(), this->GetDB()->buffer_pool_manager_,
                           this->GetDB()->transaction_manager_, this->GetDB()->lock_manager_);
  ExecutionEngine execution_engine(this->GetDB()->buffer_pool_manager_, this->GetDB()->transaction_manager_,
                                   this->GetCatalog());

  bool ok = execution_engine.Execute(&plan, nullptr, txn, &exec_ctx);
  spdlog::debug("insert table: {0}, result: {1}", statement->tableName, ok);
}

void SimpleSQL::executeUpdateStmt(Transaction *txn, hsql::SQLStatement *stmt) {
  auto *statement = dynamic_cast<hsql::DeleteStatement *>(stmt);
  spdlog::debug("update table: {0}", statement->tableName);
  auto table_meta = this->GetCatalog()->GetTable(statement->tableName);

  auto condition_name = std::string{statement->expr->getName()};
  ColumnValueExpression expB(table_meta->schema_.GetColIdx(condition_name),
                             table_meta->schema_.GetColIdx(condition_name),
                             table_meta->schema_.GetColumn(table_meta->schema_.GetColIdx(condition_name)).GetType());
  ConstantValueExpression condition1(ValueFactory::GetIntegerValue(statement->expr->expr->ival));
  ComparisonType comp_type = GetCompareType(statement->expr->opType);
  ComparisonExpression predicate(&expB, &condition1, comp_type);

  SeqScanPlanNode child_plan{&table_meta->schema_, &predicate, table_meta->oid_};
  std::unordered_map<uint32_t, UpdateInfo> update_attrs;
  update_attrs.emplace(0, UpdateInfo{UpdateType::Set, 1});
  UpdatePlanNode plan{&child_plan, table_meta->oid_, update_attrs};
  ExecutorContext exec_ctx(txn, this->GetCatalog(), this->GetDB()->buffer_pool_manager_,
                           this->GetDB()->transaction_manager_, this->GetDB()->lock_manager_);
  ExecutionEngine execution_engine(this->GetDB()->buffer_pool_manager_, this->GetDB()->transaction_manager_,
                                   this->GetCatalog());

  auto ok = execution_engine.Execute(&plan, nullptr, txn, &exec_ctx);
  spdlog::debug("update table: {0}, result: {1}", statement->tableName, ok);
}

void SimpleSQL::executeCreateStmt(Transaction *txn, hsql::SQLStatement *stmt) {
  auto *create_stmt = dynamic_cast<hsql::CreateStatement *>(stmt);
  spdlog::debug("create table: {0}", create_stmt->tableName);

  std::vector<Column> cols;
  for (const auto column : *create_stmt->columns) {
    auto col = GetCol(column);
    if (col.GetType() == TypeId::INVALID) {
      spdlog::warn("column: {0} not support", col.GetName());
      continue;
    }
    cols.push_back(col);
  }

  Schema schema{cols};
  this->GetCatalog()->CreateTable(txn, std::string(create_stmt->tableName), schema);
}

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

ComparisonType GetCompareType(hsql::OperatorType &opType) {
  ComparisonType comp_type = ComparisonType::Equal;
  if (opType == hsql::OperatorType::kOpEquals) {
    comp_type = ComparisonType::Equal;
  } else if (opType == hsql::OperatorType::kOpLess) {
    comp_type = ComparisonType::LessThan;
  } else if (opType == hsql::OperatorType::kOpLessEq) {
    comp_type = ComparisonType::LessThanOrEqual;
  } else if (opType == hsql::OperatorType::kOpGreater) {
    comp_type = ComparisonType::GreaterThan;
  } else if (opType == hsql::OperatorType::kOpGreaterEq) {
    comp_type = ComparisonType::GreaterThanOrEqual;
  } else {
    comp_type = ComparisonType::NotEqual;
  }

  return comp_type;
}

std::tuple<std::vector<Column>, std::vector<ColumnValueExpression>> GetSelectCols(std::vector<hsql::Expr *> select_list,
                                                                                  const Schema &schema) {
  std::vector<Column> cols;
  std::vector<ColumnValueExpression> exp_list;
  for (size_t i = 0; i < select_list.size(); i++) {
    auto expr = select_list.at(i);
    if (expr->isType(hsql::ExprType::kExprStar)) {
      cols.insert(cols.end(), schema.GetColumns().begin(), schema.GetColumns().end());
      // FIXME
    }
    if (!expr->isLiteral()) {
      spdlog::debug("{0} not literal, not support", expr->name);
      continue;
    }
    if (expr->isType(hsql::ExprType::kExprLiteralInt)) {
      ColumnValueExpression exp(0, i, TypeId::INTEGER);
      Column col{std::string(expr->getName()), TypeId::INTEGER, &exp};
      cols.push_back(col);
      exp_list.push_back(std::move(exp));
    }
    if (expr->isType(hsql::ExprType::kExprLiteralString)) {
      ColumnValueExpression exp(0, i, TypeId::VARCHAR);
      Column col{std::string(expr->getName()), TypeId::VARCHAR, &exp};
      cols.push_back(col);
      exp_list.push_back(std::move(exp));
    }
    if (expr->isType(hsql::ExprType::kExprLiteralFloat)) {
      ColumnValueExpression exp(0, i, TypeId::DECIMAL);
      Column col{std::string(expr->getName()), TypeId::DECIMAL, &exp};
      cols.push_back(col);
      exp_list.push_back(std::move(exp));
    }
  }
  return std::make_tuple(cols, exp_list);
}
