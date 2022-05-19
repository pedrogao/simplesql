#include <iostream>
#include <string>

#include "SQLParser.h"
#include "util/sqlhelper.h"

// ./bin/simple_sql "CREATE TABLE students (name TEXT, age INTEGER, grade INTEGER);"

int main(int argc, char *argv[]) {
  if (argc <= 1) {
    std::cerr << "Usage: ./example \"SELECT * FROM test;" << std::endl;
    return -1;
  }

  std::string query = argv[1];
  // parse a given query
  hsql::SQLParserResult result;
  hsql::SQLParser::parse(query, &result);

  // check whether the parsing was successful
  if (!result.isValid()) {
    std::cerr << "Given string is not a valid SQL query" << std::endl;
    std::cerr << result.errorMsg() << ", at (" << result.errorLine() << ":" << result.errorColumn() << ")\n"
              << std::endl;
    return -1;
  }

  std::cout << "Parsed successfully!" << std::endl;
  std::cout << "Number of statements: " << result.size() << std::endl;

  for (auto i = 0U; i < result.size(); ++i) {
    // Print a statement summary.
    auto *stmt = result.getMutableStatement(i);
    hsql::printStatementInfo(stmt);

    switch (stmt->type()) {
      case hsql::kStmtError:
        break;
      case hsql::kStmtSelect: {
        auto *select_statement = dynamic_cast<hsql::SelectStatement *>(stmt);
        std::cout << select_statement->fromTable->getName() << std::endl;
        break;
      }
      case hsql::kStmtImport:
        break;
      case hsql::kStmtInsert:
        break;
      case hsql::kStmtUpdate:
        break;
      case hsql::kStmtDelete:
        break;
      case hsql::kStmtCreate: {
        auto *create_stmt = dynamic_cast<hsql::CreateStatement *>(stmt);
        std::cout << "table name: " << create_stmt->tableName << std::endl;
        for (const auto column : *create_stmt->columns) {
          std::cout << "column, name: " << column->name << ", type: " << column->type << std::endl;
        }
        std::cout << "column size: " << create_stmt->columns->size() << std::endl;
        break;
      }
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
  return 0;
}
