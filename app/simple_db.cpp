#include <chrono>
#include <random>

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
#include "execution/plans/seq_scan_plan.h"
#include "storage/table/table_heap.h"

using namespace bustub;

// 生成记录
Tuple ConstructTuple(Schema *schema) {
  std::vector<Value> values;
  Value v(TypeId::INVALID);

  auto seed = std::chrono::system_clock::now().time_since_epoch().count();

  std::mt19937 generator(seed);  // mt19937 is a standard mersenne_twister_engine

  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    // get type
    const auto &col = schema->GetColumn(i);
    TypeId type = col.GetType();
    switch (type) {
      case TypeId::BOOLEAN:
        v = Value(type, static_cast<int8_t>(generator() % 2));
        break;
      case TypeId::TINYINT:
        v = Value(type, static_cast<int8_t>(generator()) % 1000);
        break;
      case TypeId::SMALLINT:
        v = Value(type, static_cast<int16_t>(generator()) % 1000);
        break;
      case TypeId::INTEGER:
        v = Value(type, static_cast<int32_t>(generator()) % 1000);
        break;
      case TypeId::BIGINT:
        v = Value(type, static_cast<int64_t>(generator()) % 1000);
        break;
      case TypeId::VARCHAR: {
        static const char alphanum[] =
            "0123456789"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            "abcdefghijklmnopqrstuvwxyz";
        auto len = static_cast<uint32_t>(1 + generator() % 9);
        char s[10];
        for (uint32_t j = 0; j < len; ++j) {
          s[j] = alphanum[generator() % (sizeof(alphanum) - 1)];
        }
        s[len] = 0;
        v = Value(type, s, len + 1, true);
        break;
      }
      default:
        break;
    }
    values.emplace_back(v);
  }
  return Tuple(values, schema);
}

int main(int argc, char **argv) {
  enable_logging = true;  // 开启日志
  BustubInstance db("test.db");
  Catalog catalog(db.buffer_pool_manager_, db.lock_manager_, db.log_manager_);
  // sql => plan => execute => transaction => result
  // 1. 开启事务
  Transaction *txn = db.transaction_manager_->Begin();
  // 2. 新建表
  RID rid;
  RID rid1;
  Column col1{"colA", TypeId::INTEGER};
  Column col2{"colB", TypeId::SMALLINT};
  std::vector<Column> cols{col1, col2};
  Schema schema{cols};
  // catalog
  auto table_metadata = catalog.CreateTable(txn, "pedro", schema);
  TableHeap *test_table = table_metadata->table_.get();
  // 3. 插入记录
  const Tuple tuple = ConstructTuple(&schema);
  const Tuple tuple1 = ConstructTuple(&schema);
  // insert
  std::cout << "insert tuple: " << std::endl;
  std::cout << tuple.GetValue(&schema, schema.GetColIdx("colA")).GetAs<int32_t>() << ", "
            << tuple.GetValue(&schema, schema.GetColIdx("colB")).GetAs<int32_t>() << std::endl;
  std::cout << tuple1.GetValue(&schema, schema.GetColIdx("colA")).GetAs<int32_t>() << ", "
            << tuple1.GetValue(&schema, schema.GetColIdx("colB")).GetAs<int32_t>() << std::endl;
  test_table->InsertTuple(tuple, &rid, txn);
  test_table->InsertTuple(tuple1, &rid1, txn);
  // 4. 提交事务
  db.transaction_manager_->Commit(txn);

  // 5. 查询
  Tuple tuple3;
  Transaction *txn1 = db.transaction_manager_->Begin();
  ExecutorContext exec_ctx(txn1, &catalog, db.buffer_pool_manager_, db.transaction_manager_, db.lock_manager_);
  // 6. 构造 sql
  // auto table_metadata = exec_ctx.GetCatalog()->GetTable("pedro");
  ColumnValueExpression expA(0, 0, TypeId::INTEGER);
  ColumnValueExpression expB(1, 1, TypeId::SMALLINT);
  ConstantValueExpression const5(ValueFactory::GetIntegerValue(9));
  std::vector<Column> out_cols{{"colA", expA.GetReturnType(), &expA}, {"colB", expB.GetReturnType(), &expB}};
  Schema out_schema(out_cols);
  ComparisonExpression predicate(&expB, &const5, ComparisonType::LessThan);
  SeqScanPlanNode plan{&out_schema, &predicate, table_metadata->oid_};

  // 7. 执行 sql
  std::vector<Tuple> result_set;
  ExecutionEngine execution_engine(db.buffer_pool_manager_, db.transaction_manager_, &catalog);
  execution_engine.Execute(&plan, &result_set, txn1, &exec_ctx);

  // 8. print
  std::cout << "ColA, ColB" << std::endl;
  for (const auto &tuple : result_set) {
    std::cout << tuple.GetValue(&out_schema, out_schema.GetColIdx("colA")).GetAs<int32_t>() << ", "
              << tuple.GetValue(&out_schema, out_schema.GetColIdx("colB")).GetAs<int32_t>() << std::endl;
  }

  db.transaction_manager_->Commit(txn1);

  return 0;
}