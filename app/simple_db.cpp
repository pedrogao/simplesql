#include <chrono>
#include <random>

#include "common/bustub_instance.h"
#include "common/config.h"
#include "common/logger.h"
#include "storage/table/table_heap.h"

using namespace bustub;

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
  BustubInstance db("test.db");

  Transaction *txn = db.transaction_manager_->Begin();
  TableHeap test_table(db.buffer_pool_manager_, db.lock_manager_, db.log_manager_, txn);
  page_id_t first_page_id = test_table.GetFirstPageId();
  LOG_INFO("first page id: %d", first_page_id);

  RID rid;
  RID rid1;
  Column col1{"a", TypeId::VARCHAR, 20};
  Column col2{"b", TypeId::SMALLINT};
  std::vector<Column> cols{col1, col2};
  Schema schema{cols};
  const Tuple tuple = ConstructTuple(&schema);
  const Tuple tuple1 = ConstructTuple(&schema);

  auto val_1 = tuple.GetValue(&schema, 1);
  auto val_0 = tuple.GetValue(&schema, 0);
  auto val1_1 = tuple1.GetValue(&schema, 1);
  auto val1_0 = tuple1.GetValue(&schema, 0);

  test_table.InsertTuple(tuple, &rid, txn);
  test_table.InsertTuple(tuple1, &rid1, txn);

  db.transaction_manager_->Commit(txn);
  return 0;
}