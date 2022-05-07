//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// catalog_test.cpp
//
// Identification: test/catalog/catalog_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>
#include <unordered_set>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "catalog/catalog.h"
#include "gtest/gtest.h"
#include "type/value_factory.h"

namespace bustub {

// NOLINTNEXTLINE
TEST(CatalogTest, CreateTableTest) {
  auto disk_manager = new DiskManager("catalog_test.db");
  auto bpm = new BufferPoolManager(32, disk_manager);
  auto catalog = new Catalog(bpm, nullptr, nullptr);
  std::string table_name = "potato";

  // The table shouldn't exist in the catalog yet.
  EXPECT_THROW(catalog->GetTable(table_name), std::out_of_range);

  // Put the table into the catalog.
  std::vector<Column> columns;
  columns.emplace_back("A", TypeId::INTEGER);
  columns.emplace_back("B", TypeId::BOOLEAN);

  Schema schema(columns);
  auto *table_metadata = catalog->CreateTable(nullptr, table_name, schema);
  (void)table_metadata;

  // Notice that this test case doesn't check anything! :(
  // It is up to you to extend it

  EXPECT_EQ(table_metadata->name_, table_name);
  EXPECT_EQ(schema.GetColumnCount(), columns.size());

  auto table_meta1 = catalog->GetTable(table_name);
  EXPECT_EQ(table_meta1->name_, table_name);
  EXPECT_EQ(table_meta1->schema_.GetColumnCount(), columns.size());

  auto table_meta2 = catalog->GetTable(0);
  EXPECT_EQ(table_meta2->name_, table_name);
  EXPECT_EQ(table_meta2->schema_.GetColumnCount(), columns.size());

  delete catalog;
  delete bpm;
  delete disk_manager;
}

TEST(CatalogTest, CreateIndexTest) {
  auto disk_manager = new DiskManager("catalog_test.db");
  auto bpm = new BufferPoolManager(32, disk_manager);
  auto catalog = new Catalog(bpm, nullptr, nullptr);
  std::string table_name = "potato";

  // The table shouldn't exist in the catalog yet.
  EXPECT_THROW(catalog->GetTable(table_name), std::out_of_range);

  // Put the table into the catalog.
  std::vector<Column> columns;
  columns.emplace_back("A", TypeId::INTEGER);
  columns.emplace_back("B", TypeId::BOOLEAN);

  Schema schema(columns);
  auto *table_metadata = catalog->CreateTable(nullptr, table_name, schema);
  (void)table_metadata;

  // Notice that this test case doesn't check anything! :(
  // It is up to you to extend it

  EXPECT_EQ(table_metadata->name_, table_name);
  EXPECT_EQ(schema.GetColumnCount(), columns.size());

  auto table_meta1 = catalog->GetTable(table_name);
  EXPECT_EQ(table_meta1->name_, table_name);
  EXPECT_EQ(table_meta1->schema_.GetColumnCount(), columns.size());

  auto table_meta2 = catalog->GetTable(0);
  EXPECT_EQ(table_meta2->name_, table_name);
  EXPECT_EQ(table_meta2->schema_.GetColumnCount(), columns.size());

  std::string index_name = "index_foo";
  // The index shouldn't exist in the table yet.
  EXPECT_THROW(catalog->GetIndex(index_name, table_name), std::out_of_range);

  Transaction *txn = new Transaction(0);
  std::vector<Column> kcolumns;
  kcolumns.emplace_back("A", TypeId::INTEGER);
  Schema key_schema(kcolumns);
  std::vector<uint32_t> key_attrs{0};
  size_t keysize{8};
  auto *index_info = catalog->CreateIndex<GenericKey<8>, RID, GenericComparator<8>>(txn, index_name, table_name, schema,
                                                                                    key_schema, key_attrs, keysize);
  (void)index_info;

  EXPECT_EQ(index_info->name_, index_name);

  auto index_info1 = catalog->GetIndex(index_name, table_name);
  EXPECT_EQ(index_info1->name_, index_name);

  auto index_info2 = catalog->GetIndex(0);
  EXPECT_EQ(index_info2->name_, index_name);

  auto table_indexes = catalog->GetTableIndexes(table_name);
  EXPECT_EQ(table_indexes.size(), 1);

  std::vector<Column> kcolumns1;
  kcolumns.emplace_back("A", TypeId::INTEGER);
  Schema key_schema1(kcolumns1);
  auto *index_info3 = catalog->CreateIndex<GenericKey<8>, RID, GenericComparator<8>>(
      txn, std::string("index_bar"), table_name, schema, key_schema1, key_attrs, keysize);
  (void)index_info3;

  auto table_indexes1 = catalog->GetTableIndexes(table_name);
  EXPECT_EQ(table_indexes1.size(), 2);

  delete txn;
  delete catalog;
  delete bpm;
  delete disk_manager;
}

}  // namespace bustub
