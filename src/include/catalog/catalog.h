#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "catalog/schema.h"
#include "storage/index/b_plus_tree_index.h"
#include "storage/index/index.h"
#include "storage/table/table_heap.h"

namespace bustub {

/**
 * Typedefs
 */
using table_oid_t = uint32_t;
using column_oid_t = uint32_t;
using index_oid_t = uint32_t;

/**
 * Metadata about a table. table 元数据
 */
struct TableMetadata {
  TableMetadata(Schema schema, std::string name, std::unique_ptr<TableHeap> &&table, table_oid_t oid)
      : schema_(std::move(schema)), name_(std::move(name)), table_(std::move(table)), oid_(oid) {}
  Schema schema_;
  std::string name_;
  std::unique_ptr<TableHeap> table_;
  table_oid_t oid_;
};

/**
 * Metadata about a index 索引元数据
 */
struct IndexInfo {
  IndexInfo(Schema key_schema, std::string name, std::unique_ptr<Index> &&index, index_oid_t index_oid,
            std::string table_name, size_t key_size)
      : key_schema_(std::move(key_schema)),
        name_(std::move(name)),
        index_(std::move(index)),
        index_oid_(index_oid),
        table_name_(std::move(table_name)),
        key_size_(key_size) {}
  Schema key_schema_;
  std::string name_;
  std::unique_ptr<Index> index_;
  index_oid_t index_oid_;
  std::string table_name_;
  const size_t key_size_;
};

/**
 * Catalog is a non-persistent catalog that is designed for the executor to use.
 * It handles table creation and table lookup.  Catalog 用于数据表创建和查找
 */
class Catalog {
 public:
  /**
   * Creates a new catalog object.
   * @param bpm the buffer pool manager backing tables created by this catalog
   * @param lock_manager the lock manager in use by the system
   * @param log_manager the log manager in use by the system
   */
  Catalog(BufferPoolManager *bpm, LockManager *lock_manager, LogManager *log_manager)
      : bpm_{bpm}, lock_manager_{lock_manager}, log_manager_{log_manager} {}

  /**
   * Create a new table and return its metadata.
   * @param txn the transaction in which the table is being created
   * @param table_name the name of the new table
   * @param schema the schema of the new table
   * @return a pointer to the metadata of the new table
   */
  TableMetadata *CreateTable(Transaction *txn, const std::string &table_name, const Schema &schema) {
    BUSTUB_ASSERT(names_.count(table_name) == 0, "Table names should be unique!");
    auto table_id = next_table_oid_.load();
    names_[table_name] = table_id;
    std::unique_ptr<TableHeap> table_heap = std::make_unique<TableHeap>(bpm_, lock_manager_, log_manager_, txn);
    std::unique_ptr<TableMetadata> metadata =
        std::make_unique<TableMetadata>(schema, table_name, std::move(table_heap), table_id);
    TableMetadata *ret = metadata.get();
    tables_[table_id] = std::move(metadata);
    next_table_oid_++;
    return ret;
  }

  /** @return table metadata by name */
  TableMetadata *GetTable(const std::string &table_name) {
    auto p = names_.find(table_name);
    if (p == names_.end()) {
      throw std::out_of_range("table is not exist.");
    }
    auto tableId = p->second;
    return GetTable(tableId);
  }

  /** @return table metadata by oid */
  TableMetadata *GetTable(table_oid_t table_oid) {
    auto p = tables_.find(table_oid);
    if (p == tables_.end()) {
      throw std::out_of_range("table is not exist.");
    }
    return p->second.get();
  }

  /**
   * Create a new index, populate existing data of the table and return its metadata.
   * @param txn the transaction in which the table is being created
   * @param index_name the name of the new index
   * @param table_name the name of the table
   * @param schema the schema of the table
   * @param key_schema the schema of the key
   * @param key_attrs key attributes
   * @param keysize size of the key
   * @return a pointer to the metadata of the new table
   */
  template <class KeyType, class ValueType, class KeyComparator>
  IndexInfo *CreateIndex(Transaction *txn, const std::string &index_name, const std::string &table_name,
                         const Schema &schema, const Schema &key_schema, const std::vector<uint32_t> &key_attrs,
                         size_t keysize) {
    // 索引id
    auto index_id = next_index_oid_.load();
    // 索引元数据
    std::unique_ptr<IndexMetadata> index_metadata =
        std::make_unique<IndexMetadata>(index_name, table_name, &schema, key_attrs);
    // 索引，这个地方必须 release，不能 get
    // 调用 release 会切断 unique_ptr 和它原来管理的对象的联系。
    std::unique_ptr<Index> index = std::make_unique<BPLUSTREE_INDEX_TYPE>(index_metadata.release(), bpm_);
    // 索引信息
    std::unique_ptr<IndexInfo> index_info =
        std::make_unique<IndexInfo>(key_schema, index_name, std::move(index), index_id, table_name, keysize);
    auto ret = index_info.get();
    // 记录索引
    indexes_.emplace(ret->index_oid_, std::move(index_info));
    index_names_[ret->table_name_].emplace(ret->name_, ret->index_oid_);
    next_index_oid_++;
    // table 元数据
    TableMetadata *metadata = GetTable(table_name);
    auto table_heap = metadata->table_.get();
    // 遍历 table 得到 tuples，建立索引数据
    for (auto it = table_heap->Begin(txn); it != table_heap->End(); it++) {
      ret->index_->InsertEntry(it->KeyFromTuple(schema, key_schema, key_attrs), it->GetRid(), txn);
    }
    return ret;
  }

  IndexInfo *GetIndex(const std::string &index_name, const std::string &table_name) {
    auto table_indexes = index_names_.find(table_name);
    if (table_indexes == index_names_.end()) {
      throw std::out_of_range("index is not exist.");
    }
    auto index = table_indexes->second.find(index_name);
    if (index == table_indexes->second.end()) {
      throw std::out_of_range("index is not exist.");
    }
    auto index_id = index->second;
    return GetIndex(index_id);
  }

  IndexInfo *GetIndex(index_oid_t index_oid) {
    auto p = indexes_.find(index_oid);
    if (p == indexes_.end()) {
      throw std::out_of_range("index is not exist.");
    }
    return p->second.get();
  }

  std::vector<IndexInfo *> GetTableIndexes(const std::string &table_name) {
    auto p = index_names_.find(table_name);
    if (p == index_names_.end()) {
      return {};
    }
    auto index_map = p->second;
    std::vector<IndexInfo *> indexes{};
    indexes.reserve(index_map.size());
    for (const auto &kv : index_map) {
      indexes.push_back(GetIndex(kv.second));
    }
    return indexes;
  }

 private:
  [[maybe_unused]] BufferPoolManager *bpm_;
  [[maybe_unused]] LockManager *lock_manager_;
  [[maybe_unused]] LogManager *log_manager_;

  /** tables_ : table identifiers -> table metadata. Note that tables_ owns all table metadata. */
  // table_id => table_metadata 表
  std::unordered_map<table_oid_t, std::unique_ptr<TableMetadata>> tables_;

  // table_name => table_id 表
  /** names_ : table names -> table identifiers */
  std::unordered_map<std::string, table_oid_t> names_;

  // 下一个 table_id
  /** The next table identifier to be used. */
  std::atomic<table_oid_t> next_table_oid_{0};

  // index_id => index_info 表
  /** indexes_: index identifiers -> index metadata. Note that indexes_ owns all index metadata */
  std::unordered_map<index_oid_t, std::unique_ptr<IndexInfo>> indexes_;

  // table_name => index_map，一个表可以有多个索引
  /** index_names_: table name -> index names -> index identifiers */
  std::unordered_map<std::string, std::unordered_map<std::string, index_oid_t>> index_names_;

  // 下一个索引 index_id
  /** The next index identifier to be used */
  std::atomic<index_oid_t> next_index_oid_{0};
};
}  // namespace bustub
