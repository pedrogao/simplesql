#pragma once

#include <chrono>
#include <random>

#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "storage/table/tuple.h"
#include "type/type.h"
#include "type/type_id.h"

using namespace bustub;

/**
 * @brief 生成记录
 *
 * @param schema
 * @return Tuple
 */
Tuple ConstructTuple(Schema *schema);