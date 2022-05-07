//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer_test.cpp
//
// Identification: test/buffer/lru_replacer_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>
#include <thread>  // NOLINT
#include <vector>

#include "buffer/lru_replacer.h"
#include "gtest/gtest.h"

namespace bustub {

TEST(LRUReplacerTest, SampleTest) {
  LRUReplacer lru_replacer(7);

  // Scenario: unpin six elements, i.e. add them to the replacer.
  lru_replacer.Unpin(1);
  lru_replacer.Unpin(2);
  lru_replacer.Unpin(3);
  lru_replacer.Unpin(4);
  lru_replacer.Unpin(5);
  lru_replacer.Unpin(6);
  lru_replacer.Unpin(1);  // 1 被 unpin 了两次
  EXPECT_EQ(6, lru_replacer.Size());

  // Scenario: get three victims from the lru.
  int value;
  lru_replacer.Victim(&value);
  EXPECT_EQ(1, value);  // 删除 1
  lru_replacer.Victim(&value);
  EXPECT_EQ(2, value);  // 删除 2
  lru_replacer.Victim(&value);
  EXPECT_EQ(3, value);  // 删除 3

  // Scenario: pin elements in the replacer.
  // Note that 3 has already been victimized, so pinning 3 should have no effect.
  lru_replacer.Pin(3);                // 3 已经被删了，所有无影响
  lru_replacer.Pin(4);                // 删除 4
  EXPECT_EQ(2, lru_replacer.Size());  // 只剩 5,6

  // Scenario: unpin 4. We expect that the reference bit of 4 will be set to 1.
  lru_replacer.Unpin(4);  // 再次加入 4，4 在链表最后

  // Scenario: continue looking for victims. We expect these victims.
  lru_replacer.Victim(&value);  // 删除 5
  EXPECT_EQ(5, value);
  lru_replacer.Victim(&value);  // 删除 6
  EXPECT_EQ(6, value);
  lru_replacer.Victim(&value);  // 删除 4
  EXPECT_EQ(4, value);
}

}  // namespace bustub
