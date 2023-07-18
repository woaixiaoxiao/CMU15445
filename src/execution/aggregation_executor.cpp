//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <algorithm>
#include <iterator>
#include <memory>
#include <utility>
#include <vector>

#include "common/rid.h"
#include "execution/executors/aggregation_executor.h"

namespace bustub {

// 聚集操作本质上就是分类统计，
// 这里的key代表用来分类的多种类别
// 这里的value就是需要统计的数据
// 返回的tuple其实就是被依次打印了

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      cursor_(aht_.Begin()),
      end_(aht_.End()) {}

void AggregationExecutor::Init() {
  // 让child算子也init一下
  child_->Init();
  allow_empty_output_ = true;
  // 重新构建一个哈希表
  aht_.Clear();
  Tuple child_tuple;
  RID child_rid;
  while (true) {
    bool fetch_success = child_->Next(&child_tuple, &child_rid);
    if (!fetch_success) {
      break;
    }
    // 分组/聚集的依据
    auto key = MakeAggregateKey(&child_tuple);
    // 聚集的值
    auto value = MakeAggregateValue(&child_tuple);
    aht_.InsertCombine(key, value);
  }
  cursor_ = aht_.Begin();
  end_ = aht_.End();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // 如果还没有到末尾
  if (cursor_ != end_) {
    // 取出当前的key和value
    AggregateKey key = cursor_.Key();
    AggregateValue value = cursor_.Val();
    // 拷贝到key的group_bys_，待会就一起返回了
    std::copy(value.aggregates_.begin(), value.aggregates_.end(), std::back_inserter(key.group_bys_));
    *tuple = Tuple(key.group_bys_, &plan_->OutputSchema());
    // 更新迭代器
    ++cursor_;
    return true;
  }
  // 如果到了末尾
  // 当前迭代器是第一个又是末尾，说明这个哈希表就是空的，并且允许空白输出
  if (cursor_ == aht_.Begin() && allow_empty_output_) {
    // 如果计划里的聚集本来就是空的，即本来就没有要按照什么聚合，那么可以返回个值，反正也没啥关系
    if (plan_->GetGroupBys().empty()) {
      auto value = aht_.GenerateInitialAggregateValue();
      *tuple = Tuple(value.aggregates_, &plan_->OutputSchema());
      allow_empty_output_ = false;
      return true;
    }
    // 计划里是非空的
    return false;
  }
  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
