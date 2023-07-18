#include "execution/executors/topn_executor.h"
#include <cstddef>
#include <queue>
#include "binder/bound_order_by.h"
#include "common/rid.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  // 初始化成员变量
  child_executor_->Init();
  sorted_.clear();
  // 定义排序的lambda函数
  auto orderby_keys = plan_->GetOrderBy();
  auto schema = plan_->OutputSchema();
  auto cmp_lambda = [&](const Tuple &left, const Tuple &right) -> bool {
    for (auto &[order_type, expr] : orderby_keys) {
      Value left_value = expr->Evaluate(&left, schema);
      Value right_value = expr->Evaluate(&right, schema);
      if (left_value.CompareEquals(right_value) == CmpBool::CmpTrue) {
        continue;
      }
      auto cmp_res = left_value.CompareLessThan(right_value);
      bool res1 = (cmp_res == CmpBool::CmpTrue) && (order_type != OrderByType::DESC);
      bool res2 = (cmp_res == CmpBool::CmpFalse) && (order_type == OrderByType::DESC);
      return res1 || res2;
    }
    return false;
  };
  // 创建一个优先队列
  std::priority_queue<Tuple, std::vector<Tuple>, decltype(cmp_lambda)> pq(cmp_lambda);
  // 不断地取出child算子的tuple，插入优先队列，保持这个队列的大小为n
  size_t n = plan_->GetN();
  Tuple temp_tuple{};
  RID temp_rid{};
  while (true) {
    bool fetch_res = child_executor_->Next(&temp_tuple, &temp_rid);
    if (!fetch_res) {
      break;
    }
    pq.push(temp_tuple);
    if (pq.size() > n) {
      pq.pop();
    }
  }
  sorted_.reserve(pq.size());
  while (!pq.empty()) {
    sorted_.push_back(pq.top());
    pq.pop();
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (sorted_.empty()) {
    return false;
  }
  *tuple = sorted_.back();
  sorted_.pop_back();
  return true;
}

}  // namespace bustub
