#include "execution/executors/sort_executor.h"
#include "binder/bound_order_by.h"
#include "common/rid.h"
#include "storage/table/tuple.h"
#include "type/type.h"
#include "type/value.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  // init成员变量
  child_executor_->Init();
  sorted_.clear();
  // 将所有的tuple都取出来
  Tuple temp_tuple{};
  RID temp_rid{};
  while (true) {
    bool fetch_res = child_executor_->Next(&temp_tuple, &temp_rid);
    if (!fetch_res) {
      break;
    }
    sorted_.emplace_back(temp_tuple);
  }
  // 定义排序的lambda函数，根据排序的规则，一个个比较，并且将较小的放在vector的右边
  auto sorter_lambda = [&](Tuple &ltuple, Tuple &rtuple) -> bool {
    for (const auto &[order_type, expr] : plan_->GetOrderBy()) {
      Value left_value = expr->Evaluate(&ltuple, GetOutputSchema());
      Value right_value = expr->Evaluate(&rtuple, GetOutputSchema());
      if (left_value.CompareEquals(right_value) == CmpBool::CmpTrue) {
        continue;
      }
      auto cmp_res = left_value.CompareLessThan(right_value);
      bool cond1 = (cmp_res == CmpBool::CmpTrue) && (order_type == OrderByType::DESC);
      bool cond2 = (cmp_res == CmpBool::CmpFalse && (order_type != OrderByType::DESC));
      return cond1 || cond2;
    }
    return false;
  };
  std::sort(sorted_.begin(), sorted_.end(), sorter_lambda);
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (sorted_.empty()) {
    return false;
  }
  *tuple = sorted_.back();
  sorted_.pop_back();
  return true;
}

}  // namespace bustub
