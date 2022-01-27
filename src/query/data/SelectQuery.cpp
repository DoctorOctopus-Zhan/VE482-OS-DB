//
// Created by Zhan on 21-10-27.
//

#include "SelectQuery.h"
#include "../../db/Database.h"
#include "../../threadPool/ThreadPool.h"

#include <algorithm>
#include <string>

constexpr const char *SelectQuery::qname;

QueryResult::Ptr SelectQuery::execute() {
  Database &db = Database::getInstance();
  auto &table = db[this->targetTable];
  if (table.size() > 2000) {
    return this->threadExecute(ROW_PER_SUBQUERY);
  } else {
    using namespace std;
    if (this->operands.size() == 0)
      return make_unique<ErrorMsgResult>(
          qname, this->targetTable.c_str(),
          "Invalid number of operands (? operands)."_f % operands.size());
    if (this->operands[0] != "KEY")
      return make_unique<ErrorMsgResult>(qname, this->targetTable.c_str(),
                                         "The first operand should be KEY.");

    fieldId.reserve(this->operands.size() - 1);

    std::vector<std::pair<std::string, std::vector<int>>> ans;
    // ans.reserve(this->targetTable.size());

    try {
      // table.readLock();
      for (auto it = ++this->operands.begin(); it != this->operands.end();
           ++it) {
        fieldId.emplace_back(table.getFieldIndex(*it));
      }

      auto result = initCondition(table);
      if (result.second) {
        for (auto it = table.begin(); it != table.end(); ++it) {
          if (this->evalCondition(*it)) {
            std::vector<int> fieldValue(this->operands.size() - 1);
            for (Table::FieldIndex i = 0; i < this->operands.size() - 1; ++i) {
              fieldValue[i] = (*it)[fieldId[i]];
            }
            ans.emplace_back(std::make_pair(it->key(), fieldValue));
          }
        }
      }
      // table.readUnlock();
      // table.writeUnlock();
      if (ans.empty()) {
        return make_unique<NullQueryResult>();
      }
      std::sort(ans.begin(), ans.end());
      return make_unique<SuccessMsgResult>(ans);
    } catch (const TableNameNotFound &e) {
      return make_unique<ErrorMsgResult>(qname, this->targetTable,
                                         "No such table."s);
    } catch (const IllFormedQueryCondition &e) {
      return make_unique<ErrorMsgResult>(qname, this->targetTable, e.what());
    } catch (const invalid_argument &e) {
      // Cannot convert operand to string
      return make_unique<ErrorMsgResult>(qname, this->targetTable,
                                         "Unknown error '?'"_f % e.what());
    } catch (const exception &e) {
      return make_unique<ErrorMsgResult>(qname, this->targetTable,
                                         "Unkonwn error '?'."_f % e.what());
    }
  }
}

SelectQuery::ansType SelectQuery::partialExecute(Table::Iterator begin,
                                                 Table::Iterator end,
                                                 Table &table) {
  using namespace std;

  ansType partAns;
  table.readLock();

  auto result = initCondition(table);
  if (result.second) {
    for (auto it = begin; it != end; ++it) {
      if (this->evalCondition(*it)) {
        std::vector<int> fieldValue(this->operands.size() - 1);
        for (Table::FieldIndex i = 0; i < this->operands.size() - 1; ++i) {
          fieldValue[i] = (*it)[fieldId[i]];
        }
        partAns.emplace_back(std::make_pair(it->key(), fieldValue));
      }
    }
  }
  table.readUnlock();
  return partAns;
}

std::string SelectQuery::toString() {
  return "QUERY = SELECT " + this->targetTable + "\"";
}

QueryResult::Ptr SelectQuery::threadExecute(int subQuerySize) {
  using namespace std;

  Database &db = Database::getInstance();
  auto &table = db[this->targetTable];
  int counter = 0;
  /* field id */
  fieldId.reserve(this->operands.size() - 1);
  for (auto it = ++this->operands.begin(); it != this->operands.end(); ++it) {
    fieldId.emplace_back(table.getFieldIndex(*it));
  }

  // ans.reserve(this->targetTable.size());
  table.readLock();
  /* the subquery will process [subBegin, subEnd)
   * these two are also the parameters
   * passed into partialExecute. */
  Table::Iterator subBegin = table.begin(), subEnd = table.end();

  auto &pool = ThreadPool::getInstance();

  subQuerySize = 1 + (int)(table.size() / pool.getTotalThreads());
  unsigned long subQueryNum = 1 + (table.size() / (unsigned long)subQuerySize);
  vector<std::future<ansType>> futures;
  futures.reserve(subQueryNum);

  for (auto it = table.begin();; ++it) {

    if ((counter != 0 && counter % subQuerySize == 0) || it == table.end()) {
#ifdef YUXIANG_DEBUG
      cout << "create a new sub query, on counter = " << counter << endl;
#endif
      subEnd = it;
      std::future<ansType> result =
          pool.enqueTask([this, subBegin, subEnd, &table]() {
            return this->partialExecute(subBegin, subEnd, table);
          });
      futures.emplace_back(std::move(result)); // future is move-copyable
      subBegin = subEnd;
    }
    counter++;
    if (it == table.end())
      break;
  }

  table.readUnlock();

  /* concatenate together */
  ansType totalResult;
  for (auto &future : futures) {
    auto result = future.get();
    // Move elements from result to totalResult.
    // src is left in undefined but safe-to-destruct state.
    // see in
    // https://stackoverflow.com/questions/201718/concatenating-two-stdvectors
    totalResult.insert(totalResult.end(),
                       std::make_move_iterator(result.begin()),
                       std::make_move_iterator(result.end()));
  }
  if (totalResult.empty()) {
    return make_unique<NullQueryResult>();
  }
  std::sort(totalResult.begin(), totalResult.end());
  return make_unique<SuccessMsgResult>(totalResult);
}

// using namespace std;
// if (this->operands.size() == 0)
//  return make_unique<ErrorMsgResult>(
//      qname, this->targetTable.c_str(),
//      "Invalid number of operands (? operands)."_f % operands.size());
// if (this->operands[0] != "KEY")
//  return make_unique<ErrorMsgResult>(qname, this->targetTable.c_str(),
//                                     "The first operand should be KEY.");
//
// fieldId.reserve(this->operands.size() - 1);
//
// std::vector<std::pair<std::string, std::vector<int>>> ans;
//// ans.reserve(this->targetTable.size());
//
// Database &db = Database::getInstance();
// try {
//  auto &table = db[this->targetTable];
//  table.readLock();
//  for (auto it = ++this->operands.begin(); it != this->operands.end(); ++it) {
//    fieldId.emplace_back(table.getFieldIndex(*it));
//  }
//
//  auto result = initCondition(table);
//  if (result.second) {
//    for (auto it = table.begin(); it != table.end(); ++it) {
//      if (this->evalCondition(*it)) {
//        std::vector<int> fieldValue(this->operands.size() - 1);
//        for (Table::FieldIndex i = 0; i < this->operands.size() - 1; ++i) {
//          fieldValue[i] = (*it)[fieldId[i]];
//        }
//        ans.emplace_back(std::make_pair(it->key(), fieldValue));
//      }
//    }
//  }
//  table.readUnlock();
//  // table.writeUnlock();
//  if (ans.empty()) {
//    return make_unique<NullQueryResult>();
//  }
//  std::sort(ans.begin(), ans.end());
//  return make_unique<SuccessMsgResult>(ans);
//} catch (const TableNameNotFound &e) {
//  return make_unique<ErrorMsgResult>(qname, this->targetTable,
//                                     "No such table."s);
//} catch (const IllFormedQueryCondition &e) {
//  return make_unique<ErrorMsgResult>(qname, this->targetTable, e.what());
//} catch (const invalid_argument &e) {
//  // Cannot convert operand to string
//  return make_unique<ErrorMsgResult>(qname, this->targetTable,
//                                     "Unknown error '?'"_f % e.what());
//} catch (const exception &e) {
//  return make_unique<ErrorMsgResult>(qname, this->targetTable,
//                                     "Unkonwn error '?'."_f % e.what());
//}
