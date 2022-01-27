//
// Created by Zhan on 21-10-27.
//

#include "SumQuery.h"
#include "../../threadPool/ThreadPool.h"

#include "../../db/Database.h"

constexpr const char *SumQuery::qname;

QueryResult::Ptr SumQuery::execute() {
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

    fieldId.reserve(this->operands.size());
    // vector storing the field sum, initial 0
    std::vector<Table::ValueType> fieldValue(this->operands.size(), 0);

    try {

      for (auto it = this->operands.begin(); it != this->operands.end(); ++it) {
        if (*it == "KEY")
          return make_unique<ErrorMsgResult>(qname, this->targetTable.c_str(),
                                             "KEY field cannot be summed over");
        fieldId.emplace_back(table.getFieldIndex(*it));
      }

      auto result = initCondition(table);
      if (result.second) {
        for (auto it = table.begin(); it != table.end(); ++it) {
          if (this->evalCondition(*it)) {
            for (Table::FieldIndex i = 0; i < this->operands.size(); ++i) {
              fieldValue[i] += (*it)[fieldId[i]];
            }
          }
        }
      }
      return make_unique<SuccessMsgResult>(fieldValue);
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

std::vector<Table::ValueType> SumQuery::partialExecute(Table::Iterator begin,
                                                       Table::Iterator end,
                                                       Table &table) {
  using namespace std;
  // vector storing the field sum, initial 0
  std::vector<Table::ValueType> fieldValue(this->operands.size(), 0);

  // table.readLock();

  auto result = initCondition(table);
  if (result.second) {
    for (auto it = begin; it != end; ++it) {
      if (this->evalCondition(*it)) {
        for (Table::FieldIndex i = 0; i < this->operands.size(); ++i) {
          //            cout << "summing " << (*it)[fieldId[i]] << endl;
          fieldValue[i] += (*it)[fieldId[i]];
        }
      }
    }
  }

  // table.readUnlock();

  return fieldValue;
}

std::string SumQuery::toString() {
  return "QUERY = SUM " + this->targetTable + "\"";
}

QueryResult::Ptr SumQuery::threadExecute(int subQuerySize) {
  using namespace std;
  Database &db = Database::getInstance();
  auto &table = db[this->targetTable];
  int counter = 0;

  /* fieldId */
  fieldId.reserve(this->operands.size());
  for (auto &operand : this->operands) {
    if (operand == "KEY")
      std::cout << "You cannot sum up KEY!" << std::endl;
    fieldId.emplace_back(table.getFieldIndex(operand));
  }
  // table.readLock();
  /* the subquery will process [subBegin, subEnd)
   * these two are also the parameters
   * passed into partialExecute. */
  Table::Iterator subBegin = table.begin(), subEnd = table.end();

  auto &pool = ThreadPool::getInstance();

  subQuerySize = 1 + (int)(table.size() / pool.getTotalThreads());
  unsigned long subQueryNum = 1 + (table.size() / (unsigned long)subQuerySize);
  /* the return type from subqueries */
  typedef vector<Table::ValueType> fieldType;
  vector<std::future<fieldType>> futures;
  futures.reserve(subQueryNum);

  for (auto it = table.begin();; ++it) {

    if ((counter != 0 && counter % subQuerySize == 0) || it == table.end()) {
#ifdef YUXIANG_DEBUG
      cout << "create a new sub query, on counter = " << counter << endl;
#endif
      subEnd = it;
      std::future<fieldType> result =
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
  // table.readUnlock();

  /* sum up */
  fieldType totalResult(this->operands.size(), 0);
  for (auto &future : futures) {

    auto result = future.get();
    for (unsigned long i = 0; i < totalResult.size(); ++i) {
      totalResult[i] += result[i];
    }
  }

  return make_unique<SuccessMsgResult>(totalResult);
}

// QueryResult::Ptr SumQuery::execute() {
//  using namespace std;
//  if (this->operands.size() == 0)
//    return make_unique<ErrorMsgResult>(
//        qname, this->targetTable.c_str(),
//        "Invalid number of operands (? operands)."_f % operands.size());
//
//  fieldId.reserve(this->operands.size());
//  // vector storing the field sum, initial 0
//  std::vector<Table::ValueType> fieldValue(this->operands.size(), 0);
//
//  Database &db = Database::getInstance();
//  try {
//    auto &table = db[this->targetTable];
//
//    for (auto it = this->operands.begin(); it != this->operands.end(); ++it) {
//      if (*it == "KEY")
//        return make_unique<ErrorMsgResult>(qname, this->targetTable.c_str(),
//                                           "KEY field cannot be summed over");
//      fieldId.emplace_back(table.getFieldIndex(*it));
//    }
//
//    auto result = initCondition(table);
//    if (result.second) {
//      for (auto it = table.begin(); it != table.end(); ++it) {
//        if (this->evalCondition(*it)) {
//          for (Table::FieldIndex i = 0; i < this->operands.size(); ++i) {
//            fieldValue[i] += (*it)[fieldId[i]];
//          }
//        }
//      }
//    }
//    return make_unique<SuccessMsgResult>(fieldValue);
//  } catch (const TableNameNotFound &e) {
//    return make_unique<ErrorMsgResult>(qname, this->targetTable,
//                                       "No such table."s);
//  } catch (const IllFormedQueryCondition &e) {
//    return make_unique<ErrorMsgResult>(qname, this->targetTable, e.what());
//  } catch (const invalid_argument &e) {
//    // Cannot convert operand to string
//    return make_unique<ErrorMsgResult>(qname, this->targetTable,
//                                       "Unknown error '?'"_f % e.what());
//  } catch (const exception &e) {
//    return make_unique<ErrorMsgResult>(qname, this->targetTable,
//                                       "Unkonwn error '?'."_f % e.what());
//  }
//}
