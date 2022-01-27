//
// Created by Zhan on 21-10-27.
//

#include "CountQuery.h"
#include "../../db/Database.h"
#include "../../threadPool/ThreadPool.h"

constexpr const char *CountQuery::qname;

QueryResult::Ptr CountQuery::execute() {
  Database &db = Database::getInstance();
  auto &table = db[this->targetTable];
  if (table.size() > 2000) {
    return this->threadExecute(ROW_PER_SUBQUERY);
  } else {
    using namespace std;
    if (this->operands.size() > 0)
      return make_unique<ErrorMsgResult>(
          qname, this->targetTable.c_str(),
          "Invalid number of operands (? operands)."_f % operands.size());
    Table::SizeType counter = 0;
    try {
      // table.readLock();
      if (condition.empty()) {
        counter = table.size();
        return make_unique<SuccessMsgResult>(counter);
      }

      auto result = initCondition(table);
      if (result.second) {
        for (auto it = table.begin(); it != table.end(); ++it) {
          if (this->evalCondition(*it)) {
            ++counter;
          }
        }
      }
      // table.readUnlock();
      return make_unique<SuccessMsgResult>(counter);
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

Table::SizeType CountQuery::partialExecute(Table::Iterator begin,
                                           Table::Iterator end, Table &table) {
  using namespace std;
  Table::SizeType subCounter = 0;

  // table.readLock();

  auto result = initCondition(table);
  if (result.second) {
    for (auto it = begin; it != end; ++it) {
      if (this->evalCondition(*it)) {
        //        cout << "find a correct one " << subCounter << endl;
        ++subCounter;
      }
    }
  }

  // table.readUnlock();

  return subCounter;
}

std::string CountQuery::toString() {
  return "QUERY = COUNT " + this->targetTable + "\"";
}
QueryResult::Ptr CountQuery::threadExecute(int subQuerySize) {
  using namespace std;
  Database &db = Database::getInstance();
  Table::SizeType totalCount = 0;
  auto &table = db[this->targetTable];

  if (condition.empty()) {

    // table.readLock();

    totalCount = table.size();

    // table.readUnlock();

    return make_unique<SuccessMsgResult>(totalCount);
  } else {

    // table.readLock();

    int counter = 0;
    /* the subquery will process [subBegin, subEnd)
     * these two are also the parameters
     * passed into partialExecute. */
    Table::Iterator subBegin = table.begin(), subEnd = table.end();

    auto &pool = ThreadPool::getInstance();

    subQuerySize = 1 + (int)(table.size() / pool.getTotalThreads());
    unsigned long subQueryNum =
        1 + (table.size() / (unsigned long)subQuerySize);
    vector<std::future<Table::SizeType>> futures;
    futures.reserve(subQueryNum);
    for (auto it = table.begin();; ++it) {
      if ((counter != 0 && counter % subQuerySize == 0) || it == table.end()) {
#ifdef YUXIANG_DEBUG
        cout << "create a new sub query, on counter = " << counter << endl;
#endif
        subEnd = it;
        std::future<Table::SizeType> result =
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
    for (auto &future : futures) {
      auto result = future.get();
      totalCount += result;
    }
    return make_unique<SuccessMsgResult>(totalCount);
  }
}

// using namespace std;
// if (this->operands.size() > 0)
//  return make_unique<ErrorMsgResult>(
//      qname, this->targetTable.c_str(),
//      "Invalid number of operands (? operands)."_f % operands.size());
// Database &db = Database::getInstance();
// Table::SizeType counter = 0;
// try {
//  auto &table = db[this->targetTable];
//  table.readLock();
//  if (condition.empty())
//  {
//    counter = table.size();
//    return make_unique<SuccessMsgResult>(counter);
//  }
//
//  auto result = initCondition(table);
//  if (result.second) {
//    for (auto it = table.begin(); it != table.end(); ++it) {
//      if (this->evalCondition(*it)) {
//        ++counter;
//      }
//    }
//  }
//  table.readUnlock();
//  return make_unique<SuccessMsgResult>(counter);
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
