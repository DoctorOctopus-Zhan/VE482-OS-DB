//
// Created by Zhan on 21-11-03.
//

#include "AddQuery.h"

#include "../../db/Database.h"

constexpr const char *AddQuery::qname;

QueryResult::Ptr AddQuery::execute() {
  Database &db = Database::getInstance();
  auto &table = db[this->targetTable];
  if (table.size() > 2000) {
    return this->threadExecute(ROW_PER_SUBQUERY);
  } else {
    using namespace std;
    if (this->operands.size() < 2)
      return make_unique<ErrorMsgResult>(
          qname, this->targetTable.c_str(),
          "Invalid number of operands (? operands)."_f % operands.size());

    fieldId.reserve(this->operands.size());
    size_t fieldNum = this->operands.size();

    Table::SizeType counter = 0;
    int count = 0;
    try {
      //    table.readLock();
      // table.writeLock();
      for (auto it = this->operands.begin(); it != this->operands.end(); ++it) {
        fieldId.emplace_back(table.getFieldIndex(*it));
      }
      auto result = initCondition(table);
      if (result.second) {
        for (auto it = table.begin(); it != table.end(); ++it) {
          if (this->evalCondition(*it)) {
            Table::ValueType fieldResult = 0;
            ++counter;
            for (size_t i = 0; i < fieldNum - 1; ++i) {
              fieldResult += (*it)[fieldId[i]];
            }
            (*it)[fieldId[fieldNum - 1]] = fieldResult;
          }
          ++count;
        }
      }
      //    table.readUnlock();
      //          table.writeUnlock();
      return make_unique<RecordCountResult>(counter);
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

std::string AddQuery::toString() {
  return "QUERY = ADD " + this->targetTable + "\"";
}
Table::SizeType AddQuery::partialExecute(Table::Iterator begin,
                                         Table::Iterator end, Table &table) {
  // table.writeLock();
  Table::SizeType partialCounter = 0;
  auto result = initCondition(table);
  if (result.second) {
    for (auto it = begin; it != end; ++it) {
      if (this->evalCondition(*it)) {
        Table::ValueType fieldResult = 0;
        ++partialCounter;
        for (size_t i = 0; i < this->operands.size() - 1; ++i) {
          fieldResult += (*it)[fieldId[i]];
        }
        //        table.readUnlock();
        //        table.writeLock();
        (*it)[fieldId[this->operands.size() - 1]] = fieldResult;
        // the last operand is the field to be updated
        //        table.writeUnlock();
        //        table.readLock();
      }
    }
  }
  // table.writeUnlock();
  return partialCounter;
}
QueryResult::Ptr AddQuery::threadExecute(int subQuerySize) {
  using namespace std;

  Table::SizeType totalCounter = 0;
  Database &db = Database::getInstance();
  auto &table = db[this->targetTable];

  // table.readLock();

  fieldId.reserve(this->operands.size());
  for (auto &operand : this->operands) {
    fieldId.emplace_back(table.getFieldIndex(operand));
  }

  Table::Iterator subBegin = table.begin(), subEnd = table.end();

  auto &pool = ThreadPool::getInstance();
  subQuerySize = 1 + (int)(table.size() / pool.getTotalThreads());
  unsigned long subQueryNum = 1 + (table.size() / (unsigned long)subQuerySize);
  vector<std::future<Table::SizeType>> futures;
  futures.reserve(subQueryNum);
  int counter = 0;
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
    totalCounter += result;
  }

  return make_unique<RecordCountResult>(totalCounter);
}

// using namespace std;
// if (this->operands.size() < 2)
//  return make_unique<ErrorMsgResult>(
//      qname, this->targetTable.c_str(),
//      "Invalid number of operands (? operands)."_f % operands.size());

// fieldId.reserve(this->operands.size());
// size_t fieldNum = this->operands.size();
//
// Database &db = Database::getInstance();
// Table::SizeType counter = 0;
// int count = 0;
// try {
//  auto &table = db[this->targetTable];
//  //    table.readLock();
//  table.writeLock();
//  for (auto it = this->operands.begin(); it != this->operands.end(); ++it) {
//    fieldId.emplace_back(table.getFieldIndex(*it));
//  }
//  auto result = initCondition(table);
//  if (result.second) {
//    for (auto it = table.begin(); it != table.end(); ++it) {
//      if (this->evalCondition(*it)) {
//        Table::ValueType fieldResult = 0;
//        ++counter;
//        for (size_t i = 0; i < fieldNum - 1; ++i) {
//          fieldResult += (*it)[fieldId[i]];
//        }
//        (*it)[fieldId[fieldNum - 1]] = fieldResult;
//      }
//      ++count;
//    }
//  }
//  //    table.readUnlock();
//  table.writeUnlock();
//  return make_unique<RecordCountResult>(counter);
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