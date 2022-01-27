//
// Created by liu on 18-10-25.
//

#include "UpdateQuery.h"
#include "../../db/Database.h"
#include "../../threadPool/ThreadPool.h"

constexpr const char *UpdateQuery::qname;

QueryResult::Ptr UpdateQuery::execute() {
  // return this->threadExecute(ROW_PER_SUBQUERY);
    using namespace std;
  if (this->operands.size() != 2)
    return make_unique<ErrorMsgResult>(
        qname, this->targetTable.c_str(),
        "Invalid number of operands (? operands)."_f % operands.size());
  Database &db = Database::getInstance();
  Table::SizeType counter = 0;
  try {
    auto &table = db[this->targetTable];
    if (table.size() > 2000) 
      return this->threadExecute(ROW_PER_SUBQUERY);
    if (this->operands[0] == "KEY") {
      this->keyValue = this->operands[1];
    } else {
      this->fieldId = table.getFieldIndex(this->operands[0]);
      this->fieldValue =
          (Table::ValueType)strtol(this->operands[1].c_str(), nullptr, 10);
    }
    auto result = initCondition(table);
    if (result.second) {
      for (auto it = table.begin(); it != table.end(); ++it) {
        if (this->evalCondition(*it)) {
          if (this->keyValue.empty()) {
            (*it)[this->fieldId] = this->fieldValue;
          } else {
            it->setKey(this->keyValue);
          }
          ++counter;
        }
      }
    }
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

std::string UpdateQuery::toString() {
  return "QUERY = UPDATE " + this->targetTable + "\"";
}
Table::SizeType UpdateQuery::partialExecute(Table::Iterator begin,
                                            Table::Iterator end, Table &table) {
  Table::SizeType partialCounter = 0;
  // table.writeLock();
  auto result = initCondition(table);
  if (result.second) {
    for (auto it = begin; it != end; ++it) {
      if (this->evalCondition(*it)) {
        if (this->keyValue.empty()) {
          //          table.readUnlock();
          //          table.writeLock();
          (*it)[this->fieldId] = this->fieldValue;
          //          table.writeUnlock();
          //          table.readLock();
        } else {
          //          table.readUnlock();
          //          table.writeLock();
          it->setKey(this->keyValue);
          //          table.writeUnlock();
          //          table.readLock();
        }
        ++partialCounter;
      }
    }
  }
  // table.writeUnlock();
  return partialCounter;
}
QueryResult::Ptr UpdateQuery::threadExecute(int subQuerySize) {
  using namespace std;

  Database &db = Database::getInstance();
  Table::SizeType totalCounter = 0;
  auto &table = db[this->targetTable];

  table.readLock();

  if (this->operands[0] == "KEY") {
    this->keyValue = this->operands[1];
  } else {
    this->fieldId = table.getFieldIndex(this->operands[0]);
    this->fieldValue =
        (Table::ValueType)strtol(this->operands[1].c_str(), nullptr, 10);
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

  table.readUnlock();

  /* sum up */
  for (auto &future : futures) {
    auto result = future.get();
    totalCounter += result;
  }

  return make_unique<RecordCountResult>(totalCounter);
}

// using namespace std;
// if (this->operands.size() != 2)
//  return make_unique<ErrorMsgResult>(
//      qname, this->targetTable.c_str(),
//      "Invalid number of operands (? operands)."_f % operands.size());
// Database &db = Database::getInstance();
// Table::SizeType counter = 0;
// try {
//  auto &table = db[this->targetTable];
//  table.writeLock();
//  if (this->operands[0] == "KEY") {
//    this->keyValue = this->operands[1];
//  } else {
//    this->fieldId = table.getFieldIndex(this->operands[0]);
//    this->fieldValue =
//        (Table::ValueType)strtol(this->operands[1].c_str(), nullptr, 10);
//  }
//  auto result = initCondition(table);
//  if (result.second) {
//    for (auto it = table.begin(); it != table.end(); ++it) {
//      if (this->evalCondition(*it)) {
//        if (this->keyValue.empty()) {
//          (*it)[this->fieldId] = this->fieldValue;
//        } else {
//          it->setKey(this->keyValue);
//        }
//        ++counter;
//      }
//    }
//  }
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
