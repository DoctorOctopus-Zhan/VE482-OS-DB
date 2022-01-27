//
// Created by Zhan on 21-10-28.
//

#include "SwapQuery.h"

#include "../../db/Database.h"

constexpr const char *SwapQuery::qname;

QueryResult::Ptr SwapQuery::execute() {
  using namespace std;
  if (this->operands.size() != 2)
    return make_unique<ErrorMsgResult>(
        qname, this->targetTable.c_str(),
        "Invalid number of operands (? operands)."_f % operands.size());

  if (this->operands[0] == "KEY" || this->operands[1] == "KEY")
    return make_unique<ErrorMsgResult>(qname, this->targetTable.c_str(),
                                       "KEY cannot be swapped");

  Database &db = Database::getInstance();
  Table::SizeType counter = 0;
  Table::SizeType count = 0;
  try {
    auto &table = db[this->targetTable];
    // table.readLock();
    // table.writeLock();
    Table::FieldIndex fieldId1 = table.getFieldIndex(this->operands[0]);
    Table::FieldIndex fieldId2 = table.getFieldIndex(this->operands[1]);
    auto result = initCondition(table);
    if (result.second) {
      for (auto it = table.begin(); it != table.end(); ++it) {
        if (this->evalCondition(*it)) {
          ++counter;
          table.swapByIndex(count, fieldId1, fieldId2);
        }
        ++count;
      }
    }
    // table.readUnlock();
    // table.writeUnlock();
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

std::string SwapQuery::toString() {
  return "QUERY = SWAP " + this->targetTable + "\"";
}
