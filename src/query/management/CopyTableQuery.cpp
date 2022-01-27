//
// Created by leo on 2021/10/31.
//
#include "CopyTableQuery.h"

#include "../../db/Database.h"

constexpr const char *CopyTableQuery::qname;

QueryResult::Ptr CopyTableQuery::execute() {
  using namespace std;
  Database &db = Database::getInstance();
  try {
    // db.dbLock();
    db.copyTableFromTable(this->srcTable, this->targetTable);
    // db.dbUnlock();
    // return make_unique<SuccessMsgResult>(qname, targetTable);
    return make_unique<NullQueryResult>();
  } catch (const TableNameNotFound &e) {
    return make_unique<ErrorMsgResult>(qname, srcTable, "No such table."s);
  } catch (const exception &e) {
    return make_unique<ErrorMsgResult>(qname, e.what());
  }
}

std::string CopyTableQuery::toString() {
  return "QUERY = Copy TABLE, from " + this->srcTable + " to " +
         this->targetTable;
}
