//
// Created by Li on 21-11-4.
//

#include "TruncateTableQuery.h"

#include "../../db/Database.h"

constexpr const char *TruncateTableQuery::qname;

QueryResult::Ptr TruncateTableQuery::execute() {
  using namespace std;
  Database &db = Database::getInstance();
  try {
    // db.dbLock();
    auto table = &db[this->targetTable];
    table->clear();
    // db.dbUnlock();
    // return make_unique<SuccessMsgResult>(qname);
    return make_unique<NullQueryResult>();
  } catch (const TableNameNotFound &e) {
    return make_unique<ErrorMsgResult>(qname, targetTable, "No such table."s);
  } catch (const exception &e) {
    return make_unique<ErrorMsgResult>(qname, e.what());
  }
}

std::string TruncateTableQuery::toString() {
  return "QUERY = TRUNCATE, Table = \"" + targetTable + "\"";
}