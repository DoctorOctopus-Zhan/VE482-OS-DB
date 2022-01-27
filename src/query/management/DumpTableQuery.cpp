//
// Created by liu on 18-10-25.
//

#include "DumpTableQuery.h"

#include <fstream>

#include "../../db/Database.h"

constexpr const char *DumpTableQuery::qname;

QueryResult::Ptr DumpTableQuery::execute() {
  using namespace std;
  auto &db = Database::getInstance();
  try {
    // db.dbLock();
    ofstream outfile(this->fileName);
    if (!outfile.is_open()) {
      return make_unique<ErrorMsgResult>(qname, "Cannot open file '?'"_f %
                                                    this->fileName);
    }
    outfile << db[this->targetTable];
    outfile.close();
    // db.dbUnlock();
    // return make_unique<SuccessMsgResult>(qname, targetTable);
    return make_unique<NullQueryResult>();
  } catch (const exception &e) {
    return make_unique<ErrorMsgResult>(qname, e.what());
  }
}

std::string DumpTableQuery::toString() {
  return "QUERY = Dump TABLE, FILE = \"" + this->fileName + "\"";
}
