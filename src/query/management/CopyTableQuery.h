//
// Created by leo on 2021/10/31.
//

#ifndef LEMONDB_COPYTABLEQUERY_H
#define LEMONDB_COPYTABLEQUERY_H
#include "../Query.h"

class CopyTableQuery : public Query {
  static constexpr const char *qname = "COPYTABLE";

public:
  const std::string srcTable;

  explicit CopyTableQuery(std::string srcTableName, std::string dstTableName)
      : Query(std::move(dstTableName)), srcTable(std::move(srcTableName)) {}

  QueryResult::Ptr execute() override;

  std::string toString() override;
};

#endif // LEMONDB_COPYTABLEQUERY_H
