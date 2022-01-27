//
// Created by Li on 21-11-4.
//

#ifndef PROJECT_TRUNCATETABLEQUERY_H
#define PROJECT_TRUNCATETABLEQUERY_H

#include "../Query.h"

class TruncateTableQuery : public Query {
  static constexpr const char *qname = "TRUNCATE";

public:
  using Query::Query;

  QueryResult::Ptr execute() override;

  std::string toString() override;
};

#endif // PROJECT_TRUNCATETABLEQUERY_H
