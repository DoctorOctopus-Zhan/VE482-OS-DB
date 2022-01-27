//
// Created by Zhan on 21-10-27.
//

#ifndef PROJECT_COUNTQUERY_H
#define PROJECT_COUNTQUERY_H

#include "../Query.h"

class CountQuery : public ComplexQuery {
  static constexpr const char *qname = "COUNT";

public:
  using ComplexQuery::ComplexQuery;

  QueryResult::Ptr execute() override;

  Table::SizeType partialExecute(Table::Iterator begin, Table::Iterator end,
                                 Table &table);

  QueryResult::Ptr threadExecute(int subQuerySize) override;

  std::string toString() override;
};

#endif // PROJECT_COUNTQUERY_H
