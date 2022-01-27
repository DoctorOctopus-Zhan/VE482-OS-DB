//
// Created by Zhan on 21-11-03.
//

#ifndef PROJECT_ADDQUERY_H
#define PROJECT_ADDQUERY_H

#include "../../threadPool/ThreadPool.h"
#include "../Query.h"

class AddQuery : public ComplexQuery {
  static constexpr const char *qname = "ADD";
  // vector storing the field index
  std::vector<Table::FieldIndex> fieldId;

public:
  using ComplexQuery::ComplexQuery;

  QueryResult::Ptr execute() override;
  /**
   * process partial query
   * @param begin
   * @param end
   * @param table
   * @return the sum of sizetype
   */
  Table::SizeType partialExecute(Table::Iterator begin, Table::Iterator end,
                                 Table &table);
  QueryResult::Ptr threadExecute(int subQuerySize) override;

  std::string toString() override;
};

#endif // PROJECT_ADDQUERY_H
