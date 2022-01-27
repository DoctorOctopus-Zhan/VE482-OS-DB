//
// Created by Zhan on 21-10-27.
//

#ifndef PROJECT_MAXQUERY_H
#define PROJECT_MAXQUERY_H

#include "../Query.h"

class MaxQuery : public ComplexQuery {
  static constexpr const char *qname = "MAX";
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
   * @return the sums, in fieldValue. (return by value, efficiency provided by
   * move-semantic)
   */
  std::vector<Table::ValueType>
  partialExecute(Table::Iterator begin, Table::Iterator end, Table &table);
  QueryResult::Ptr threadExecute(int subQuerySize) override;
  std::string toString() override;
};

#endif // PROJECT_MAXQUERY_H
