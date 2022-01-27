//
// Created by Zhan on 21-10-27.
//

#ifndef PROJECT_SELECTQUERY_H
#define PROJECT_SELECTQUERY_H

#include "../Query.h"

class SelectQuery : public ComplexQuery {
  static constexpr const char *qname = "SELECT";
  // vector storing the field index
  std::vector<Table::FieldIndex> fieldId;
  typedef std::vector<std::pair<std::string, std::vector<int>>> ansType;

public:
  using ComplexQuery::ComplexQuery;

  QueryResult::Ptr execute() override;
  /**
   * process partial query
   * @param begin
   * @param end
   * @param table
   * @return the selected values in a vector
   */
  ansType partialExecute(Table::Iterator begin, Table::Iterator end,
                         Table &table);
  QueryResult::Ptr threadExecute(int subQuerySize) override;

  std::string toString() override;
};

#endif // PROJECT_SELECTQUERY_H
