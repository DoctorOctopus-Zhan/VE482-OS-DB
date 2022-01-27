//
// Created by liu on 18-10-25.
//

#ifndef PROJECT_UPDATEQUERY_H
#define PROJECT_UPDATEQUERY_H

#include "../Query.h"

class UpdateQuery : public ComplexQuery {
  static constexpr const char *qname = "UPDATE";
  Table::ValueType
      fieldValue; // = (operands[0]=="KEY")? 0 :std::stoi(operands[1]);
  Table::FieldIndex fieldId;
  Table::KeyType keyValue;

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

#endif // PROJECT_UPDATEQUERY_H
