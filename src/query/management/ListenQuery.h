//
// Created by boqun on 21-11-21.
//

#ifndef PROJECT_LISTENQUERY_H
#define PROJECT_LISTENQUERY_H

#include "../../threadPool/ThreadPool.h"
#include "../Query.h"
#include "../QueryBuilders.h"
#include "../QueryParser.h"

class ListenQuery : public Query {
  static constexpr const char *qname = "LISTEN";
  std::string filePath;

public:
  explicit ListenQuery(std::string filePath) : filePath(filePath) {}

  QueryResult::Ptr execute() override;

  void listen(size_t &counter, QueryParser &p);

  std::string toString() override;
};

#endif // PROJECT_LISTENQUERY_H
