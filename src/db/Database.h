//
// Created by liu on 18-10-23.
//

#ifndef PROJECT_DB_H
#define PROJECT_DB_H

#include <atomic>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "Table.h"

class Database {
private:
  /**
   * A unique pointer to the global database object
   */
  static std::unique_ptr<Database> instance;

  /**
   * The map of tableName -> table unique ptr
   */
  std::unordered_map<std::string, Table::Ptr> tables;

  /**
   * The map of fileName -> tableName
   */
  std::unordered_map<std::string, std::string> fileTableNameMap;

  /**
   * The big lock to protect the whole database
   */
  std::mutex dbMutex;

  // /**
  //  * The map of fileName -> readerLock
  //  */
  // std::unordered_map<std::string, std::unique_ptr<std::mutex>> readerLockMap;

  // /**
  //  * The map of fileName -> writerLock
  //  */
  // std::unordered_map<std::string, std::unique_ptr<std::mutex>> writerLockMap;

  /**
   * The default constructor is made private for singleton instance
   */
  Database() = default;

public:
  void dbLock();

  void dbUnlock();

  void testDuplicate(const std::string &tableName);

  Table &registerTable(Table::Ptr &&table);

  void dropTable(const std::string &tableName);

  void clearTable(const std::string &tableName);

  void printAllTable();

  Table &operator[](const std::string &tableName);

  const Table &operator[](const std::string &tableName) const;

  Database &operator=(const Database &) = delete;

  Database &operator=(Database &&) = delete;

  Database(const Database &) = delete;

  Database(Database &&) = delete;

  ~Database() = default;

  static Database &getInstance();

  void updateFileTableName(const std::string &fileName,
                           const std::string &tableName);

  std::string getFileTableName(const std::string &fileName);

  /**
   * Load a table from an input stream (i.e., a file)
   * @param is
   * @param source
   * @return reference of loaded table
   */
  Table &loadTableFromStream(std::istream &is, std::string source = "");

  Table &copyTableFromTable(const std::string &srcTableName,
                            const std::string &dstTableName);

  void exit();
};

#endif // PROJECT_DB_H
