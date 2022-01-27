//
// Created by liu on 18-10-23.
//

#include "Table.h"

#include <iomanip>
#include <iostream>
#include <sstream>

#include "Database.h"

constexpr const Table::ValueType Table::ValueTypeMax;
constexpr const Table::ValueType Table::ValueTypeMin;
//#define YUXIANG_DEBUG
#ifdef YUXIANG_DEBUG
#include <cstdio> // only for atomic output
#endif
void Table::readLock() {
//  this->readerMutex.lock();
//  if (++this->readerCount == 1) {
//    this->writerMutex.lock();
//  }
#ifdef YUXIANG_DEBUG
  printf("lock reader lock, now there is %d reader lock\n", readerCount);
#endif
  this->readerMutex.unlock();
}

void Table::readUnlock() {
//  this->readerMutex.lock();
//  if (--this->readerCount == 0) {
//    this->writerMutex.unlock();
#ifdef YUXIANG_DEBUG
  printf("unlock writer lock!\n");
#endif
//  }
#ifdef YUXIANG_DEBUG
  printf("unlock reader lock, now there is %d reader lock\n", readerCount);
#endif
  //  this->readerMutex.unlock();
}

void Table::writeLock() {
#ifdef YUXIANG_DEBUG
  printf("try to get writer lock\n");
#endif
//  this->writerMutex.lock();
#ifdef YUXIANG_DEBUG
  printf("get writer lock successfully\n");
#endif
}

void Table::writeUnlock() {
//  this->writerMutex.unlock();
#ifdef YUXIANG_DEBUG
  printf("unlock the writer lock!!!!!!!!!!!!!!\n");
#endif
}

Table::FieldIndex
Table::getFieldIndex(const Table::FieldNameType &field) const {
  try {
    return this->fieldMap.at(field);
  } catch (const std::out_of_range &e) {
    throw TableFieldNotFound(R"(Field name "?" doesn't exists.)"_f % (field));
  }
}

void Table::insertByIndex(KeyType key, std::vector<ValueType> &&data) {
  if (this->keyMap.find(key) != this->keyMap.end()) {
    std::string err = "In Table \"" + this->tableName + "\" : Key \"" + key +
                      "\" already exists!";
    throw ConflictingKey(err);
  }
  this->keyMap.emplace(key, this->data.size());
  this->data.emplace_back(std::move(key), data);
}

// delete a record from table
void Table::deleteByIndex(KeyType key, SizeType count) {
  if (this->keyMap.find(key) == this->keyMap.end()) {
    std::string err = "In Table \"" + this->tableName + "\" : Key \"" + key +
                      "\" not exists!";
    throw ConflictingKey(err);
  }
  // this->data.erase(this->data.begin() + count);
  data[count] = std::move(data.back());
  data.pop_back();
  this->keyMap.erase(this->keyMap.find(key));
}

// swap data
void Table::swapByIndex(SizeType count, FieldIndex fieldId1,
                        FieldIndex fieldId2) {
  auto it = this->data.begin() + static_cast<long>(count);
  int fieldValue_1 = it->datum[fieldId1];
  it->datum[fieldId1] = it->datum[fieldId2];
  it->datum[fieldId2] = fieldValue_1;
}

// add or sub data
void Table::add_subByIndex(SizeType count, FieldIndex fieldDest,
                           ValueType fieldResult) {
  auto it = this->data.begin() + static_cast<long>(count);
  it->datum[fieldDest] = fieldResult;
}

// duplicate data
void Table::duplicateByIndex(SizeType count, size_t &counter) {
  auto it = this->data.begin() + static_cast<long>(count);
  KeyType key = it->key + "_copy";
  if (this->keyMap.find(key) != this->keyMap.end())
    return;
  this->keyMap.emplace(key, this->data.size());
  this->data.emplace_back(std::move(key), it->datum);
  ++counter;
}

Table::Object::Ptr Table::operator[](const Table::KeyType &key) {
  auto it = keyMap.find(key);
  if (it == keyMap.end()) {
    // not found
    return nullptr;
  } else {
    return createProxy(
        data.begin() +
            static_cast<std::vector<Table::Datum>::difference_type>(it->second),
        this);
  }
}

std::ostream &operator<<(std::ostream &os, const Table &table) {
  const int width = 10;
  std::stringstream buffer;
  buffer << table.tableName << "\t" << (table.fields.size() + 1) << "\n";
  buffer << std::setw(width) << "KEY";
  for (const auto &field : table.fields) {
    buffer << std::setw(width) << field;
  }
  buffer << "\n";
  auto numFields = table.fields.size();
  for (const auto &datum : table.data) {
    buffer << std::setw(width) << datum.key;
    for (decltype(numFields) i = 0; i < numFields; ++i) {
      buffer << std::setw(width) << datum.datum[i];
    }
    buffer << "\n";
  }
  return os << buffer.str();
}
