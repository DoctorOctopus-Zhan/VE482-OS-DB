#include "ListenQuery.h"
#include "../../utils/uexception.h"
#include <fstream>
#include <iostream>
#include <string>

QueryResult::Ptr ListenQuery::execute() {
  using namespace std;
  ifstream ifs(this->filePath);
  if (!ifs.is_open()) {
    return make_unique<ListenErrorMsgResult>(this->filePath);
  }
  ifs.close();
  string fileName;
  auto const pos = this->filePath.find_last_of('/');
  if (pos != string::npos) {
    auto const pos_dot = this->filePath.find_last_of('.');
    if (pos_dot != string::npos) {
      fileName = this->filePath.substr(pos + 1, pos_dot - pos - 1);
    } else {
      fileName = this->filePath.substr(pos + 1);
    }
  } else {
    auto const pos_dot = this->filePath.find_first_of('.');
    if (pos_dot != string::npos) {
      fileName = this->filePath.substr(0, pos);
    } else {
      fileName = this->filePath;
    }
  }
  return make_unique<SuccessMsgResult>("ANSWER = ( listening from ? )"_f %
                                       filePath);
}

std::string ListenQuery::toString() {
  if (this->filePath.empty()) {
    return "QUERY = LISTEN, FILE NOT FOUND";
  }
  return "QUERY = LISTEN, FILE = \"" + this->filePath + "\"";
}

std::string extractQuery(std::istream &is) {
  std::string buf;
  do {
    int ch = is.get();
    if (ch == ';')
      return buf;
    if (ch == EOF)
      throw std::ios_base::failure("End of input");
    buf.push_back((char)ch);
  } while (true);
}

void ListenQuery::listen(size_t &counter, QueryParser &p) {
  using namespace std;
  // if invalid argument
  if (this->filePath.empty()) {
    UnableToOpenFile err("");
    throw err;
  }
  // if fail to open file
  ifstream ifs(this->filePath);
  if (!ifs.is_open()) {
    FailListenFile err(this->filePath);
    return;
  }
  // while loop
  istream is(ifs.rdbuf());
  auto &pool = ThreadPool::getInstance();
  while (is) {
    try {
      // A very standard REPL
      // REPL: Read-Evaluate-Print-Loop
      std::string queryStr = extractQuery(is);
      Query::Ptr query = p.parseQuery(queryStr);
      // quit the main thread after join others
      if (query->toString() == "QUERY = Quit") {
        exit(0);
      }
      QueryResult::Ptr res = nullptr;
      std::future<QueryResult::Ptr> result =
          pool.enqueTask([&query]() { return query->execute(); });
      res = result.get();
      std::cout << ++counter << "\n";
      if (res->success()) {
        if (res->display()) {
          std::cout << *res;
        } else {
#ifndef NDEBUG
          std::cout.flush();
          std::cerr << *res;
#endif
        }
      } else {
        std::cout.flush();
        std::cerr << "QUERY FAILED:\n\t" << *res;
      }
      // if listen to some file
      if (query->toString().compare(0, 14, "QUERY = LISTEN") == 0) {
        ListenQuery *ptr = dynamic_cast<ListenQuery *>(query.get());
        ptr->listen(counter, p);
      }
    } catch (const std::ios_base::failure &e) {
      // End of input
      break;
    } catch (const std::exception &e) {
      std::cout.flush();
      std::cerr << e.what() << std::endl;
    }
  }
  ifs.close();
  //   string fileName;
  //   auto const pos = this->filePath.find_last_of('/');
  //   if (pos != string::npos) {
  //     auto const pos_dot = this->filePath.find_last_of('.');
  //     if (pos_dot != string::npos) {
  //         fileName = this->filePath.substr(pos + 1, pos_dot - pos - 1);
  //     } else {
  //         fileName = this->filePath.substr(pos + 1);
  //     }
  //   } else {
  //     auto const pos_dot = this->filePath.find_last_of('.');
  //     if (pos_dot != string::npos) {
  //         fileName = this->filePath.substr(0, pos);
  //     } else {
  //         fileName = this->filePath;
  //     }
  //   }
  //   return make_unique<SuccessMsgResult>("ANSWER = ( listening from ? )"_f %
  //   fileName);
}
