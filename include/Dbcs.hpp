// Copyright 2021 <elizavetamaikova>

#ifndef INCLUDE_DBCS_HPP_
#define INCLUDE_DBCS_HPP_

#include <rocksdb/db.h>
#include <time.h>

#include <ThreadPool.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/log/sinks.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/utility/setup.hpp>
#include <boost/program_options.hpp>
#include <iostream>
#include <map>
#include <mutex>
#include <picosha2.hpp>
#include <string>
#include <vector>

/**
 * @brief Class to operate with existed ( created by time ) rocksdb DataBase
 * @class Dbcs
 */

class Dbcs {
 public:
  Dbcs();
  void read_db(int thread_count, std::string &path, std::string &output);
  void producer(int thread_count, std::string &path, std::string &output);
  void create_start_db(int thread_count, std::string &path,
                       std::string& output);
  void init_log(std::string log_str);
  void log(std::string message);
  void out_help();

 private:
  std::string log_level;
  std::vector<std::string> families_;
  std::vector<std::string> families_hash;
  int columnSize_ = 5;
  int familyNum_ = 3;
  rocksdb::DB *db_;
  rocksdb::DB *db_hash;
  std::vector<rocksdb::ColumnFamilyHandle *> handles_hash;
  rocksdb::WriteBatch batch;
};

#endif  // INCLUDE_DBCS_HPP_
