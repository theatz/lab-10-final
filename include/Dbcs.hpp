//"Copyright [year] <Copyright Owner>"
//
// Created by mrbgn on 4/21/21.
//

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>

#include <boost/log/common.hpp>
#include <boost/log/core.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/expressions/keyword.hpp>
#include <boost/log/sinks.hpp>
#include <boost/log/sinks/text_file_backend.hpp>
#include <boost/log/sources/logger.hpp>
#include <boost/log/sources/record_ostream.hpp>
#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>
#include <boost/log/utility/setup/console.hpp>
#include <ctime>
#include <iostream>
#include <string>

#include "PicoSHA.hpp"
#include "ThreadPool.hpp"

#ifndef INCLUDE_DBCS_HPP_
#define INCLUDE_DBCS_HPP_

/**
 * @brief Struct to forward key-value pairs to consumer
 * @param key Key from key-value pair
 * @param value value from key-value pair
 */

struct KeyValuePair {
  rocksdb::Slice key;
  rocksdb::Slice value;
};

struct CycledList {
  uint32_t length;
  uint32_t position;
  CycledList(uint32_t len) : length(len){position = 0;}
  uint32_t NextIndex(){
    position = (position + 1) % length;
    return position;
  }
};

/**
 * @brief Class to operate with existed ( created by time ) rocksdb DataBase
 * @class Dbcs
 *
 * @param log_level Log level to Boost_log; if log_level is unexpected it's set
 * to fatal
 * @param thread_count Quantity of threads to operate with db that is equal to
 * quantity of families in output db
 * @param output Link to output db
 * @param input Link to input db
 *
 */

class Dbcs {
 public:
  /**
   * @todo operation with ThreadPool
   * @param log_level
   * @param thread_count
   * @param output
   */

  Dbcs(std::string& log_level, uint32_t& thread_count, std::string& output,
       std::string& input);
  /**
   * @brief void to create test db with random key-value pairs
   */
  void CreateTestDataBase();
  /**
   *  @brief void to read from db in one threads
   */
  void ReadFromDataBaseSync();

  /**
   * @brief void to create new key-hash entry in multiple threads
   */
  void AddNewKeyHashEntry(KeyValuePair& pair);

  /**
   * @brief void to enable logging via Boost::Log
   */

  void EnableLogging();

  /**
   * @brief void to log adding new key-value pairs
   * @param pair key-value pair to write in new db
   * @param success gives us info about Succeded/No wirting to new db
   */
  void WriteLog(KeyValuePair&& pair, bool&& success);

  /**
   * @brief void to read from db supporting families
   */

  void AnotherReadFromDb();

 private:
  std::string _dbReadPath;
  std::string _dbWritePath;
  ThreadPool _threadPool;
  rocksdb::DB* _dbRead;
  rocksdb::DB* _dbWrite;
  std::string _logLevel;
  std::vector<std::string> _families;
  //  std::string _kDBPath;
};

#endif  // INCLUDE_DBCS_HPP_
