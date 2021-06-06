//"Copyright [year] <Copyright Owner>"
//
// Created by mrbgn on 4/21/21.
//

#include "Dbcs.hpp"

namespace logging = boost::log;
namespace attrs = boost::log::attributes;
namespace src = boost::log::sources;
namespace sinks = boost::log::sinks;
namespace expr = boost::log::expressions;
namespace keywords = boost::log::keywords;


Dbcs::Dbcs(std::string &log_level, uint32_t &thread_count, std::string &output,
           std::string &input)
    : _dbReadPath(input), _dbWritePath(output), _threadPool(thread_count), _logLevel(log_level) {
  rocksdb::Options option;
  option.create_if_missing = false;
  rocksdb::Status s = rocksdb::DB::OpenForReadOnly(option, input, &_dbRead);
  if (!s.ok()) {
    option.create_if_missing = true;
    s = rocksdb::DB::Open(option, input, &_dbRead);
    CreateTestDataBase();
  }

  option.create_if_missing = true;
  s = rocksdb::DB::Open(option, output, &_dbWrite);
  if (!s.ok()) {
    std::cout << "error occured with creating db" << std::endl;
    return;
  }
  ReadFromDataBaseSync();
}

/**
 * @brief iterative adding random key-value pair in 1 thread
 */
void Dbcs::CreateTestDataBase() {
  rocksdb::WriteOptions write_options;
  write_options.sync = false;
  srand((unsigned)time(NULL));
  for (uint32_t i = 0; i < 100; ++i)
    _dbRead->Put(write_options, std::to_string(std::rand()),
                 std::to_string(std::rand()));
}
/**
 * iterative reading db in 1 thread and forwarding to multiple thread's consumer
 */
void Dbcs::ReadFromDataBaseSync() {
  rocksdb::Iterator *it = _dbRead->NewIterator(rocksdb::ReadOptions());
  it->SeekToLast();
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    KeyValuePair pair{it->key(), it->value()};
    _threadPool.enqueue(&Dbcs::AddNewKeyHashEntry, std::ref(*this), pair);
  }
  delete it;
}

void Dbcs::AddNewKeyHashEntry(KeyValuePair &pair) {
  rocksdb::Status s;
  rocksdb::WriteOptions write_options;
  write_options.sync = false;

  s = _dbWrite->Put(write_options, pair.key,
                    picosha2::hash256_hex_string(pair.value.ToString()));
  WriteLog(KeyValuePair{pair.key,
                        picosha2::hash256_hex_string(pair.value.ToString())},
           s.ok());
}

void Dbcs::EnableLogging() {
  const std::string format = "%TimeStamp% <%Severity%> (%ThreadID%): %Message%";
  logging::add_console_log(
      std::cout, keywords::format = "[%TimeStamp%] [%Severity%] %Message%",
      keywords::auto_flush = true);
  logging::add_common_attributes();
}
void Dbcs::WriteLog(KeyValuePair &&pair, bool &&success) {
  if (_logLevel == "trace") {
    if (success)
      BOOST_LOG_TRIVIAL(trace)
          << std ::endl
          << "!Wrote pair [" << pair.key.ToString() << "] hash is ["
          << picosha2::hash256_hex_string(pair.value.ToString()) << "]!"
          << std::endl;
    else
      BOOST_LOG_TRIVIAL(trace)
          << std ::endl
          << "!Error writing pair [" << pair.key.ToString() << "] hash is ["
          << picosha2::hash256_hex_string(pair.value.ToString()) << "]!"
          << std::endl;
  } else if (_logLevel == "error") {
    if (success)
      BOOST_LOG_TRIVIAL(error)
          << std ::endl
          << "!Wrote pair [" << pair.key.ToString() << "] hash is ["
          << picosha2::hash256_hex_string(pair.value.ToString()) << "]!"
          << std::endl;
    else
      BOOST_LOG_TRIVIAL(error)
          << std ::endl
          << "!Error writing pair [" << pair.key.ToString() << "] hash is ["
          << picosha2::hash256_hex_string(pair.value.ToString()) << "]!"
          << std::endl;
  } else if (_logLevel == "info") {
    if (success)
      BOOST_LOG_TRIVIAL(info)
          << std ::endl
          << "!Wrote pair [" << pair.key.ToString() << "] hash is ["
          << picosha2::hash256_hex_string(pair.value.ToString()) << "]!"
          << std::endl;
    else
      BOOST_LOG_TRIVIAL(info)
          << std ::endl
          << "!Error writing pair [" << pair.key.ToString() << "] hash is ["
          << picosha2::hash256_hex_string(pair.value.ToString()) << "]!"
          << std::endl;
  } else {
    if (success)
      BOOST_LOG_TRIVIAL(fatal)
          << std ::endl
          << "!Wrote pair [" << pair.key.ToString() << "] hash is ["
          << picosha2::hash256_hex_string(pair.value.ToString()) << "]!"
          << std::endl;
    else
      BOOST_LOG_TRIVIAL(fatal)
          << std ::endl
          << "!Error writing pair [" << pair.key.ToString() << "] hash is ["
          << picosha2::hash256_hex_string(pair.value.ToString()) << "]!"
          << std::endl;
  }
}
void Dbcs::AnotherReadFromDb() {
  rocksdb::Options options;
  options.create_if_missing = false;
  rocksdb::Status status = rocksdb::DB::OpenForReadOnly(options, _dbReadPath, &_dbRead);
  if (!status.ok()) CreateTestDataBase();
  options.create_if_missing = true;
  status = rocksdb::DB::OpenForReadOnly(options, _dbReadPath, &_dbRead);
  if (!status.ok())
  {
    BOOST_LOG_TRIVIAL(fatal)
      << std ::endl
      << "!Error Creating test Database!" << std::endl;
    return;
  }
  _dbRead->ListColumnFamilies(options, _dbReadPath, &_families);
  delete _dbRead;
}
