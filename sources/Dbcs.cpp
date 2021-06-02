// Copyright 2021 <elizavetamaikova>

#include <Dbcs.hpp>
namespace logging = boost::log;
namespace attrs = boost::log::attributes;
namespace src = boost::log::sources;
namespace sinks = boost::log::sinks;
namespace expr = boost::log::expressions;
namespace keywords = boost::log::keywords;

Dbcs::Dbcs() {}

void Dbcs::out_help() {
  std::string out_info;
  out_info = R"(Options:
  --log-level <string>              = "info"|"warning"|"error"
                                    = default: "error"
  --thread-count <number>           =
                                    = default: count of logical core
  --output <path>                   = <path/to/output/storage.db>
                                    = default: <path/to/input/dbcs-storage.db>)";
  std::cout << out_info << std::endl;
}

void Dbcs::init_log(std::string log_str) {
  log_level = log_str;
  const std::string format = "%TimeStamp% <%Severity%> (%ThreadID%): %Message%";
  logging::add_console_log(
      std::cout, keywords::format = "[%TimeStamp%] [%Severity%] %Message%",
      keywords::auto_flush = true);
  logging::add_common_attributes();
}

void Dbcs::create_start_db(int thread_count, std::string &path,
                           std::string &output) {
  rocksdb::Options options;
  options.create_if_missing = true;
  std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
  column_families.push_back(rocksdb::ColumnFamilyDescriptor(
      rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions()));

  rocksdb::Status status = rocksdb::DB::Open(options, path, &db_);

  assert(status.ok());

  for (int i = 0; i < familyNum_; ++i) {
    rocksdb::ColumnFamilyHandle *cf;
    status = db_->CreateColumnFamily(rocksdb::ColumnFamilyOptions(),
                                     "family_" + std::to_string(i), &cf);
    assert(status.ok());
    column_families.push_back(rocksdb::ColumnFamilyDescriptor(
        "family_" + std::to_string(i), rocksdb::ColumnFamilyOptions()));
    families_.push_back("family_" + std::to_string(i));
    db_->DestroyColumnFamilyHandle(cf);
  }
  delete db_;

  status = rocksdb::DB::Open(options, output, &db_hash);
  assert(status.ok());

  for (int i = 0; i < familyNum_; ++i) {
    rocksdb::ColumnFamilyHandle *cf;
    status = db_hash->CreateColumnFamily(rocksdb::ColumnFamilyOptions(),
                                         "family_" + std::to_string(i), &cf);
    assert(status.ok());
    db_hash->DestroyColumnFamilyHandle(cf);
  }
  delete db_hash;

  std::vector<rocksdb::ColumnFamilyHandle *> handles;
  status = rocksdb::DB::Open(rocksdb::DBOptions(), path, column_families,
                             &handles, &db_);
  assert(status.ok());
  for (size_t i = 0; i < column_families.size(); ++i) {
    for (int k = 0; k < columnSize_; ++k) {
      status = db_->Put(
          rocksdb::WriteOptions(), handles[i],
          rocksdb::Slice("key_" + std::to_string(i) + std::to_string(k)),
          rocksdb::Slice("value_" + std::to_string(i) + std::to_string(k)));
      assert(status.ok());
    }
  }

  for (auto &handle : handles) {
    status = db_->DestroyColumnFamilyHandle(handle);
    assert(status.ok());
  }

  delete db_;
  producer(thread_count, path, output);
}

void Dbcs::log(std::string message) {
  if (log_level == "trace") {
    BOOST_LOG_TRIVIAL(trace) << std ::endl << message << std::endl;
  } else if (log_level == "error") {
    BOOST_LOG_TRIVIAL(error) << std ::endl << message << std::endl;
  } else if (log_level == "info") {
    BOOST_LOG_TRIVIAL(info) << std ::endl << message << std::endl;
  } else {
    BOOST_LOG_TRIVIAL(fatal) << std ::endl << message << std::endl;
  }
}
void Dbcs::read_db(int thread_count, std::string &path, std::string &output) {
  rocksdb::Options options;
  options.create_if_missing = false;

  rocksdb::Status status = rocksdb::DB::OpenForReadOnly(options, path, &db_);

  if (!status.ok()) {
    create_start_db(thread_count, path, output);
    return;
  }

  assert(status.ok());

  db_->ListColumnFamilies(options, path, &families_);

  delete db_;

  families_.erase(families_.begin());

  options.create_if_missing = true;
  options.error_if_exists = true;
  status = rocksdb::DB::Open(options, output, &db_hash);

  if (!status.ok()) {
    std::vector<std::string> families;
    db_hash->ListColumnFamilies(options, output, &families);
    std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
    for (auto &family : families)
      column_families.push_back(rocksdb::ColumnFamilyDescriptor(
          family, rocksdb::ColumnFamilyOptions()));
    rocksdb::DestroyDB(output, options, column_families);
    delete db_hash;

    options.create_if_missing = true;
    options.error_if_exists = false;
    status = rocksdb::DB::Open(options, output, &db_hash);
  }
  assert(status.ok());

  for (auto &family : families_) {
    rocksdb::ColumnFamilyHandle *cf;
    status = db_hash->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), family,
                                         &cf);
    assert(status.ok());
    db_hash->DestroyColumnFamilyHandle(cf);
  }
  delete db_hash;

  producer(thread_count, path, output);
}

void Dbcs::producer(int thread_count, std::string &path, std::string &output) {
  std::vector<rocksdb::ColumnFamilyHandle *> handles;

  std::vector<rocksdb::ColumnFamilyDescriptor> column_families;

  column_families.push_back(rocksdb::ColumnFamilyDescriptor(
      rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions()));

  for (auto &family : families_)
    column_families.push_back(rocksdb::ColumnFamilyDescriptor(
        family, rocksdb::ColumnFamilyOptions()));

  rocksdb::Status status = rocksdb::DB::OpenForReadOnly(
      rocksdb::DBOptions(), path, column_families, &handles, &db_);

  assert(status.ok());

  status = rocksdb::DB::Open(rocksdb::DBOptions(), output, column_families,
                             &handles_hash, &db_hash);

  assert(status.ok());

  ThreadPool TP(thread_count);
  for (int i = 0; i < column_families.size(); ++i) {
    rocksdb::Iterator *it =
        db_->NewIterator(rocksdb::ReadOptions(), handles[i]);
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      std::string key = it->key().ToString();
      std::string value = it->value().ToString();
      log("adding task pair: [key " + key + "; value " + value + " ]" +
          "[ family : " + std::to_string(i) + " ]");

      TP.enqueue([this, &key, &value, i] {
        rocksdb::WriteOptions options;
        options.sync = true;
        std::string hash = picosha2::hash256_hex_string(value);
        log("writing pair: [key " + key + "; value " + value + " ]" +
            "[ family : " + std::to_string(i) + " ]");
        rocksdb::Status status = batch.Put(handles_hash[i], key, hash);
        assert(status.ok());

        log("wrote pair: [key " + key + "; value " + hash + " ]" +
            "[ family : " + std::to_string(i) + " ]");
      });
    }
    delete it;
  }

  status = db_hash->Write(rocksdb::WriteOptions(), &batch);
  assert(status.ok());

  for (auto &handle : handles) {
    status = db_hash->DestroyColumnFamilyHandle(handle);
    assert(status.ok());
  }

  delete db_;

  for (auto &handle : handles_hash) {
    status = db_hash->DestroyColumnFamilyHandle(handle);
    assert(status.ok());
  }

  delete db_hash;
}
