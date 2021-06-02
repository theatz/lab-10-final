#include <Dbcs.hpp>

int main(int argc, char** argv) {
  Dbcs new_dbcs;

  std::string db_path = "../output-rocksdb.db";

  std::string log_level;
  int thread_count;
  std::string output;

  boost::program_options::options_description desc("General options");
  std::string task_type;

  desc.add_options()("help", "")(
      "log-level",
      boost::program_options::value<std::string>()->default_value(
          "error"))("thread-count",
                    boost::program_options::value<int>()->default_value(
                        std::thread::
                            hardware_concurrency()))("output",
                                                     boost::program_options::
                                                         value<std::string>()
                                                             ->default_value(
                                                                 "../"
                                                                 "dbcs-storage."
                                                                 "db"));

  boost::program_options::variables_map vm;
  try {
    boost::program_options::parsed_options parsed =
        boost::program_options::command_line_parser(argc, argv)
            .options(desc)
            .allow_unregistered()
            .run();
    store(parsed, vm);

    if (vm.count("help")) {
      new_dbcs.out_help();
      return 1;
    }

    log_level = vm["log-level"].as<std::string>();
    thread_count = vm["thread-count"].as<int>();
    output = vm["output"].as<std::string>();

    new_dbcs.init_log(log_level);
    //создать тестовую
    //    new_dbcs.create_start_db(thread_count, db_path, output);
    new_dbcs.read_db(thread_count, db_path, output);

  } catch (std::exception& ex) {
    std::cout << desc << std::endl;
  }
}