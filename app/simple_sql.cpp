#include <iostream>
#include <map>
#include <string>

#include "db.h"
#include "spdlog/spdlog.h"

int main(int argc, char *argv[]) {
  if (argc <= 1) {
    std::cerr << "Usage: ./example \"SELECT * FROM test;" << std::endl;
    return -1;
  }
  spdlog::set_level(spdlog::level::debug);
  std::string path = "test.db";
  spdlog::debug("init sql db at {0}", path);
  SimpleSQL db_instance(std::move(path));

  std::string query = argv[1];
  // parse a given query
  auto result = db_instance.ParseSQL(query);
  // check whether the parsing was successful
  if (!result.isValid()) {
    std::cerr << "Given string is not a valid SQL query" << std::endl;
    std::cerr << result.errorMsg() << ", at (" << result.errorLine() << ":" << result.errorColumn() << ")\n"
              << std::endl;
    return -1;
  }

  std::cout << "Parsed successfully!" << std::endl;
  std::cout << "Number of statements: " << result.size() << std::endl;

  db_instance.Execute(result);

  return 0;
}
