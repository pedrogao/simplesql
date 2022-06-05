#include <cstdio>
#include <sstream>
#include <string>

#include "db.h"
#include "workflow/WFHttpServer.h"

int main(int argc, char *argv[]) {
  if (argc <= 1) {
    std::cerr << "Usage: ./spsql_d [PATH]" << std::endl;
    return -1;
  }
  spdlog::set_level(spdlog::level::debug);
  std::string path = argv[1];
  spdlog::debug("init sql db at {0}", path);
  SimpleSQL db_instance(std::move(path));

  WFHttpServer server([&db_instance](WFHttpTask *task) {
    auto req = task->get_req();
    const void *body;
    size_t len;
    req->get_parsed_body(&body, &len);
    spdlog::info("req body size : {}", len);
    std::string query{static_cast<const char *>(body), len};

    // std::string query{""};
    auto result = db_instance.ParseSQL(query);

    if (!result.isValid()) {
      std::stringstream ss;
      ss << result.errorMsg() << ", at (" << result.errorLine() << ":" << result.errorColumn() << ")\n";
      task->get_resp()->append_output_body(ss.str());
      return;
    }

    db_instance.Execute(result);
    task->get_resp()->append_output_body("execute successful!");
  });

  if (server.start(8888) == 0) {  // start server on port 8888
    getchar();                    // press "Enter" to end.
    server.stop();
  }

  return 0;
}