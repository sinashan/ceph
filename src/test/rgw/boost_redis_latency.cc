#include <thread>
#include <gtest/gtest.h>

#include <boost/asio/io_context.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/strand.hpp>
#include <boost/redis/connection.hpp>
#include <spawn/spawn.hpp>

namespace net = boost::asio;
using boost::redis::config;
using boost::redis::connection;
using boost::redis::request;
using boost::redis::response;

class ConnectionFixture: public ::testing::Test {
    
  protected:
    virtual void SetUp() {
      conn = std::make_shared<connection>(boost::asio::make_strand(io));

      ASSERT_NE(conn, nullptr);
      
      config cfg;
      //cfg.health_check_interval = std::chrono::seconds{0};
      cfg.addr.host = "127.0.0.1";
      cfg.addr.port = "6379";

      conn->async_run(cfg, {}, net::detached);
    }

    virtual void TearDown() {
      //delete conn;
    }

    net::io_context io;
    std::shared_ptr<connection> conn;
};

TEST_F(ConnectionFixture, DelayTest) {
  float avg = 0;
  std::list<std::string> vals;
  std::vector<std::string> data;
  
  for (int i = 0; i < 20; ++i) {
    vals.push_back("field" + std::to_string(i));
    vals.push_back("value" + std::to_string(i));
    data.push_back("field" + std::to_string(i));
  }

  for (int i = 0; i < 100; ++i) {
    clock_t t = clock();

    spawn::spawn(io, [this, &vals, &data] (spawn::yield_context yield) {
      request req;
      response<int, std::vector<std::string>, boost::redis::ignore_t> resp;

      boost::system::error_code ec;
      req.push_range("HSET", "key", vals);
      req.push_range("HMGET", "key", data);
      req.push("FLUSHALL");

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ(std::get<0>(resp).value(), 20);
      EXPECT_EQ(std::get<1>(resp).value().size(), 20);
      conn->cancel();
    });

    io.run();

    const double runtime = (clock() - t) / double(CLOCKS_PER_SEC);
    avg += runtime; 
  }

  std::cout << "Average runtime of " << avg / 100 << std::endl;
}

int main(int argc, char *argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

