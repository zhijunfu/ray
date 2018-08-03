#include <iostream>
#include <thread>

#include "gtest/gtest.h"

#include "ray/object_manager/object_manager.h"

namespace ray {

static inline void flushall_redis(void) {
  redisContext *context = redisConnect("127.0.0.1", 6379);
  freeReplyObject(redisCommand(context, "FLUSHALL"));
  redisFree(context);
}

std::string store_executable;

class MockServer {
 public:
  MockServer(boost::asio::io_service &main_service,
             const ObjectManagerConfig &object_manager_config,
             std::shared_ptr<gcs::AsyncGcsClient> gcs_client)
      : object_manager_acceptor_(
            main_service, boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), 0)),
        object_manager_socket_(main_service),
        gcs_client_(gcs_client),
        object_manager_(main_service, object_manager_config, gcs_client) {
    RAY_CHECK_OK(RegisterGcs(main_service));
    // Start listening for clients.
    DoAcceptObjectManager();
  }

  ~MockServer() { RAY_CHECK_OK(gcs_client_->client_table().Disconnect()); }

 private:
  ray::Status RegisterGcs(boost::asio::io_service &io_service) {
    RAY_RETURN_NOT_OK(gcs_client_->Connect("127.0.0.1", 6379, /*sharding*/ false));
    RAY_RETURN_NOT_OK(gcs_client_->Attach(io_service));

    boost::asio::ip::tcp::endpoint endpoint = object_manager_acceptor_.local_endpoint();
    std::string ip = endpoint.address().to_string();
    unsigned short object_manager_port = endpoint.port();

    ClientTableDataT client_info = gcs_client_->client_table().GetLocalClient();
    client_info.node_manager_address = ip;
    client_info.node_manager_port = object_manager_port;
    client_info.object_manager_port = object_manager_port;
    ray::Status status = gcs_client_->client_table().Connect(client_info);
    object_manager_.RegisterGcs();
    return status;
  }

  void DoAcceptObjectManager() {
    object_manager_acceptor_.async_accept(
        object_manager_socket_, boost::bind(&MockServer::HandleAcceptObjectManager, this,
                                            boost::asio::placeholders::error));
  }

  void HandleAcceptObjectManager(const boost::system::error_code &error) {
    ClientHandler<boost::asio::ip::tcp> client_handler =
        [this](TcpClientConnection &client) { object_manager_.ProcessNewClient(client); };
    MessageHandler<boost::asio::ip::tcp> message_handler = [this](
        std::shared_ptr<TcpClientConnection> client, int64_t message_type,
        const uint8_t *message) {
      
      object_manager_.ProcessClientMessage(client, message_type, message);
    };
    // Accept a new local client and dispatch it to the node manager.
    auto new_connection = TcpClientConnection::Create(client_handler, message_handler,
                                                      std::move(object_manager_socket_));
    DoAcceptObjectManager();
  }

  friend class TestObjectManager;

  boost::asio::ip::tcp::acceptor object_manager_acceptor_;
  boost::asio::ip::tcp::socket object_manager_socket_;
  std::shared_ptr<gcs::AsyncGcsClient> gcs_client_;
  ObjectManager object_manager_;
};

class TestObjectManagerBase : public ::testing::Test {
 public:
  TestObjectManagerBase() : work(test_service) {}

  std::string StartStore(const std::string &id) {
    std::string store_id = "/tmp/store";
    store_id = store_id + id;
    std::string store_pid = store_id + ".pid";
    std::string plasma_command = store_executable + " -m 1000000000 -s " + store_id +
                                 " 1> /dev/null 2> /dev/null &" + " echo $! > " +
                                 store_pid;

    RAY_LOG(DEBUG) << plasma_command;
    int ec = system(plasma_command.c_str());
    RAY_CHECK(ec == 0);
    sleep(1);
    return store_id;
  }

  void StopStore(std::string store_id) {
    std::string store_pid = store_id + ".pid";
    std::string kill_1 = "kill -9 `cat " + store_pid + "`";
    ASSERT_TRUE(!system(kill_1.c_str()));
  }

  void SetUp() {
    flushall_redis();

    // start store
    store_id_1 = StartStore(UniqueID::from_random().hex());
    store_id_2 = StartStore(UniqueID::from_random().hex());

    uint pull_timeout_ms = 1;
    int max_sends = 2;
    int max_receives = 2;
    uint64_t object_chunk_size = static_cast<uint64_t>(std::pow(10, 3));
    push_timeout_ms = 1000;

    // start first server
    gcs_client_1 = std::shared_ptr<gcs::AsyncGcsClient>(new gcs::AsyncGcsClient());
    ObjectManagerConfig om_config_1;
    om_config_1.store_socket_name = store_id_1;
    om_config_1.pull_timeout_ms = pull_timeout_ms;
    om_config_1.max_sends = max_sends;
    om_config_1.max_receives = max_receives;
    om_config_1.object_chunk_size = object_chunk_size;
    om_config_1.push_timeout_ms = push_timeout_ms;
    server1.reset(new MockServer(main_service, om_config_1, gcs_client_1));

    // start second server
    gcs_client_2 = std::shared_ptr<gcs::AsyncGcsClient>(new gcs::AsyncGcsClient());
    ObjectManagerConfig om_config_2;
    om_config_2.store_socket_name = store_id_2;
    om_config_2.pull_timeout_ms = pull_timeout_ms;
    om_config_2.max_sends = max_sends;
    om_config_2.max_receives = max_receives;
    om_config_2.object_chunk_size = object_chunk_size;
    om_config_2.push_timeout_ms = push_timeout_ms;
    server2.reset(new MockServer(main_service, om_config_2, gcs_client_2));

    test_thread = std::thread(&TestObjectManagerBase::StartTestService, this);

    // connect to stores.
    ARROW_CHECK_OK(client1.Connect(store_id_1, "", plasma::kPlasmaDefaultReleaseDelay));
    ARROW_CHECK_OK(client2.Connect(store_id_2, "", plasma::kPlasmaDefaultReleaseDelay));
  }

  void TearDown() {
    arrow::Status client1_status = client1.Disconnect();
    arrow::Status client2_status = client2.Disconnect();
    ASSERT_TRUE(client1_status.ok() && client2_status.ok());

    this->server1.reset();
    this->server2.reset();

    test_service.stop();
    test_thread.join();

    StopStore(store_id_1);
    StopStore(store_id_2);
  }

  void StartTestService() { test_service.run(); }
  
  ObjectID WriteDataToClient(plasma::PlasmaClient &client, int64_t data_size) {
    return WriteDataToClient(client, data_size, ObjectID::from_random());
  }

  ObjectID WriteDataToClient(plasma::PlasmaClient &client, int64_t data_size,
                             ObjectID object_id) {
    RAY_LOG(DEBUG) << "ObjectID Created: " << object_id;
    uint8_t metadata[] = {5};
    int64_t metadata_size = sizeof(metadata);
    std::shared_ptr<Buffer> data;
    ARROW_CHECK_OK(client.Create(object_id.to_plasma_id(), data_size, metadata,
                                 metadata_size, &data));
    ARROW_CHECK_OK(client.Seal(object_id.to_plasma_id()));
    return object_id;
  }

  void object_added_handler_1(ObjectID object_id) { v1.push_back(object_id); };

  void object_added_handler_2(ObjectID object_id) { v2.push_back(object_id); };

 protected:
  std::thread p;
  boost::asio::io_service main_service;
  boost::asio::io_service test_service;
  boost::asio::io_service::work work;
  std::thread test_thread;
  std::thread thread2;
  std::shared_ptr<gcs::AsyncGcsClient> gcs_client_1;
  std::shared_ptr<gcs::AsyncGcsClient> gcs_client_2;
  std::unique_ptr<MockServer> server1;
  std::unique_ptr<MockServer> server2;

  plasma::PlasmaClient client1;
  plasma::PlasmaClient client2;
  std::vector<ObjectID> v1;
  std::vector<ObjectID> v2;

  std::string store_id_1;
  std::string store_id_2;

  uint push_timeout_ms;
};

class TestObjectManager : public TestObjectManagerBase {
 public:
  int current_wait_test = -1;
  int num_connected_clients = 0;
  ClientID client_id_1;
  ClientID client_id_2;

  ObjectID created_object_id1;
  ObjectID created_object_id2;

  std::unique_ptr<boost::asio::deadline_timer> timer;

  void WaitConnections() {
    client_id_1 = gcs_client_1->client_table().GetLocalClientId();
    client_id_2 = gcs_client_2->client_table().GetLocalClientId();
    gcs_client_1->client_table().RegisterClientAddedCallback([this](
        gcs::AsyncGcsClient *client, const ClientID &id, const ClientTableDataT &data) {
      ClientID parsed_id = ClientID::from_binary(data.client_id);
      if (parsed_id == client_id_1 || parsed_id == client_id_2) {
        num_connected_clients += 1;
      }
      if (num_connected_clients == 2) {
        StartTests();
      }
    });
  }

  void StartTests() {
    TestConnections();
    //TestNotifications();
    TestPlasmaQueue();
  }

  void TestPlasmaQueue() {

    plasma::ObjectID object_id = plasma::ObjectID::from_random();
    std::vector<plasma::ObjectBuffer> object_buffers;

    int64_t queue_size = 100 * 1024 * 1024;
    std::shared_ptr<Buffer> data;
    ARROW_CHECK_OK(client1.CreateQueue(object_id, queue_size, &data));

    RAY_CHECK_OK(server2->object_manager_.Wait(
      {object_id}, -1, 1, false,
      [this, object_id](
          const std::vector<ray::ObjectID> &found,
          const std::vector<ray::ObjectID> &remaining) {

      RAY_CHECK(found.size() == 1 && remaining.empty());
      RAY_CHECK(found[0] == object_id); 
      
      RAY_CHECK_OK(server2->object_manager_.SubscribeQueue(
        object_id,
        [this, object_id](bool success) {
        
          RAY_CHECK(success);
          RAY_LOG(INFO) << "SubscribeQueue callback invoked: succeeded " << object_id;

          test_service.post([this, object_id]() {

          RAY_LOG(INFO) << "Start plasma queue test " << object_id;  
          // Test that the second client can get the object.
          int notify_fd;
          bool has_object = false;
          ARROW_CHECK_OK(client2.GetQueue(object_id, -1, &notify_fd));
          ARROW_CHECK_OK(client2.Contains(object_id, &has_object));
          RAY_CHECK(has_object);

          // Sleep to make sure the plasma manager for client2 has create local queue
          // and subscribed to plasma manager for client1. otherwise if PushQueueItem()
          // is called before plasma manager subscription, wth current implmentation 
          // the item notification would not be pushed to client2 (will fix later).
          //sleep(5);

          RAY_LOG(INFO) << "PushQueueItem started " << object_id;  

          std::vector<uint64_t> items;
          items.resize(100 * 1000);
          for (uint32_t i = 0; i < items.size(); i++) {
            items[i] = i;
          }

          using namespace std::chrono;
          auto curr_time = std::chrono::system_clock::now();
          RAY_LOG(INFO) << "PushQueueItem started " << object_id;
          for (uint32_t i = 0; i < items.size(); i++) {
            uint8_t* data = reinterpret_cast<uint8_t*>(&items[i]);
            uint32_t data_size = static_cast<uint32_t>(sizeof(uint64_t));
            ARROW_CHECK_OK(client1.PushQueueItem(object_id, data, data_size));
          }
          duration<double> push_time = std::chrono::system_clock::now() - curr_time;
          RAY_LOG(INFO) << "PushQueueItem takes " << push_time.count() << " seconds";  

          curr_time = std::chrono::system_clock::now();
          RAY_LOG(INFO) << "GetQueueItem started " << object_id;  
          for (uint32_t i = 0; i < items.size(); i++) {
            uint8_t* buff = nullptr;
            uint32_t buff_size = 0;
            uint64_t seq_id = -1;

            ARROW_CHECK_OK(client2.GetQueueItem(object_id, buff, buff_size, seq_id));
            RAY_CHECK(static_cast<uint32_t>(seq_id) == i + 1);
            RAY_CHECK(buff_size == sizeof(uint64_t));
            uint64_t value = *(uint64_t*)(buff);
            RAY_CHECK(value == items[i]);
          }
          duration<double> get_time = std::chrono::system_clock::now() - curr_time;
          RAY_LOG(INFO) << "GetQueueItem takes " << get_time.count() << " seconds";            

          RAY_LOG(INFO) << "Plasma queue test done " << object_id;  
          TestComplete();
        });

      })); // SubscribeQueue

    })); // Wait
    
  }

  void TestComplete() { main_service.stop(); }

  void TestConnections() {
    RAY_LOG(DEBUG) << "\n"
                   << "Server client ids:"
                   << "\n";
    const ClientTableDataT &data = gcs_client_1->client_table().GetClient(client_id_1);
    RAY_LOG(DEBUG) << (ClientID::from_binary(data.client_id) == ClientID::nil());
    RAY_LOG(DEBUG) << "Server 1 ClientID=" << ClientID::from_binary(data.client_id);
    RAY_LOG(DEBUG) << "Server 1 ClientIp=" << data.node_manager_address;
    RAY_LOG(DEBUG) << "Server 1 ClientPort=" << data.node_manager_port;
    ASSERT_EQ(client_id_1, ClientID::from_binary(data.client_id));
    const ClientTableDataT &data2 = gcs_client_1->client_table().GetClient(client_id_2);
    RAY_LOG(DEBUG) << "Server 2 ClientID=" << ClientID::from_binary(data2.client_id);
    RAY_LOG(DEBUG) << "Server 2 ClientIp=" << data2.node_manager_address;
    RAY_LOG(DEBUG) << "Server 2 ClientPort=" << data2.node_manager_port;
    ASSERT_EQ(client_id_2, ClientID::from_binary(data2.client_id));
  }
};

TEST_F(TestObjectManager, StartTestObjectManager) {
  auto AsyncStartTests = main_service.wrap([this]() { WaitConnections(); });
  AsyncStartTests();
  main_service.run();
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  ray::store_executable = std::string(argv[1]);
  return RUN_ALL_TESTS();
}