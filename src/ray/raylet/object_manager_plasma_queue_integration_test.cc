#include <iostream>
#include <thread>

#include "gtest/gtest.h"

#include "ray/raylet/raylet.h"

namespace ray {

namespace raylet {

std::string test_executable;
std::string store_executable;

// TODO(hme): Get this working once the dust settles.
class TestObjectManagerBase : public ::testing::Test {
 public:
  TestObjectManagerBase() : work(test_service)  { RAY_LOG(INFO) << "TestObjectManagerBase: started."; }

  std::string StartStore(const std::string &id) {
    std::string store_id = "/tmp/store";
    store_id = store_id + id;
    std::string plasma_command = store_executable + " -m 1000000000 -s " + store_id +
                                 " 1> /dev/null 2> /dev/null &";
    RAY_LOG(INFO) << plasma_command;
    int ec = system(plasma_command.c_str());
    RAY_CHECK(ec == 0);
    return store_id;
  }

  NodeManagerConfig GetNodeManagerConfig(std::string raylet_socket_name,
                                         std::string store_socket_name) {
    // Configuration for the node manager.
    ray::raylet::NodeManagerConfig node_manager_config;
    std::unordered_map<std::string, double> static_resource_conf;
    static_resource_conf = {{"CPU", 1}, {"GPU", 1}};
    node_manager_config.resource_config =
        ray::raylet::ResourceSet(std::move(static_resource_conf));
    node_manager_config.num_initial_workers = 0;
    node_manager_config.num_workers_per_process = 1;
    // Use a default worker that can execute empty tasks with dependencies.
    node_manager_config.worker_command.push_back("python");
    node_manager_config.worker_command.push_back(
        "../python/ray/workers/default_worker.py");
    node_manager_config.worker_command.push_back(raylet_socket_name.c_str());
    node_manager_config.worker_command.push_back(store_socket_name.c_str());
    return node_manager_config;
  };

  void SetUp() {
    // start store
    std::string store_sock_1 = StartStore("1");
    std::string store_sock_2 = StartStore("2");

    // start first server
    gcs_client_1 = std::shared_ptr<gcs::AsyncGcsClient>(new gcs::AsyncGcsClient());
    ObjectManagerConfig om_config_1;
    om_config_1.store_socket_name = store_sock_1;
    om_config_1.push_timeout_ms = 10000;
    om_config_1.pull_timeout_ms = 20;
    om_config_1.max_sends = 2;
    om_config_1.max_receives = 2;
    om_config_1.object_chunk_size = 100000000;

    raylet_sock1 = "/tmp/raylet_1";
    server1.reset(new ray::raylet::Raylet(
        main_service, raylet_sock1, "127.0.0.1", "127.0.0.1", 6379,
        GetNodeManagerConfig(raylet_sock1, store_sock_1), om_config_1, gcs_client_1));

    // start second server
    gcs_client_2 = std::shared_ptr<gcs::AsyncGcsClient>(new gcs::AsyncGcsClient());
    ObjectManagerConfig om_config_2;
    om_config_2.store_socket_name = store_sock_2;
    om_config_2.push_timeout_ms = 10000;
    om_config_2.pull_timeout_ms = 20;
    om_config_2.max_sends = 2;
    om_config_2.max_receives = 2;
    om_config_2.object_chunk_size = 100000000;

    raylet_sock2 = "/tmp/raylet_2";
    server2.reset(new ray::raylet::Raylet(
        main_service, raylet_sock2, "127.0.0.1", "127.0.0.1", 6379,
        GetNodeManagerConfig(raylet_sock2, store_sock_2), om_config_2, gcs_client_2));

    test_thread = std::thread(&TestObjectManagerBase::StartTestService, this);

    // connect to stores.
    ARROW_CHECK_OK(client1.Connect(store_sock_1, "", plasma::kPlasmaDefaultReleaseDelay));
    ARROW_CHECK_OK(client2.Connect(store_sock_2, "", plasma::kPlasmaDefaultReleaseDelay));
  }

  void TearDown() {
    arrow::Status client1_status = client1.Disconnect();
    arrow::Status client2_status = client2.Disconnect();
    ASSERT_TRUE(client1_status.ok() && client2_status.ok());

    this->server1.reset();
    this->server2.reset();

    test_service.stop();
    test_thread.join();    

    int s = system("killall plasma_store");
    //ASSERT_TRUE(!s);
    s = system("rm /tmp/raylet_1");
    //ASSERT_TRUE(!s);
    s = system("rm /tmp/raylet_2");
    //ASSERT_TRUE(!s);
    RAY_LOG(DEBUG) << s;
  }

   void StartTestService() { test_service.run(); }

  ObjectID WriteDataToClient(plasma::PlasmaClient &client, int64_t data_size) {
    ObjectID object_id = ObjectID::from_random();
    RAY_LOG(DEBUG) << "ObjectID Created: " << object_id;
    uint8_t metadata[] = {5};
    int64_t metadata_size = sizeof(metadata);
    std::shared_ptr<Buffer> data;
    ARROW_CHECK_OK(client.Create(object_id.to_plasma_id(), data_size, metadata,
                                 metadata_size, &data));
    ARROW_CHECK_OK(client.Seal(object_id.to_plasma_id()));
    return object_id;
  }

 protected:
  std::thread p;
  boost::asio::io_service main_service;
  boost::asio::io_service test_service;
  boost::asio::io_service::work work;
  std::thread test_thread;
  std::shared_ptr<gcs::AsyncGcsClient> gcs_client_1;
  std::shared_ptr<gcs::AsyncGcsClient> gcs_client_2;
  std::unique_ptr<ray::raylet::Raylet> server1;
  std::unique_ptr<ray::raylet::Raylet> server2;
  std::string raylet_sock1;
  std::string raylet_sock2;

  plasma::PlasmaClient client1;
  plasma::PlasmaClient client2;
  std::vector<ObjectID> v1;
  std::vector<ObjectID> v2;
};

class TestObjectManagerIntegration : public TestObjectManagerBase {
 public:
  uint num_expected_objects;

  int num_connected_clients_client1 = 0;
  int num_connected_clients_client2 = 0;

  ClientID client_id_1;
  ClientID client_id_2;

  void WaitConnections() {
    client_id_1 = gcs_client_1->client_table().GetLocalClientId();
    client_id_2 = gcs_client_2->client_table().GetLocalClientId();
    gcs_client_1->client_table().RegisterClientAddedCallback([this](
        gcs::AsyncGcsClient *client, const ClientID &id, const ClientTableDataT &data) {
      ClientID parsed_id = ClientID::from_binary(data.client_id);
      if (parsed_id == client_id_1 || parsed_id == client_id_2) {
        num_connected_clients_client1 += 1;
      }
      if (num_connected_clients_client1 == 2) {
        StartTestsIfSatisfied();
      }
    });
    gcs_client_2->client_table().RegisterClientAddedCallback([this](
        gcs::AsyncGcsClient *client, const ClientID &id, const ClientTableDataT &data) {
      ClientID parsed_id = ClientID::from_binary(data.client_id);
      if (parsed_id == client_id_1 || parsed_id == client_id_2) {
        num_connected_clients_client2 += 1;
      }
      if (num_connected_clients_client2 == 2) {
        StartTestsIfSatisfied();
      }
    }); 
  }

  void StartTestsIfSatisfied() {
    if (num_connected_clients_client1 == 2 && num_connected_clients_client2 == 2) {
      StartTests();
    }
  }
  
  void StartTests() {
    TestConnections();
    TestPlasmaQueue();
  }


  void TestPlasmaQueue() {

    RAY_LOG(INFO) << "Plasma Queue Test... ";   
    plasma::ObjectID object_id = plasma::ObjectID::from_random();
    std::vector<plasma::ObjectBuffer> object_buffers;

    RAY_LOG(INFO) << "Create queue object " << object_id;  
    int64_t queue_size = 100 * 1024 * 1024;
    std::shared_ptr<Buffer> data;
    ARROW_CHECK_OK(client1.CreateQueue(object_id, queue_size, &data));

    RAY_LOG(INFO) << "Connect to raylet2 socket";  
    int raylet2_conn = -1;
    auto status = ConnectIpcSocketRetry(raylet_sock2, -1, -1, &raylet2_conn);
    RAY_CHECK(status.ok());
    RAY_CHECK(raylet2_conn > 0);

    // Start plasma queue test.
    test_service.post([this, object_id, raylet2_conn]() {

      // Note that Wait & SubscribeQueue below need to run in separate thread,
      // as they involve receiving messages which would cause deadlock if running
      // in the same thread with ObjectManager. Plasma queue test also involves 
      // receiving messages (GetQueueItem) thus cannot run within the same thread
      // as ObjectManager.
      RAY_LOG(INFO) << "Wait for object id to appear in GCS";
      auto wait_return = RayletWait(raylet2_conn, {object_id}, 1, -1, false);
      
      RAY_CHECK(wait_return.first.size() == 1 && wait_return.second.empty());
      RAY_CHECK(wait_return.first[0] == object_id); 

      RAY_LOG(INFO) << "Subscribe queue update";
      bool success = RayletSubscribeQueue(raylet2_conn, object_id);
      RAY_CHECK(success);

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
      RAY_LOG(INFO) << "PushQueueItem takes " << push_time.count()  << " seconds";  

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
  
  }
  
  // Copied from local_scheduler_client.c:local_scheuler_wait().
  std::pair<std::vector<ObjectID>, std::vector<ObjectID>> RayletWait(
      int conn,
      const std::vector<ObjectID> &object_ids,
      int num_returns,
      int64_t timeout_milliseconds,
      bool wait_local) {
    // Write request.
    flatbuffers::FlatBufferBuilder fbb;
    auto message = ray::protocol::CreateWaitRequest(
        fbb, to_flatbuf(fbb, object_ids), num_returns, timeout_milliseconds,
        wait_local);
    fbb.Finish(message);
    write_message(conn,
                  static_cast<int64_t>(ray::protocol::MessageType::WaitRequest),
                  fbb.GetSize(), fbb.GetBufferPointer());
    // Read result.
    int64_t type;
    int64_t reply_size;
    uint8_t *reply;
    read_message(conn, &type, &reply_size, &reply);
    RAY_CHECK(static_cast<ray::protocol::MessageType>(type) ==
              ray::protocol::MessageType::WaitReply);
    auto reply_message = flatbuffers::GetRoot<ray::protocol::WaitReply>(reply);
    // Convert result.
    std::pair<std::vector<ObjectID>, std::vector<ObjectID>> result;
    auto found = reply_message->found();
    for (uint i = 0; i < found->size(); i++) {
      ObjectID object_id = ObjectID::from_binary(found->Get(i)->str());
      result.first.push_back(object_id);
    }
    auto remaining = reply_message->remaining();
    for (uint i = 0; i < remaining->size(); i++) {
      ObjectID object_id = ObjectID::from_binary(remaining->Get(i)->str());
      result.second.push_back(object_id);
    }
    /* Free the original message from the local scheduler. */
    free(reply);
    return result;
  }

  bool RayletSubscribeQueue(
      int conn,
      ObjectID object_id) {
    // Write request.
    flatbuffers::FlatBufferBuilder fbb;
    auto message = ray::protocol::CreateSubscribeQueueRequest(
        fbb, to_flatbuf(fbb, object_id));
    fbb.Finish(message);
    write_message(conn,
                  static_cast<int64_t>(ray::protocol::MessageType::SubscribeQueueRequest),
                  fbb.GetSize(), fbb.GetBufferPointer());
    // Read result.
    int64_t type;
    int64_t reply_size;
    uint8_t *reply;
    read_message(conn, &type, &reply_size, &reply);
    RAY_CHECK(static_cast<ray::protocol::MessageType>(type) ==
              ray::protocol::MessageType::SubscribeQueueReply);
    auto reply_message = flatbuffers::GetRoot<ray::protocol::SubscribeQueueReply>(reply);
    // Convert result.
    bool success = reply_message->success();
    /* Free the original message from the local scheduler. */
    free(reply);
    return success;
  }
  

  int write_bytes(int fd, uint8_t *cursor, size_t length) {
    ssize_t nbytes = 0;
    size_t bytesleft = length;
    size_t offset = 0;
    while (bytesleft > 0) {
      /* While we haven't written the whole message, write to the file
      * descriptor, advance the cursor, and decrease the amount left to write. */
      nbytes = write(fd, cursor + offset, bytesleft);
      if (nbytes < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
          continue;
        }
        return -1; /* Errno will be set. */
      } else if (0 == nbytes) {
        /* Encountered early EOF. */
        return -1;
      }
      RAY_CHECK(nbytes > 0);
      bytesleft -= nbytes;
      offset += nbytes;
    }

    return 0;
  }

  int write_message(int fd, int64_t type, int64_t length, uint8_t *bytes) {
    int64_t version = RayConfig::instance().ray_protocol_version();
    int closed;
    closed = write_bytes(fd, (uint8_t *) &version, sizeof(version));
    if (closed) {
      return closed;
    }
    closed = write_bytes(fd, (uint8_t *) &type, sizeof(type));
    if (closed) {
      return closed;
    }
    closed = write_bytes(fd, (uint8_t *) &length, sizeof(length));
    if (closed) {
      return closed;
    }
    closed = write_bytes(fd, bytes, length * sizeof(char));
    if (closed) {
      return closed;
    }
    return 0;
  }

  int read_bytes(int fd, uint8_t *cursor, size_t length) {
    ssize_t nbytes = 0;
    /* Termination condition: EOF or read 'length' bytes total. */
    size_t bytesleft = length;
    size_t offset = 0;
    while (bytesleft > 0) {
      nbytes = read(fd, cursor + offset, bytesleft);
      if (nbytes < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
          continue;
        }
        return -1; /* Errno will be set. */
      } else if (0 == nbytes) {
        /* Encountered early EOF. */
        return -1;
      }
      RAY_CHECK(nbytes > 0);
      bytesleft -= nbytes;
      offset += nbytes;
    }

    return 0;
  }

  void read_message(int fd, int64_t *type, int64_t *length, uint8_t **bytes) {
    int64_t version;
    int closed = read_bytes(fd, (uint8_t *) &version, sizeof(version));
    if (closed) {
      goto disconnected;
    }
    RAY_CHECK(version == RayConfig::instance().ray_protocol_version());
    closed = read_bytes(fd, (uint8_t *) type, sizeof(*type));
    if (closed) {
      goto disconnected;
    }
    closed = read_bytes(fd, (uint8_t *) length, sizeof(*length));
    if (closed) {
      goto disconnected;
    }
    *bytes = (uint8_t *) malloc(*length * sizeof(uint8_t));
    closed = read_bytes(fd, *bytes, *length);
    if (closed) {
      free(*bytes);
      goto disconnected;
    }
    return;

  disconnected:
    /* Handle the case in which the socket is closed. */
    *type = static_cast<int64_t>(protocol::MessageType::DisconnectClient);
    *length = 0;
    *bytes = NULL;
    return;
  }

  Status ConnectIpcSocketRetry(const std::string& pathname, int num_retries,
                              int64_t timeout, int* fd) {
    // Pick the default values if the user did not specify.
    if (num_retries < 0) {
      num_retries = 50;
    }
    if (timeout < 0) {
      timeout = 100;
    }
    *fd = connect_ipc_sock(pathname);
    while (*fd < 0 && num_retries > 0) {
      ARROW_LOG(ERROR) << "Connection to IPC socket failed for pathname " << pathname
                      << ", retrying " << num_retries << " more times";
      // Sleep for timeout milliseconds.
      usleep(static_cast<int>(timeout * 1000));
      *fd = connect_ipc_sock(pathname);
      --num_retries;
    }
    // If we could not connect to the socket, exit.
    if (*fd == -1) {
      std::stringstream ss;
      ss << "Could not connect to socket " << pathname;
      return Status::IOError(ss.str());
    }
    return Status::OK();
  }

  int connect_ipc_sock(const std::string& pathname) {
    struct sockaddr_un socket_address;
    int socket_fd;

    socket_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (socket_fd < 0) {
      ARROW_LOG(ERROR) << "socket() failed for pathname " << pathname;
      return -1;
    }

    memset(&socket_address, 0, sizeof(socket_address));
    socket_address.sun_family = AF_UNIX;
    if (pathname.size() + 1 > sizeof(socket_address.sun_path)) {
      ARROW_LOG(ERROR) << "Socket pathname is too long.";
      return -1;
    }
    strncpy(socket_address.sun_path, pathname.c_str(), pathname.size() + 1);

    if (connect(socket_fd, reinterpret_cast<struct sockaddr*>(&socket_address),
                sizeof(socket_address)) != 0) {
      close(socket_fd);
      return -1;
    }

    return socket_fd;
  }

  void TestConnections() {
    RAY_LOG(INFO) << "\n"
                  << "Server client ids:"
                  << "\n";
    ClientID client_id_1 = gcs_client_1->client_table().GetLocalClientId();
    ClientID client_id_2 = gcs_client_2->client_table().GetLocalClientId();
    RAY_LOG(INFO) << "Server 1: " << client_id_1;
    RAY_LOG(INFO) << "Server 2: " << client_id_2;

    RAY_LOG(INFO) << "\n"
                  << "All connected clients:"
                  << "\n";
    const ClientTableDataT &data = gcs_client_2->client_table().GetClient(client_id_1);
    RAY_LOG(INFO) << (ClientID::from_binary(data.client_id) == ClientID::nil());
    RAY_LOG(INFO) << "ClientID=" << ClientID::from_binary(data.client_id);
    RAY_LOG(INFO) << "ClientIp=" << data.node_manager_address;
    RAY_LOG(INFO) << "ClientPort=" << data.node_manager_port;
    const ClientTableDataT &data2 = gcs_client_1->client_table().GetClient(client_id_2);
    RAY_LOG(INFO) << "ClientID=" << ClientID::from_binary(data2.client_id);
    RAY_LOG(INFO) << "ClientIp=" << data2.node_manager_address;
    RAY_LOG(INFO) << "ClientPort=" << data2.node_manager_port;
  }

  void TestComplete() { main_service.stop(); }
};

TEST_F(TestObjectManagerIntegration, StartTestObjectManagerPush) {
  auto AsyncStartTests = main_service.wrap([this]() { WaitConnections(); });
  AsyncStartTests();
  main_service.run();
}

}  // namespace raylet

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  ray::raylet::test_executable = std::string(argv[0]);
  ray::raylet::store_executable = std::string(argv[1]);
  return RUN_ALL_TESTS();
}
