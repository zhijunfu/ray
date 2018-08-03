#ifndef RAY_OBJECT_MANAGER_QUEUE_NOTIFICATION_MANAGER_H
#define RAY_OBJECT_MANAGER_QUEUE_NOTIFICATION_MANAGER_H

#include <list>
#include <memory>
#include <vector>

#include <boost/asio.hpp>
#include <boost/asio/error.hpp>
#include <boost/bind.hpp>

#include "plasma/client.h"
#include "plasma/events.h"

#include "ray/id.h"
#include "ray/status.h"

#include "ray/object_manager/object_directory.h"

namespace ray {

/// \class ObjectStoreClientPool
///
/// Encapsulates notification handling from the object store.
class QueueItemNotificationManager {
 public:
  /// Constructor.
  ///
  /// \param io_service The asio service to be used.
  /// \param store_socket_name The store socket to connect to.
  QueueItemNotificationManager(boost::asio::io_service &io_service,
                                 const std::string &store_socket_name);

  ~QueueItemNotificationManager();

  /// Subscribe to notifications of objects added to local store.
  /// Upon subscribing, the callback will be invoked for all objects that
  /// already exist in the local store
  ///
  /// \param callback A callback expecting an ObjectID.
  void SubscribeQueueItemAdded(const ray::ObjectID &, std::function<void(const PlasmaQueueItemInfoT &)> callback);

  /// Subscribe to notifications of objects deleted from local store.
  ///
  /// \param callback A callback expecting an ObjectID.
  void SubscribeQueueItemDeleted(const ray::ObjectID &, std::function<void(const ray::ObjectID &, uint64_t)> callback);

 private:
  /// Async loop for handling object store notifications.
  void NotificationWait();
  void ProcessQueueItemLength(const boost::system::error_code &error);
  void ProcessQueueItemNotification(const boost::system::error_code &error);

  /// Support for rebroadcasting object add/rem events.
  void ProcessQueueItemAdd(const PlasmaQueueItemInfoT &object_info);
  void ProcessQueueItemRemove(const ObjectID &object_id, uint64_t);

  using AddHandler = std::function<void(const PlasmaQueueItemInfoT &)>;
  using RemoveHandler = std::function<void(const ray::ObjectID &, uint64_t)>;

  std::unordered_map<ObjectID, std::vector<AddHandler>> add_handlers_;
  std::unordered_map<ObjectID, std::vector<RemoveHandler>> rem_handlers_;

  plasma::PlasmaClient store_client_;
  int c_socket_;
  int64_t length_;
  std::vector<uint8_t> notification_;
  boost::asio::local::stream_protocol::socket socket_;
};

}  // namespace ray

#endif  // RAY_OBJECT_MANAGER_OBJECT_STORE_CLIENT_H
