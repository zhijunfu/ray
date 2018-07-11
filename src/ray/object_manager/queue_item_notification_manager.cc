#include <future>
#include <iostream>

#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/function.hpp>

#include "common/common.h"
#include "common/common_protocol.h"

#include "ray/object_manager/queue_item_notification_manager.h"

namespace ray {

QueueItemNotificationManager::QueueItemNotificationManager(
    boost::asio::io_service &io_service, const std::string &store_socket_name)
    : store_client_(), socket_(io_service) {
  ARROW_CHECK_OK(store_client_.Connect(store_socket_name.c_str(), "",
                                       plasma::kPlasmaDefaultReleaseDelay));

  ARROW_CHECK_OK(store_client_.SubscribeQueue(&c_socket_));
  boost::system::error_code ec;
  socket_.assign(boost::asio::local::stream_protocol(), c_socket_, ec);
  assert(!ec.value());
  NotificationWait();
}

QueueItemNotificationManager::~QueueItemNotificationManager() {
  ARROW_CHECK_OK(store_client_.Disconnect());
}

void QueueItemNotificationManager::NotificationWait() {
  boost::asio::async_read(socket_, boost::asio::buffer(&length_, sizeof(length_)),
                          boost::bind(&QueueItemNotificationManager::ProcessQueueItemLength,
                                      this, boost::asio::placeholders::error));
}

void QueueItemNotificationManager::ProcessQueueItemLength(
    const boost::system::error_code &error) {
  notification_.resize(length_);
  boost::asio::async_read(
      socket_, boost::asio::buffer(notification_),
      boost::bind(&QueueItemNotificationManager::ProcessQueueItemNotification, this,
                  boost::asio::placeholders::error));
}

void QueueItemNotificationManager::ProcessQueueItemNotification(
    const boost::system::error_code &error) {
  if (error) {
    RAY_LOG(FATAL) << error.message();
  }

  const auto &item_info = flatbuffers::GetRoot<PlasmaQueueItemInfo>(notification_.data());
  // NOTE: right now we don't support notification of queue item deletion yet.

  PlasmaQueueItemInfoT result;
  item_info->UnPackTo(&result);
  ProcessQueueItemAdd(result);
  
  NotificationWait();
}

void QueueItemNotificationManager::ProcessQueueItemAdd(const PlasmaQueueItemInfoT &item_info) {
  const auto &object_id = ObjectID::from_binary(item_info.object_id);
  for (auto &handler : add_handlers_[object_id]) {
    handler(item_info);
  }
}

void QueueItemNotificationManager::ProcessQueueItemRemove(const ObjectID &object_id, uint64_t seq_id) {
  for (auto &handler : rem_handlers_[object_id]) {
    handler(object_id, seq_id);
  }
}

void QueueItemNotificationManager::SubscribeQueueItemAdded(
    const ray::ObjectID & object_id,
    std::function<void(const PlasmaQueueItemInfoT &)> callback) {
  add_handlers_[object_id].push_back(std::move(callback));
}

void QueueItemNotificationManager::SubscribeQueueItemDeleted(
    const ray::ObjectID & object_id,
    std::function<void(const ObjectID &, uint64_t)> callback) {
  rem_handlers_[object_id].push_back(std::move(callback));
}

}  // namespace ray
