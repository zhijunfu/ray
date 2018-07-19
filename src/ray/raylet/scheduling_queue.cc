#include "scheduling_queue.h"

#include "ray/status.h"

namespace ray {

namespace raylet {

bool SchedulingQueue::TaskQueue::Append(const TaskID &task_id, const Task &task) {
  if (map_.find(task_id) != map_.end()) {
    // This shouldn't happen.
    return false;
  }

  auto list_iter = list_.insert(list_.end(), task);
  map_[task_id] = list_iter;
  return true;
}

bool SchedulingQueue::TaskQueue::Remove(const TaskID &task_id) {
  auto iter = map_.find(task_id);
  if (iter == map_.end()) {
    // not exist.
    return false;
  }

  auto list_iter = iter->second;
  map_.erase(iter);
  list_.erase(list_iter);
  return true;
}

bool SchedulingQueue::TaskQueue::Remove(const TaskID &task_id, std::vector<Task> &removed_tasks) {
  auto iter = map_.find(task_id);
  if (iter == map_.end()) {
    // not exist.
    return false;
  }

  auto list_iter = iter->second;
  removed_tasks.push_back(std::move(*list_iter));
  map_.erase(iter);
  list_.erase(list_iter);
  return true;
}

const std::list<Task> &SchedulingQueue::TaskQueue::GetTasks() const {
  return list_;
}

const std::list<Task> &SchedulingQueue::GetUncreatedActorMethods() const {
  return this->uncreated_actor_methods_.GetTasks();
}

const std::list<Task> &SchedulingQueue::GetWaitingTasks() const {
  return this->waiting_tasks_.GetTasks();
}

const std::list<Task> &SchedulingQueue::GetPlaceableTasks() const {
  return this->placeable_tasks_.GetTasks();
}

const std::list<Task> &SchedulingQueue::GetReadyTasks() const {
  return this->ready_tasks_.GetTasks();
}

const std::list<Task> &SchedulingQueue::GetRunningTasks() const {
  return this->running_tasks_.GetTasks();
}

const std::list<Task> &SchedulingQueue::GetBlockedTasks() const {
  return this->blocked_tasks_.GetTasks();
}

// Helper function to remove tasks in the given set of task_ids from a
// queue, and append them to the given vector removed_tasks.
void removeTasksFromQueue(SchedulingQueue::TaskQueue &queue, std::unordered_set<TaskID> &task_ids,
                          std::vector<Task> &removed_tasks) {
  for (auto it = task_ids.begin(); it != task_ids.end();) {
    if (queue.Remove(*it, removed_tasks)) {
      it = task_ids.erase(it);
    } else {
      it++;
    }
  }
}

// Helper function to queue the given tasks to the given queue.
inline void queueTasks(SchedulingQueue::TaskQueue &queue, const std::vector<Task> &tasks) {
  for (auto &task : tasks) {
    queue.Append(task.GetTaskSpecification().TaskId(), task);
  }
}

std::vector<Task> SchedulingQueue::RemoveTasks(std::unordered_set<TaskID> task_ids) {
  // List of removed tasks to be returned.
  std::vector<Task> removed_tasks;

  // Try to find the tasks to remove from the waiting tasks.
  removeTasksFromQueue(uncreated_actor_methods_, task_ids, removed_tasks);
  removeTasksFromQueue(waiting_tasks_, task_ids, removed_tasks);
  removeTasksFromQueue(placeable_tasks_, task_ids, removed_tasks);
  removeTasksFromQueue(ready_tasks_, task_ids, removed_tasks);
  removeTasksFromQueue(running_tasks_, task_ids, removed_tasks);
  removeTasksFromQueue(blocked_tasks_, task_ids, removed_tasks);
  // TODO(swang): Remove from running methods.

  RAY_CHECK(task_ids.size() == 0);
  return removed_tasks;
}

void SchedulingQueue::MoveTasks(std::unordered_set<TaskID> task_ids, TaskState src_state,
                                TaskState dst_state) {
  // TODO(atumanov): check the states first to ensure the move is transactional.
  std::vector<Task> removed_tasks;
  // Remove the tasks from the specified source queue.
  switch (src_state) {
  case PLACEABLE:
    removeTasksFromQueue(placeable_tasks_, task_ids, removed_tasks);
    break;
  case WAITING:
    removeTasksFromQueue(waiting_tasks_, task_ids, removed_tasks);
    break;
  case READY:
    removeTasksFromQueue(ready_tasks_, task_ids, removed_tasks);
    break;
  case RUNNING:
    removeTasksFromQueue(running_tasks_, task_ids, removed_tasks);
    break;
  default:
    RAY_LOG(ERROR) << "Attempting to move tasks from unrecognized state " << src_state;
  }
  // Add the tasks to the specified destination queue.
  switch (dst_state) {
  case PLACEABLE:
    queueTasks(placeable_tasks_, removed_tasks);
    break;
  case WAITING:
    queueTasks(waiting_tasks_, removed_tasks);
    break;
  case READY:
    queueTasks(ready_tasks_, removed_tasks);
    break;
  case RUNNING:
    queueTasks(running_tasks_, removed_tasks);
    break;
  default:
    RAY_LOG(ERROR) << "Attempting to move tasks to unrecognized state " << dst_state;
  }
}

void SchedulingQueue::QueueUncreatedActorMethods(const std::vector<Task> &tasks) {
  queueTasks(uncreated_actor_methods_, tasks);
}

void SchedulingQueue::QueueWaitingTasks(const std::vector<Task> &tasks) {
  queueTasks(waiting_tasks_, tasks);
}

void SchedulingQueue::QueuePlaceableTasks(const std::vector<Task> &tasks) {
  queueTasks(placeable_tasks_, tasks);
}

void SchedulingQueue::QueueReadyTasks(const std::vector<Task> &tasks) {
  queueTasks(ready_tasks_, tasks);
}

void SchedulingQueue::QueueRunningTasks(const std::vector<Task> &tasks) {
  queueTasks(running_tasks_, tasks);
}

void SchedulingQueue::QueueBlockedTasks(const std::vector<Task> &tasks) {
  queueTasks(blocked_tasks_, tasks);
}

}  // namespace raylet

}  // namespace ray
