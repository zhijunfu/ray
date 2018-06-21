package org.ray.core;

import org.ray.api.UniqueID;
import org.ray.core.model.RayParameters;
import org.ray.spi.model.TaskSpec;

import java.util.HashMap;
import java.util.Map;

public class WorkerContext {

  private static final ThreadLocal<WorkerContext> currentWorkerCtx =
      ThreadLocal.withInitial(() -> init(RayRuntime.getParams()));
  /**
   * id of worker.
   */
  public static UniqueID workerID = UniqueID.randomId();
  /**
   * current doing task.
   */
  private TaskSpec currentTask;
  /**
   * current app classloader.
   */
  private ClassLoader currentClassLoader;
  /**
   * how many puts done by current task.
   */
  private int currentTaskPutCount;
  /**
   * how many calls done by current task.
   */
  private int currentTaskCallCount;
  /**
   * a dictionary mapping the ObjectId of a plasma queue to the current
   * reading position of the queue.
   */
  private Map<UniqueID, Long> plasmaQueueReadingIndex;

  public static WorkerContext init(RayParameters params) {
    WorkerContext ctx = new WorkerContext();
    currentWorkerCtx.set(ctx);

    TaskSpec dummy = new TaskSpec();
    dummy.parentTaskId = UniqueID.nil;
    dummy.taskId = UniqueID.nil;
    dummy.actorId = UniqueID.nil;
    dummy.driverId = params.driver_id;
    prepare(dummy, null);

    return ctx;
  }

  public static void prepare(TaskSpec task, ClassLoader classLoader) {
    WorkerContext wc = get();
    wc.currentTask = task;
    wc.currentTaskPutCount = 0;
    wc.currentTaskCallCount = 0;
    wc.currentClassLoader = classLoader;
    wc.plasmaQueueReadingIndex = new HashMap<>();
  }

  public static WorkerContext get() {
    return currentWorkerCtx.get();
  }

  public static TaskSpec currentTask() {
    return get().currentTask;
  }

  public static int nextPutIndex() {
    return ++get().currentTaskPutCount;
  }

  public static int nextCallIndex() {
    return ++get().currentTaskCallCount;
  }

  public static UniqueID currentWorkerId() {
    return WorkerContext.workerID;
  }

  public static ClassLoader currentClassLoader() {
    return get().currentClassLoader;
  }

  public static boolean isSubscribedPlasmaQueue(UniqueID qid) {
    Boolean flag = get().plasmaQueueReadingIndex.containsKey(qid);
    if (flag == false) { get().plasmaQueueReadingIndex.put(qid, 0L); }
    return flag;
  }

  public static Long plasmaQueueReadingIndex(UniqueID qid) {
    Long index = get().plasmaQueueReadingIndex.get(qid);
    get().plasmaQueueReadingIndex.put(qid, index+1);
    return index;
  }
}
