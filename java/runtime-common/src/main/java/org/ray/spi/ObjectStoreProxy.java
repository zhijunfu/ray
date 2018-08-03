package org.ray.spi;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.plasma.ObjectStoreLink;
import org.apache.commons.lang3.tuple.Pair;
import org.ray.api.RayList;
import org.ray.api.RayObject;
import org.ray.api.UniqueID;
import org.ray.api.WaitResult;
import org.ray.core.Serializer;
import org.ray.core.WorkerContext;
import org.ray.util.exception.TaskExecutionException;
import org.ray.util.logger.RayLog;

/**
 * Object store proxy, which handles serialization and deserialization, and utilize a {@code
 * org.ray.spi.ObjectStoreLink} to actually store data.
 */
public class ObjectStoreProxy {

  private final ObjectStoreLink store;
  private final int getTimeoutMs = 1000;

  public ObjectStoreProxy(ObjectStoreLink store) {
    this.store = store;
  }

  public <T> Pair<T, GetStatus> get(UniqueID objectId, boolean isMetadata)
      throws TaskExecutionException {
    return get(objectId, getTimeoutMs, isMetadata);
  }

  public <T> Pair<T, GetStatus> get(UniqueID id, int timeoutMs, boolean isMetadata)
      throws TaskExecutionException {
    byte[] obj = store.get(id.getBytes(), timeoutMs, isMetadata);
    if (obj != null) {
      T t = Serializer.decode(obj, WorkerContext.currentClassLoader());
      store.release(id.getBytes());
      if (t instanceof TaskExecutionException) {
        throw (TaskExecutionException) t;
      }
      return Pair.of(t, GetStatus.SUCCESS);
    } else {
      return Pair.of(null, GetStatus.FAILED);
    }
  }

  public <T> List<Pair<T, GetStatus>> get(List<UniqueID> objectIds, boolean isMetadata)
      throws TaskExecutionException {
    return get(objectIds, getTimeoutMs, isMetadata);
  }

  public <T> List<Pair<T, GetStatus>> get(List<UniqueID> ids, int timeoutMs, boolean isMetadata)
      throws TaskExecutionException {
    List<byte[]> objs = store.get(getIdBytes(ids), timeoutMs, isMetadata);
    List<Pair<T, GetStatus>> ret = new ArrayList<>();
    for (int i = 0; i < objs.size(); i++) {
      byte[] obj = objs.get(i);
      if (obj != null) {
        T t = Serializer.decode(obj, WorkerContext.currentClassLoader());
        store.release(ids.get(i).getBytes());
        if (t instanceof TaskExecutionException) {
          throw (TaskExecutionException) t;
        }
        ret.add(Pair.of(t, GetStatus.SUCCESS));
      } else {
        ret.add(Pair.of(null, GetStatus.FAILED));
      }
    }
    return ret;
  }

  private static byte[][] getIdBytes(List<UniqueID> objectIds) {
    int size = objectIds.size();
    byte[][] ids = new byte[size][];
    for (int i = 0; i < size; i++) {
      ids[i] = objectIds.get(i).getBytes();
    }
    return ids;
  }

  public void put(UniqueID id, Object obj, Object metadata) {
    byte[] obj_data = Serializer.encode(obj);
    byte[] meta_data = Serializer.encode(metadata);
    RayLog.core.info("put " + id.toString() + " object size: "+ obj_data.length + " metadata size: " + meta_data.length);
    store.put(id.getBytes(), obj_data, meta_data);
    RayLog.core.info("put done " + id.toString());
  }

  public void createQueue(UniqueID id, int totalBytes) {
    store.createQueue(id.getBytes(), totalBytes);
  }

  public void pushQueue(UniqueID qid, Object obj) {
    store.pushQueue(qid.getBytes(), Serializer.encode(obj));
  }

  public void getQueue(UniqueID qid) {
    getQueue(qid, getTimeoutMs);
  }

  public void getQueue(UniqueID qid, int timeoutMs) {
    if (localSchedulerLink != null) {
      // raylet case.
      RayObject obj = new RayObject(qid);
      RayList<RayObject> waitObjs = new RayList<>();
      waitObjs.add(obj);
      
      WaitResult<RayObject> result = wait(waitObjs, 1, timeoutMs);
      if (result.getReadyOnes().size() != 1 || result.getRemainOnes().size() != 0) {
        throw new RuntimeException("wait(0 should return only one object");
      }

      if (!localSchedulerLink.subscribeQueue(qid)) {
        throw new RuntimeException("subscribeQueue failed");
      }
    }

    store.getQueue(qid.getBytes(), timeoutMs);
  }

  public <T> T readQueue(UniqueID qid, long index) throws TaskExecutionException {
    return readQueue(qid, index, getTimeoutMs);
  }

  public <T> T readQueue(UniqueID qid, long index, int timeoutMs)
      throws TaskExecutionException {
    byte[] obj = store.readQueue(qid.getBytes(), index, timeoutMs);

    if (obj != null) {
      T t = Serializer.decode(obj, WorkerContext.currentClassLoader());
      //store.release(qid.getBytes());
      if (t instanceof TaskExecutionException) {
        throw (TaskExecutionException) t;
      }
      return t;
    } else {
      return null;
    }
  }

  public <T> WaitResult<T> wait(RayList<T> waitfor, int numReturns, int timeout) {
    List<UniqueID> ids = new ArrayList<>();
    for (RayObject<T> obj : waitfor.Objects()) {
      ids.add(obj.getId());
    }
    List<byte[]> readys = store.wait(getIdBytes(ids), timeout, numReturns);

    RayList<T> readyObjs = new RayList<>();
    RayList<T> remainObjs = new RayList<>();
    for (RayObject<T> obj : waitfor.Objects()) {
      if (readys.contains(obj.getId().getBytes())) {
        readyObjs.add(obj);
      } else {
        remainObjs.add(obj);
      }
    }

    return new WaitResult<>(readyObjs, remainObjs);
  }

  public void fetch(UniqueID objectId) {
    store.fetch(objectId.getBytes());
  }

  public void fetch(List<UniqueID> objectIds) {
    store.fetch(getIdBytes(objectIds));
  }

  public int getFetchSize() {
    return 10000;
  }


  public enum GetStatus {
    SUCCESS, FAILED
  }
}
