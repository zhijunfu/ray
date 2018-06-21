package org.ray.api.test;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ray.api.*;
import org.ray.util.logger.RayLog;

import java.io.FileWriter;
import java.io.IOException;

/**
 * Plasma queue test.
 */
@RunWith(MyRunner.class)
public class PlasmaQueueTest {

  @Test
  public void test() throws InterruptedException {
    RayActor<A> a = Ray.create(PlasmaQueueTest.A.class);
    RayLog.rapp.info("plasma queue test started......");

    Ray.call(A::f, a);
    Thread.sleep(10000);
  }

  @RayRemote
  public static class A {
    public A() {

      RayLog.rapp.info("Actor A started......");
    }

    public Integer f() throws InterruptedException {
      // Create a plasma queue
      UniqueID qid = Ray.createQueue(1024000);
      RayLog.rapp.info("Create plasma queueu successfully......");

      Thread.sleep(2000);

      RayActor<PlasmaQueueTest.B> b = Ray.create(PlasmaQueueTest.B.class);
      Ray.call(B::setQid, b, qid);

      Ray.call(B::f, b);
      Thread.sleep(2000);

      // Store objects to the plasma queue
      for (int i=0; i< 1000;i=i+1) {
        Ray.pushQueue(qid, i);

      }
      Ray.pushQueue(qid, 1);
      RayLog.rapp.info("put object: 1");
      Ray.pushQueue(qid, 2);
      RayLog.rapp.info("put object: 2");
      Ray.pushQueue(qid, 3);
      RayLog.rapp.info("put object: 3");
      Ray.pushQueue(qid, 4.1);
      RayLog.rapp.info("put object: 4.1");
      Ray.pushQueue(qid, 5.1);
      RayLog.rapp.info("put object: 5.1");
      Ray.pushQueue(qid, 6.1);
      RayLog.rapp.info("put object: 6.1");
      Ray.pushQueue(qid, 'a');
      RayLog.rapp.info("put object: a");
      Ray.pushQueue(qid, 'b');
      RayLog.rapp.info("put object: b");
      Ray.pushQueue(qid, 'c');
      RayLog.rapp.info("put object: c");
      Ray.pushQueue(qid, "d");
      RayLog.rapp.info("put object: d");
      Thread.sleep(5000);
      return 0;
    }
  }

  @RayRemote
  public static class B {
    public B() {
      RayLog.rapp.info("Actor B started......");
    }
    private UniqueID qid;

    public Integer setQid(UniqueID qid) {
      this.qid = qid;
      return 0;
    }

    public Integer f() {
      RayLog.rapp.info("B::f started...");
      for (int i = 0; i < 1010; i = i + 1) {
        // Get object from a plasma queue
        Object obj = Ray.readQueue(this.qid);
        RayLog.rapp.info("get obj: " + obj.toString());
      }
      return 0;
    }
  }
}
