package org.ray.example;

import java.io.Serializable;
import org.ray.api.Ray;
import org.ray.api.RayObject;
import org.ray.api.RayRemote;
import org.ray.core.RayRuntime;
import org.ray.api.*;
import org.ray.util.logger.RayLog;

import java.io.FileWriter;
import java.io.IOException;

import java.util.Date;
import java.text.*;

/**
 * Plasma queue test.
 */

public class HelloWorld {

  public static void main(String[] args) throws Exception {
        try {
          Ray.init(args);

          new Exception().printStackTrace();

          Integer numOfItems = 10; // 100 * 1000;
          Integer queueSize = 200 * 1000 * 1000;
/*
          RayLog.rapp.warn("plasma queue test started......");          
          RayActor<A> a = Ray.create(HelloWorld.A.class);      
          Ray.call(A::f, a, numOfItems, queueSize);
          Thread.sleep(30 * 1000);
*/

          RayLog.rapp.warn("plasma object test started......");   
          RayActor<AA> aa = Ray.create(HelloWorld.AA.class);
          Ray.call(AA::f, aa, numOfItems);
          Thread.sleep(100 * 1000);

        } catch (Throwable t) {
          t.printStackTrace();
        } finally {
          RayRuntime.getInstance().cleanUp();
        }
      }
  

  @RayRemote
  public static class AA {
    public AA() {
      new Exception().printStackTrace();

      RayLog.rapp.warn("Actor AAA started......");
    }

    public Integer f(Integer numOfItems) throws InterruptedException {
      new Exception().printStackTrace();

      RayActor<HelloWorld.BB> b = Ray.create(HelloWorld.BB.class);
      // Ray.call(BB::setQid, b, qid);
      Ray.call(BB::setNumOfItems, b, numOfItems);

      // Store objects to the plasma queue
      String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss.SSS").format(new Date());
      RayLog.rapp.warn("push_object start......" + timeStamp);
      for (int i=0; i< numOfItems;i=i+1) {
        Ray.call(BB::f, b, i);
        // Ray.put(i);
      }
      timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss.SSS").format(new Date());
      RayLog.rapp.warn("push_object end......" + timeStamp);
      Thread.sleep(100 * 1000);
      return 0;
    }
  }

  @RayRemote
  public static class BB {
    public BB() {
      new Exception().printStackTrace();

      this.count = 0;
      RayLog.rapp.warn("Actor BB started......");
    }
    private UniqueID qid;
    private Integer numOfItems;
    private int count;

    public Integer setQid(UniqueID qid) {
      this.qid = qid;
      return 0;
    }

    public Integer setNumOfItems(Integer numOfItems) {
      this.numOfItems = numOfItems;
      return 0;
    }

    public Integer f(Integer i) {
      new Exception().printStackTrace();
    /*
      if (i == 0) {
        String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss.SSS").format(new Date());
        RayLog.rapp.warn("get_object start......" + timeStamp);
      }
      if (i == this.numOfItems - 1) {
        String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss.SSS").format(new Date());
        RayLog.rapp.warn("get_object end......" + timeStamp);
      }
      
      if (i % 10000 == 0) {
        String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss.SSS").format(new Date());
        RayLog.rapp.warn("get_object " + i + " ......" + timeStamp); 
      } 
      */
      
      if (this.count == 0) {
        String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss.SSS").format(new Date());
        RayLog.rapp.warn("get_object start......" + timeStamp);
      }
      if (this.count == this.numOfItems - 1) {
        String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss.SSS").format(new Date());
        RayLog.rapp.warn("get_object end......" + timeStamp);
      }
      
      if (this.count % 10000 == 0) {
        String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss.SSS").format(new Date());
        RayLog.rapp.warn("get_object " + i + " ......" + timeStamp); 
      }
      
      this.count++;

      return 0;
    }
  }
}
