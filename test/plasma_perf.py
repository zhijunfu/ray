# Simple test for plasma queue usage. Actor A produces datas and Actor B
# consumes them. We also add a normal Ray implementation for comparition.

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
import time
import datetime

## Provider A and Receiver B not using plasma queue
#@ray.remote
#class A(object):
#    def __init__(self):
#        self.b = B.remote()
#
#    def f(self):
#        for i in range(10):
#            self.b.add.remote(i)
#        return ray.get(self.b.get_sum.remote())
#
#@ray.remote
#class B(object):
#    def __init__(self):
#        self.sum = 0
#
#    def add(self, val):
#        self.sum += val
#
#    def get_sum(self):
#        return self.sum


# Provider A and Receiver B using plasma queue
@ray.remote
class A(object):
    def __init__(self):
        print("Actor A start...")

    def f(self):
        num_of_items = 100 * 1000

        b = B.remote(num_of_items)
        time_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')
        print("push_queue, start " + time_str)
        for i in range(num_of_items):
            b.f.remote(i)
        time_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')
        print("push_queue, end " + time_str)
        time.sleep(5)


@ray.remote
class B(object):
    def __init__(self, num_of_items):
        print("Actor B start...")
        self.num_of_items = num_of_items
        self.sum = 0

    def f(self, object_id):
        #ray.get(object_id)
        #print (object_id)
        self.sum += 1
        if (self.sum == 1) :
            time_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')
            print("read, start " + time_str)

        if (self.sum == self.num_of_items) :
            time_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')
            print("read, end " + time_str)

    def get_sum(self):
        return self.sum


if __name__ == "__main__":
    ray.init()
    A.remote().f.remote()
    time.sleep(200)
