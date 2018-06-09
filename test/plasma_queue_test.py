# Simple test for plasma queue usage. Actor A produces datas and Actor B 
# consumes them. We also add a normal Ray implementation for comparition.

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
import time

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
        qid = ray.create_queue()
        print("create_queue success, qid: " + str(qid))
        time.sleep(5)
        b = B.remote(qid)
        time.sleep(5)
        b.f.remote()
        time.sleep(5)
        for i in range(10):
            ray.push_queue(qid, i)
            print("push_queue, val: " + str(i))
        time.sleep(5)

@ray.remote
class B(object):
    def __init__(self, qid):
        print("Actor B start...")
        self.qid = qid
        self.sum = 0
    def f(self):
        for _ in range(10):
            val = ray.read_queue(self.qid)
            self.sum += val
            print("read_queue, val: " + str(val))
    def get_sum(self):
        return self.sum

if __name__ == "__main__":
    ray.init()
    A.remote().f.remote()
    time.sleep(20)
