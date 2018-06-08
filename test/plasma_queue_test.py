# Simple test for plasma queue usage. Actor A produces datas and Actor B 
# consumes them. We also add a normal Ray implementation for comparition.

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray

# Provider A and Receiver B not using plasma queue
@ray.remote
class A(object):
    def __init__(self):
        self.b = B.remote()

    def f(self):
        for i in range(10):
            self.b.add.remote(i)
        return ray.get(self.b.get_sum.remote())

@ray.remote
class B(object):
    def __init__(self):
        self.sum = 0

    def add(self, val):
        self.sum += val

    def get_sum(self):
        return self.sum

# Provider A and Receiver B using plasma queue
@ray.remote
class A_QUEUE(object):
    def __init__(self):
        self.qid = ray.create_queue()
    
    def f(self):
        B_QUEUE.remote(self.qid).f.remote()
        for i in range(10):
            ray.push_queue(qid, i)
        return ray.get(B_QUEUE.remote.get_sum.remote())

@ray.remote
class B_QUEUE(object):
    def __init__(self, qid):
        self.qid = qid
        self.sum = 0

    def add(self):
        for _ in range(10):
            self.sum += ray.read_queue(qid)

    def get_sum(self):
        return self.sum

if __name__ == "__main__":
    ray.init()
    print(ray.get(A.remote().f.remote()))
    print(ray.get(A_QUEUE.remote().f.remote()))
