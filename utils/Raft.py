from __future__ import print_function

import sys
import time
from functools import partial
from pysyncobj import SyncObj, replicated

#
#   *** Raft Service: This class overrides few methods as per our need. ***
#
class Raft(SyncObj):

  def __init__(self, selfNodeAddr, otherNodeAddrs):
      super(Raft, self).__init__(selfNodeAddr, otherNodeAddrs)
      self.__counter = 0

  @replicated
  def incCounter(self):
      self.__counter += 1
      return self.__counter

  @replicated
  def addValue(self, value, cn):
      self.__counter += value
      return self.__counter, cn

  def getCounter(self):
      return self.__counter