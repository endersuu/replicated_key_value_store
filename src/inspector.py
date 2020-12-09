#!/usr/bin/python3
import ast
import random
import sys
import glob
import time

import pickle

import base64

sys.path.append('gen-py')
sys.path.insert(0, glob.glob('/home/cs557-inst/thrift-0.13.0/lib/py/build/lib*')[0])

from chord import KVStore
from chord.ttypes import ClientRequest, ServerResponse, KV

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

import logging

FORMAT = f"%(filename)-13s|%(funcName)-13s|%(lineno)-3d|%(message)s"
logging.basicConfig(format=FORMAT)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def getClient(ip, port):
  transport = TSocket.TSocket(ip, port)

  # Buffering is critical. Raw sockets are very slow
  transport = TTransport.TBufferedTransport(transport)

  # Wrap in a protocol
  protocol = TBinaryProtocol.TBinaryProtocol(transport)

  # Create a client to use the protocol encoder
  client = KVStore.Client(protocol)

  # Connect!
  transport.open()
  return client


def main():
  with open('nodes.txt') as serversInfo:
    serversConfig = map(lambda line: line.strip().split(':'), serversInfo.readlines())

  kvs = {}
  for info in inspect(serversConfig):
    print(info)
    kvMem = info['kvMem']
    for key in kvMem.keys():
      # add new key
      if key not in kvs:
        kvs[key] = kvMem[key]
        continue

      # compare result
      if kvs[key] != kvMem[key]:
        logger.warning(f'>>>>>> Ooops, check the value of server {info["serverID"]} key {key}!')


def inspect(serversConfig):
  for ip, port in serversConfig:
    try:
      client = getClient(ip, port)
      yield pickle.loads(base64.b64decode(client.getInfo().encode()))

    except Exception as e:
      logger.critical(e)


if __name__ == '__main__':
  try:
    main()
  except Thrift.TException as tx:
    logger.error('%s' % tx.message)
