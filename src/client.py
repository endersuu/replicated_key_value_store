#!/usr/bin/python3
import random
import sys
import glob

sys.path.append('gen-py')
sys.path.insert(0, glob.glob('/home/cs557-inst/thrift-0.13.0/lib/py/build/lib*')[0])

from chord import KVStore
from chord.ttypes import SystemException, ClientRequest, ServerResponse, KV

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

import logging

FORMAT = f"%(filename)-13s|%(funcName)-13s|%(lineno)-3d|%(message)s"
logging.basicConfig(format=FORMAT)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class Console:
  def __init__(self, client):
    self.coordinator = client

  def sendToCoord(self, command: str):
    logger.debug(f'Receiver command "{command}"')
    """
    get 1 one
    put 1 10 quorum
    """
    tokens = command.split(' ')
    key = int(tokens[1])
    constLevel = tokens[-1].upper()

    if not 0 <= key <= 255:
      raise SystemException('Keys are out of range [0, 255]')

    if constLevel != 'ONE' and constLevel != 'QUORUM':
      raise SystemException(f'ConstLevel should be either "ONE" or "QUORUM"')

    if tokens[0] == 'put' and len(tokens) == 4:
      return self.coordinator.clientPut(ClientRequest(constLevel, KV(key, tokens[2])))
    if tokens[0] == 'get' and len(tokens) == 3:
      return self.coordinator.clientGet(ClientRequest(constLevel, KV(key)))
    else:
      raise SystemException('Bad request format')


class GetClient:
  def __init__(self, ip, port):
    self.ip = ip
    self.port = port

  def __enter__(self):
    self.transport = TSocket.TSocket(self.ip, self.port)

    # Buffering is critical. Raw sockets are very slow
    self.transport = TTransport.TBufferedTransport(self.transport)

    # Wrap in a protocol
    protocol = TBinaryProtocol.TBinaryProtocol(self.transport)

    # Create a client to use the protocol encoder
    client = KVStore.Client(protocol)

    # Connect!
    self.transport.open()
    logger.info(f'Client has connected to {self.ip}:{self.port}')
    return client

  def __exit__(self, exc_type, exc_val, exc_tb):
    self.transport.close()


def main():
  # Make socket

  with open('nodes.txt', 'r') as config:
    serversConfig = tuple(map(str.strip, config.readlines()))

  serverConfig = random.choice(serversConfig) if len(sys.argv) == 1 else serversConfig[int(sys.argv[1])]

  ip, port = serverConfig.split(':')
  port = int(port)

  '''console start'''

  while True:
    try:
      commands = input("\nWait for commands:\n")
      if commands == '':
        continue
      if commands == 'exit':
        break

      with GetClient(ip, port) as client:
        console = Console(client)
        for command in commands.split('\n'):
          logger.info(console.sendToCoord(command.strip()))

    except Exception as e:
      logger.critical(e)


if __name__ == '__main__':
  try:
    main()
  except Thrift.TException as tx:
    logger.error('%s' % tx.message)
