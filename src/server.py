#!/usr/bin/python3

import glob
import sys
import logging

import base64

import database as db
import typing
import time
import os
import threading
import pickle

sys.path.append('gen-py')
sys.path.insert(0, glob.glob('/home/cs557-inst/thrift-0.13.0/lib/py/build/lib*')[0])

from chord import KVStore
from chord.ttypes import SystemException, KV, ClientRequest, ServerResponse

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
from thrift.protocol.THeaderProtocol import THeaderProtocolFactory

FORMAT = f"%(filename)-13s|%(funcName)-13s|%(lineno)-3d|%(message)s"
logging.basicConfig(format=FORMAT)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

currentTime = lambda: int(round(time.time() * 100000))


class MyServer(TServer.TServer):
  """My single-threaded server that just pumps around one transport."""

  def __init__(self, *args):
    TServer.TServer.__init__(self, *args)

  def serve(self, method):
    self.serverTransport.listen()

    t = threading.Thread(target=method, name='recoverThread')
    t.start()

    while True:
      client = self.serverTransport.accept()

      if not client:
        continue

      itrans = self.inputTransportFactory.getTransport(client)
      iprot = self.inputProtocolFactory.getProtocol(itrans)

      # for THeaderProtocol, we must use the same protocol instance for
      # input and output so that the response is in the same dialect that
      # the server detected the request was in.
      if isinstance(self.inputProtocolFactory, THeaderProtocolFactory):
        otrans = None
        oprot = iprot
      else:
        otrans = self.outputTransportFactory.getTransport(client)
        oprot = self.outputProtocolFactory.getProtocol(otrans)
      try:
        while True:
          self.processor.process(iprot, oprot)
      except TTransport.TTransportException:
        pass
      except Exception as x:
        logger.exception(x)

      itrans.close()
      if otrans:
        otrans.close()


class KVStoreHandler:
  def __init__(self, serverID: int, serversConf: str = 'nodes.txt'):
    """
    :param serverID: 0, 1, 2, 3
    :param serversConf: path of server configuration, content of configuration includes ip and port of all servers
    assumption:
      1. a coordinator is always able to write data to itself successfully
      2. put-requests from clients always carry values
    """
    self.serverID = serverID
    dbname = f'{serverID}.db'
    self.brandNewServer = not os.path.isfile(dbname)

    # for data storage
    self.kvMem = {}
    self.dbLock = threading.Lock()
    with self.dbLock:
      self.dbCon, self.dbCur = db.sqlInit(name=dbname)

    'a coordinator read config then it can understand how to connect to replicas'
    with open(serversConf, 'r') as conf:
      # [('128.226.117.11', 9001), ('128.226.117.11', 9002), ('128.226.117.11', 9003), ('128.226.117.11', 9004)]
      self.serversInfo: typing.List[typing.Tuple[str, int]] = [
        tuple((serverInfo.rstrip().split(':')[0], int(serverInfo.rstrip().split(':')[1])))
        for serverInfo in conf.readlines()]

    logger.debug('handler init end')

  def __str__(self):
    return str({'serverID': self.serverID, 'kvMem': self.kvMem})

  __repr__ = __str__

  def clientGet(self, request):
    """
    coordinator receivers request from client, then make RPC to replica
    """
    logger.debug(f'coordinator {self.serverID} receive request = {request}')

    # request format check
    if request.kv.value is not None or (request.constLevel != 'ONE' and request.constLevel != 'QUORUM'):
      raise SystemException("Bad request format")

    successResponses = []
    for replicaID in self._partition(request):
      # try to connect to each replica
      try:
        with self._GetClientFromServer(self, replicaID) as replica:
          # read from replica
          serverResponse = replica.serverGet(request.kv)
          if serverResponse.status == "SUCCESS":
            successResponses.append(serverResponse)

      except TSocket.TTransportException as e:
        logger.warning(f'server {replicaID} crashed.')
        logger.warning(e)
        continue

      # check return condition
      if request.constLevel == "ONE" and len(successResponses) == 1:
        return ServerResponse("SUCCESS", successResponses[0].kv)
      if request.constLevel == "QUORUM" and len(successResponses) == 2:
        return ServerResponse("SUCCESS", successResponses[0].kv) \
          if successResponses[0].kv.time > successResponses[1].kv.time \
          else ServerResponse("SUCCESS", successResponses[1].kv)

    raise SystemException('Not enough replicas alive')

  def clientPut(self, request):
    """
    coordinator receivers request from client, then make RPC to replica
    """
    logger.debug(f'coordinator {self.serverID} receive request = {request}')

    # request format check
    if request.kv.value is None or (request.constLevel != 'ONE' and request.constLevel != 'QUORUM'):
      raise SystemException("Bad request format")

    current = currentTime()
    successReplicas = []
    pendingHints = []
    for replicaID in self._partition(request):
      # try to connect to each replica
      try:
        with self._GetClientFromServer(self, replicaID) as replica:
          # write to replica
          request.kv.time = current
          serverResponse = replica.serverPut(request.kv)
          # FIXME is it possible that serverResponse.status == "FALL" in write request?
          if serverResponse.status == "FAIL":
            pendingHints.append((replicaID, current, request.kv.key, request.kv.value))
            continue
          elif serverResponse.status == "SUCCESS":
            successReplicas.append(replicaID)

      except TSocket.TTransportException as e:
        logger.warning(f'server {replicaID} crashed.')
        logger.warning(e)
        # pend hint
        pendingHints.append((replicaID, current, request.kv.key, request.kv.value))
        continue

    # check return condition
    if (request.constLevel == "ONE" and len(successReplicas) < 1) or \
        (request.constLevel == "QUORUM" and len(successReplicas) < 2):
      # delete all previous writes to replica, assume the replicas won't crash since we just write to them
      for replicaID in successReplicas:
        # try to connect to each replica
        try:
          with self._GetClientFromServer(self, replicaID) as replica:
            # delete the failure write from replicas 
            replica.serverDelete(request.kv)

        except TSocket.TTransportException as e:
          logger.warning(f'server {replicaID} crashed when attempting to delete a KV pair.')
          logger.warning(e)
          continue

      raise SystemException('Not enough replicas alive')

    for pendingHint in pendingHints:
      # log the hint
      with self.dbLock:
        db.addHintLog(self.dbCon, self.dbCur, *pendingHint)

    return ServerResponse("SUCCESS")

  def serverGet(self, kv):
    """
    replica receivers request from coordinator
    """
    logger.debug(f'replica {self.serverID} receive request, get kv = {kv}')

    ret = ServerResponse("SUCCESS", self.kvMem[kv.key]) \
      if kv.key in self.kvMem \
      else ServerResponse("FAIL")

    return ret

  def serverPut(self, kv, addWriteLog=True):
    """
    replica receivers request from coordinator
    """
    logger.debug(f'replica {self.serverID} receive request, put kv = {kv}')

    if addWriteLog:
      with self.dbLock:
        db.addWriteLog(self.dbCon, self.dbCur, kv.time, kv.key, kv.value)
    if (kv.key not in self.kvMem) or (kv.key in self.kvMem and kv.time > self.kvMem[kv.key].time):
      self.kvMem[kv.key] = kv
    ret = ServerResponse("SUCCESS")

    return ret

  def serverDelete(self, kv):
    """
    replica receivers delete request from coordinator after failure write
    """
    logger.debug(f'replica {self.serverID} receive request, delete kv = {kv}')

    with self.dbLock:
      # delete corresponding write log
      db.deleteWriteLog(self.dbCon, self.dbCur, kv.time, kv.key, kv.value)
      # recover the previous write log
      prevWriteLog = db.findPrevWriteLog(self.dbCon, self.dbCur, kv.time, kv.key, kv.value)
    # if no prev write log is found, directly clear in in-memory data
    if len(prevWriteLog) == 0:
      del self.kvMem[kv.key]
    # if a prev write log is found, restore that kv pair
    else:
      self.kvMem[kv.key] = KV(key=prevWriteLog[0][1], value=prevWriteLog[0][2], time=prevWriteLog[0][0])

  def recoverStart(self):
    """
    after crashed server up, it is starting recover progress
    """
    logger.debug(f'RECOVER: server {self.serverID}, recover start')
    if self.brandNewServer:
      logger.debug(f'RECOVER: server {self.serverID} is brand new server, recover end')
      self.brandNewServer = False
      return

    # recover from local log
    self.selfRecover()
    # send recover notification to other coordinators
    self.recoverNotify()
    # resend hints to other replicas
    for replicaID in range(len(self.serversInfo)):
      if replicaID == self.serverID:
        continue

      try:
        self.resendHints(replicaID)
      except Exception as e:
        logger.warning(f'server {replicaID} crashed.')
        logger.warning(e)
        continue

    logger.debug(f'RECOVER: server {self.serverID}, recover end')

  def selfRecover(self):
    """
    replica is recovering from local log
    """
    logger.debug(f'RECOVER: server {self.serverID}, self recover start')

    with self.dbLock:
      logs = db.getAllWriteLog(self.dbCon, self.dbCur)

    # self recovery from stored log
    # log: (time, key, value)
    for log in logs:
      logger.debug(f'RECOVER: key, value, time = {log[1]}, {log[2]}, {log[0]}')
      self.serverPut(KV(log[1], log[2], log[0]), addWriteLog=False)

    logger.debug(f'RECOVER: server {self.serverID}, self recover end')

  def recoverNotify(self):
    """
    replica is sending recover-notification to all other coordinators
    """
    logger.debug(f'RECOVER: server {self.serverID}, recover notify start')

    # send recovery notification to other coordinators
    for coordinatorID in range(len(self.serversInfo)):
      if coordinatorID == self.serverID:
        continue

      try:
        with self._GetClientFromServer(self, coordinatorID) as coordinator:
          # notify other coordinators
          logger.debug(f'RECOVER: replica {self.serverID} recover notify to coordinator {coordinator.getServerID()}')
          coordinator.resendHints(self.serverID)

      except Exception as e:
        logger.warning(f'server {coordinatorID} crashed. some data may lost')
        logger.warning(e)
        continue

    logger.debug(f'RECOVER: server {self.serverID}, recover notify end')

  def resendHints(self, replicaID):
    """
    coordinator is resending hints to a replica
    """
    with self.dbLock:
      hints = db.getHintLog(self.dbCon, self.dbCur, replicaID)
    hintsKVs = [KV(key=hint[2], value=hint[3], time=hint[1]) for hint in hints]
    logger.debug(f'RECOVER: coordinator {self.serverID}, hintsKVs = {hintsKVs}')

    # no hints to send
    if len(hintsKVs) == 0:
      return

    # make connection to replica
    with self._GetClientFromServer(self, replicaID) as replica:
      # send hints to replica
      logger.debug(f'RECOVER: coordinator {self.serverID} send hints to replica {replica.getServerID()}')

      for kv in hintsKVs:
        replica.serverPut(kv)

      with self.dbLock:
        db.deleteHintLog(self.dbCon, self.dbCur, replicaID)

  def hello(self) -> str:
    return str(self.serverID)

  def getServerID(self):
    return self.serverID

  def getInfo(self):
    return base64.b64encode(pickle.dumps({'serverID': self.serverID, 'kvMem': self.kvMem})).decode()

  @staticmethod
  def _partition(request) -> typing.Tuple[int, int, int]:
    """
    _partition decides which replicas this request is routed to
    :param request: ClientRequest
    :return: return the ids of Primary replica, replica next to Primary replica, replica next to next replica
    """
    replicaID: int = request.kv.key // 64
    return replicaID % 4, (replicaID + 1) % 4, (replicaID + 2) % 4

  class _GetClientFromServer:
    """
    used to create a new client
    """

    def __init__(self, localServer, remoteServerID: int):
      self.remoteServerID = remoteServerID
      self.localServer = localServer
      self.remoteServerInfo = localServer.serversInfo[remoteServerID]

    def __enter__(self):
      if self.localServer.serverID == self.remoteServerID:
        logger.debug(f'return local {self.remoteServerID} to {self.localServer.serverID}')
        return self.localServer

      self.transport = TSocket.TSocket(self.remoteServerInfo[0], str(self.remoteServerInfo[1]))
      self.transport = TTransport.TBufferedTransport(self.transport)
      protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
      client = KVStore.Client(protocol)
      self.transport.open()
      logger.debug(f'return remote {self.remoteServerID} to {self.localServer.serverID}')
      return client

    def __exit__(self, exc_type, exc_val, exc_tb):
      if self.localServer.serverID != self.remoteServerID:
        logger.debug(f'close connection from remote {self.remoteServerID} to local {self.localServer.serverID}')
        self.transport.close()

      else:
        logger.debug(f'nothing to close from local {self.remoteServerID} to local {self.localServer.serverID}')


if __name__ == '__main__':
  handler = KVStoreHandler(serverID=(int(sys.argv[1]) - 9000))
  processor = KVStore.Processor(handler)
  transport = TSocket.TServerSocket(port=int(sys.argv[1]))
  tfactory = TTransport.TBufferedTransportFactory()
  pfactory = TBinaryProtocol.TBinaryProtocolFactory()

  server = MyServer(processor, transport, tfactory, pfactory)
  # handler.recoverNotify()
  logger.info(f'Starting the server... pid = {os.getpid()}, port = {sys.argv[1]}')
  server.serve(handler.recoverStart)
  logger.info('done.')
