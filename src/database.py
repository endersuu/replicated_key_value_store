import sqlite3

import logging

FORMAT = f"%(filename)-13s|%(funcName)-13s|%(lineno)-3d|%(message)s"
logging.basicConfig(format=FORMAT)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def sqlInit(name="config.db"):
  conn = sqlite3.connect(name, check_same_thread=False)
  c = conn.cursor()

  c.execute("""
        CREATE TABLE IF NOT EXISTS writeLog (
        time  integer,
        key   integer,
        value text
        )""")

  c.execute("""
        CREATE TABLE IF NOT EXISTS hintLog (
        serverId integer,
        time  integer,
        key   integer,
        value text
        )""")
  return conn, c


def getAllWriteLog(conn, c):
  try:
    with conn:
      c.execute("SELECT * FROM writeLog")
  except sqlite3.IntegrityError:
    logger.error("sqlite error: cannot get all write log")

  return c.fetchall()


def deleteWriteLog(conn, c, time, key, value):
  try:
    with conn:
      c.execute("DELETE FROM writeLog where key=? AND value=? AND time=?", (key, value, time))
  except sqlite3.IntegrityError:
    logger.error("sqlite error: cannot delete a write log")


def findPrevWriteLog(conn, c, time, key, value):
  try:
    with conn:
      c.execute("SELECT * FROM writeLog WHERE key=? AND time < ? ORDER BY time DESC LIMIT 1",
                (key, time))
  except sqlite3.IntegrityError:
    logger.error("sqlite error: cannot find a write log")

  return c.fetchall()


def addWriteLog(conn, c, time, key, value):
  try:
    with conn:
      c.execute("INSERT INTO writeLog VALUES (?,?,?)",
                (time, key, value))
  except sqlite3.IntegrityError:
    logger.error("sqlite error: cannot add a write log")


def getHintLog(conn, c, serverId):
  try:
    with conn:
      c.execute("SELECT * FROM hintLog WHERE serverId=?", (serverId,))
  except sqlite3.IntegrityError:
    logger.error("sqlite error: cannot get a hint log")

  return c.fetchall()


def addHintLog(conn, c, serverId, time, key, value):
  try:
    with conn:
      c.execute("INSERT INTO hintLog VALUES (?,?,?,?)",
                (serverId, time, key, value))
  except sqlite3.IntegrityError:
    logger.error("sqlite error: cannot add a hint log")


def deleteHintLog(conn, c, serverId):
  try:
    with conn:
      c.execute("DELETE FROM hintLog where serverId=?", (serverId,))
  except sqlite3.IntegrityError:
    logger.error("sqlite error: cannot delete a hint log")
