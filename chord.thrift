exception SystemException {
  1: optional string message;
}

struct KV {
  1: i32 key;
  2: optional string value;
  3: optional i64 time; // server timestamp
}

struct ClientRequest {
  1: string constLevel; // ONE or QUORUM
  2: KV kv;
}

struct ServerResponse {
  1: string status; // fail or success
  2: optional KV kv;
}

service KVStore {
  // client request
  ServerResponse clientGet(1: ClientRequest request)
    throws (1: SystemException systemException),
  ServerResponse clientPut(1: ClientRequest request)
    throws (1: SystemException systemException),

  // used between servers
  ServerResponse serverGet(1: KV kv)
    throws (1: SystemException systemException),
  ServerResponse serverPut(1: KV kv)
    throws (1: SystemException systemException),
  void serverDelete(1: KV kv)
    throws (1: SystemException systemException),

  // server reboot
  void resendHints(1: i32 recoveredServerID)
    throws (1: SystemException systemException),

   // for debug
  string hello()
    throws (1: SystemException systemException),

  i32 getServerID()
    throws (1: SystemException systemException),

  string getInfo()
    throws (1: SystemException systemException),
}
