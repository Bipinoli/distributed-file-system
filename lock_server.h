// this is the lock server
// the lock client has a similar interface

#ifndef lock_server_h
#define lock_server_h

#include <string>
#include "lock_protocol.h"
#include "lock_client.h"
#include "rpc.h"
#include <map>
#include <vector>
#include <pthread.h>

class lock_server {

 private:
  lock_protocol::state get_lock_state(lock_protocol::lockid_t lock_id){
    pthread_mutex_lock(&lock_map_guard);
    if(locks_map.find(lock_id) == locks_map.end()){
      locks_map[lock_id] = PTHREAD_MUTEX_INITIALIZER;
      free_condition_map[lock_id] = PTHREAD_COND_INITIALIZER;
      lock_status_map[lock_id] = lock_protocol::state::free;
    }
    pthread_mutex_unlock(&lock_map_guard);
    return lock_status_map[lock_id];
  }

  void acquire_lock(lock_protocol::lockid_t lock_id){
    lock_status_map[lock_id] = lock_protocol::state::locked;
  }

  void release_lock(lock_protocol::lockid_t lock_id){
    lock_status_map[lock_id] = lock_protocol::state::free;
  }

 protected:
  int nacquire;
  std::map<lock_protocol::lockid_t, lock_protocol::state> lock_status_map;
  std::map<lock_protocol::lockid_t, pthread_cond_t> free_condition_map;
  std::map<lock_protocol::lockid_t, pthread_mutex_t> locks_map;
  pthread_mutex_t lock_map_guard = PTHREAD_MUTEX_INITIALIZER;

 public:
  lock_server();
  ~lock_server() {};
  lock_protocol::status stat(int clt, lock_protocol::lockid_t lid, int &);
  lock_protocol::status acquire(int clt, lock_protocol::lockid_t lid, int &);
  lock_protocol::status release(int clt, lock_protocol::lockid_t lid, int &);
};

#endif 







