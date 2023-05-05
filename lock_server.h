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
  lock_protocol::state create_new_lock(lock_protocol::lockid_t lock_id){
    lock_map[lock_id] = lock_protocol::state::free;
    return lock_map[lock_id];
  }

  lock_protocol::state get_lock_state(lock_protocol::lockid_t lock_id){
    if(lock_map.find(lock_id) == lock_map.end()){
      return create_new_lock(lock_id);
    }
    return lock_map[lock_id];
  }

  void acquire_lock(lock_protocol::lockid_t lock_id){
    lock_map[lock_id] = lock_protocol::state::locked;

  }

  void release_lock(lock_protocol::lockid_t lock_id){
    lock_map[lock_id] = lock_protocol::state::free;
  }

  // bool is_duplicate_call(lock_protocol::lockid_t lock_id, int clt, lock_protocol::rpc_numbers proc){
  //   std::tuple<lock_protocol::lockid_t, int, lock_protocol::rpc_numbers> call = std::make_tuple(lock_id, clt, proc);
  //   if(std::count(history.begin(), history.end(), call)){
  //     return true;
  //   } else {
  //     history.push_back(call);
  //     return false;
  //   }
  // }

 protected:
  int nacquire;
  std::map<lock_protocol::lockid_t, lock_protocol::state> lock_map;
  pthread_cond_t condition = PTHREAD_COND_INITIALIZER;
  pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
  pthread_cond_t condition2 = PTHREAD_COND_INITIALIZER;
  pthread_mutex_t lock2 = PTHREAD_MUTEX_INITIALIZER;
  // std::vector<std::tuple<lock_protocol::lockid_t, int, lock_protocol::rpc_numbers>> history;

 public:
  lock_server();
  ~lock_server() {};
  lock_protocol::status stat(int clt, lock_protocol::lockid_t lid, int &);
  lock_protocol::status acquire(int clt, lock_protocol::lockid_t lid, int &);
  lock_protocol::status release(int clt, lock_protocol::lockid_t lid, int &);
};

#endif 







