// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

lock_server::lock_server():
  nacquire (0)
{
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  printf("stat request from clt %d\n", clt);
  r = nacquire;
  return ret;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r)
{
  printf("acquire request from clt %d - lid %016llx\n", clt, lid);
  while (get_lock_state(lid) == lock_protocol::state::locked) {
    pthread_cond_wait(&free_condition_map[lid], &locks_map[lid]);
  }
  acquire_lock(lid);
  nacquire++;
  pthread_mutex_unlock(&locks_map[lid]);
  return lock_protocol::OK;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
  printf("release request from clt %d - lid %016llx\n", clt, lid);
  nacquire--;
  // condition signal can be sent from a thread even when it doesn't own the mutex
  release_lock(lid);
  pthread_cond_signal(&free_condition_map[lid]);
  return lock_protocol::OK;
}