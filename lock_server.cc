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
  pthread_mutex_lock(&lock);
  lock_protocol::state lock_state = get_lock_state(lid);
  if(lock_state == lock_protocol::state::locked){
    pthread_cond_wait(&condition, &lock);
  }
  printf("acquire request from clt %d - lid %016llx\n", clt, lid);
  acquire_lock(lid);
  r = ++nacquire;
  pthread_cond_signal(&condition);
  pthread_mutex_unlock(&lock);
  return lock_protocol::OK;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
  pthread_mutex_lock(&lock2);
  lock_protocol::state lock_state = get_lock_state(lid);
  if(lock_state == lock_protocol::state::free){
    pthread_cond_wait(&condition2, &lock2);
  }
  printf("release request from clt %d - lid %016llx\n", clt, lid);
  release_lock(lid);
  r = --nacquire;
  pthread_cond_signal(&condition2);
  pthread_mutex_unlock(&lock2);
  return lock_protocol::OK;
}