// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>


static void *
releasethread(void *x)
{
  lock_client_cache *cc = (lock_client_cache *) x;
  cc->releaser();
  return 0;
}

int lock_client_cache::last_port = 0;

lock_client_cache::lock_client_cache(std::string xdst, 
				     class lock_release_user *_lu)
  : lock_client(xdst), lu(_lu)
{
  srand(time(NULL)^last_port);
  rlock_port = ((rand()%32000) | (0x1 << 10));
  const char *hname;
  // assert(gethostname(hname, 100) == 0);
  hname = "127.0.0.1";
  std::ostringstream host;
  host << hname << ":" << rlock_port;
  id = host.str();
  last_port = rlock_port;
  rpcs *rlsrpc = new rpcs(rlock_port);

  /* register RPC handlers with rlsrpc */
  rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache::revoke_handler);
  rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::retry_handler);

  // setup connection to server
  int _r; assert(cl->call(lock_protocol::subscribe, cl->id(), id, _r) == lock_protocol::OK);

  pthread_t th;
  int r = pthread_create(&th, NULL, &releasethread, (void *) this);
  assert (r == 0);
}


void
lock_client_cache::releaser() {
  // This method should be a continuous loop, waiting to be notified of
  // freed locks that have been revoked by the server, so that it can
  // send a release RPC.
  while (true) {
    auto req = release_queue.consume();
    pthread_mutex_lock(&cache_mutex);
    Lock &lock = cache[req.lid];
    lock.status = Lock::RELEASING;
    pthread_mutex_unlock(&cache_mutex);

    int r; assert(cl->call(lock_protocol::release, cl->id(), req.lid, req.seq, r) == lock_protocol::OK);

    pthread_mutex_lock(&cache_mutex);
    lock.status = Lock::NONE;
    pthread_mutex_unlock(&cache_mutex);
    pthread_cond_broadcast(&acquire_signal);
  }
}



lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid) {
  ScopedLock guard(&cache_mutex);

  Lock& lock = cache[lid];

  while (true) {
    if (lock.status == Lock::LOCKED || lock.status == Lock::ACQUIRING || lock.status == Lock::RELEASING) {
      pthread_cond_wait(&acquire_signal, &cache_mutex);
      continue;
    }
    if (lock.status == Lock::FREE) {
      lock.status = Lock::LOCKED;
      return lock_protocol::OK;
    }

    lock.status = Lock::ACQUIRING;
    auto seq = ++lock.seqnum;

    while (true) {
      pthread_mutex_unlock(&cache_mutex);

      int r; auto ret = cl->call(lock_protocol::acquire, cl->id(), lid, seq, r);
      if (ret == lock_protocol::OK) break;

      pthread_mutex_lock(&cache_mutex);
      // retry activated by outdated messages should be ignored
      while (lock.seqnum_at_retry < lock.seqnum) {
        pthread_cond_wait(&retry_signal, &cache_mutex);
      }
      // if the call fails we need to wait for another retry request
      // doing this to enter the signal wait again
      lock.seqnum_at_retry -= 1;
    }

    pthread_mutex_lock(&cache_mutex);
    lock.status = Lock::LOCKED;
    lock.seqnum_at_retry = lock.seqnum;
    return lock_protocol::OK;
  }
}



lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid) {
  ScopedLock guard(&cache_mutex);

  Lock& lock = cache[lid];
  // release to cache
  if (lock.seqnum_at_revoke < lock.seqnum) {
    lock.status = Lock::FREE;
    pthread_cond_broadcast(&acquire_signal);
    return lock_protocol::OK;
  }
  // release to server
  lock.status = Lock::RELEASING;
  release_queue.add(release_req(lid, lock.seqnum));
  return lock_protocol::OK;
}



rlock_protocol::status
lock_client_cache::revoke_handler(lock_protocol::lockid_t lid, unsigned int seq, int& r) {
  ScopedLock guard(&cache_mutex);

  Lock& lock = cache[lid];
  lock.seqnum_at_revoke = seq;

  if (lock.status != Lock::FREE) {
    // it will be released after the thread releases the lock - release method
    return rlock_protocol::OK;
  }
  lock.status = Lock::RELEASING;
  release_queue.add(release_req(lid, seq));
  return rlock_protocol::OK;
}



rlock_protocol::status
lock_client_cache::retry_handler(lock_protocol::lockid_t lid, unsigned int seq, int& r) {
  pthread_mutex_lock(&cache_mutex);
  cache[lid].seqnum_at_retry = seq;
  pthread_mutex_unlock(&cache_mutex);

  pthread_cond_broadcast(&retry_signal);
  return rlock_protocol::OK;
}