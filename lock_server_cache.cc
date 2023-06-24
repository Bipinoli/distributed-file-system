// the caching lock server implementation

#include "slock.h"
#include "lock_server_cache.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

static void *
revokethread(void *x)
{
  lock_server_cache *sc = (lock_server_cache *) x;
  sc->revoker();
  return 0;
}

static void *
retrythread(void *x)
{
  lock_server_cache *sc = (lock_server_cache *) x;
  sc->retryer();
  return 0;
}

lock_server_cache::lock_server_cache()
{
  pthread_t th;
  int r = pthread_create(&th, NULL, &revokethread, (void *) this);
  assert (r == 0);
  r = pthread_create(&th, NULL, &retrythread, (void *) this);
  assert (r == 0);
}

void
lock_server_cache::revoker()
{
  // This method should be a continuous loop, that sends revoke
  // messages to lock holders whenever another client wants the
  // same lock
  while (true) {
    auto lid = revoke_queue.consume();
    std::cout << "revoker\n";
    pthread_mutex_lock(&cache_mutex);
    auto lock_holder = cached_locks[lid].owning_client;
    rpcc *cl = clients[lock_holder.clt];
    pthread_mutex_unlock(&cache_mutex);
    int r; assert(cl->call(rlock_protocol::revoke, lid, lock_holder.seq, r) == rlock_protocol::OK);
  }
}


void
lock_server_cache::retryer()
{
  // This method should be a continuous loop, waiting for locks
  // to be released and then sending retry messages to those who
  // are waiting for it.
  while (true) {
    auto lid = retry_queue.consume();
    pthread_mutex_lock(&cache_mutex);
    lock_info lock = cached_locks[lid];
    if (lock.waiting_clients.empty()) {
      pthread_mutex_unlock(&cache_mutex);
      continue;
    }
    auto client = lock.waiting_clients.front();
    lock.waiting_clients.pop();
    rpcc *cl = clients[client.clt];
    pthread_mutex_unlock(&cache_mutex);
    int r; assert(cl->call(rlock_protocol::retry, lid, client.seq, r) == rlock_protocol::OK);
  }
}


lock_protocol::status
lock_server_cache::subscribe(int clt, std::string dst, int &) {
  std::cout << "subscribe\n";
  sockaddr_in dstsock;
  make_sockaddr(dst.c_str(), &dstsock);
  rpcc *cl = new rpcc(dstsock);
  if (cl->bind() < 0) {
    printf("lock_server subscribe: call bind with the clt: %d\n", clt);
  }
  pthread_mutex_lock(&cache_mutex);
  clients[clt] = cl;
  pthread_mutex_unlock(&cache_mutex);
  return lock_protocol::OK;
}


lock_protocol::status
lock_server_cache::acquire(int clt, lock_protocol::lockid_t lid, unsigned int seq, int &) {
  std::cout << "acquire\n";
  ScopedLock guard(&cache_mutex);

  Client client(clt, seq);
  lock_info& lock = cached_locks[lid];

  if (lock.status == lock_info::LOCKED) {
    lock.waiting_clients.push(client);
    lock.status = lock_info::REVOKING;
    revoke_queue.add(lid);
    std::cout << "revoking - sent retry\n";
    return lock_protocol::RETRY;
  }

  if (lock.status == lock_info::REVOKING) {
    lock.waiting_clients.push(client);
    std::cout << "is revoking - sent retry\n";
    return lock_protocol::RETRY;
  }

  if (lock.status == lock_info::FREE) {
    lock.owning_client = client;
    if (!lock.waiting_clients.empty()) {
      lock.status = lock_info::REVOKING;
      revoke_queue.add(lid);
      std::cout << "lock granted - not empty\n";
      return lock_protocol::OK;
    }
    lock.status = lock_info::LOCKED;
    return lock_protocol::OK;
  }

  return lock_protocol::RPCERR;
}


lock_protocol::status
lock_server_cache::release(int clt, lock_protocol::lockid_t lid, unsigned int seq, int &) {
  std::cout << "release\n";
  ScopedLock guard(&cache_mutex);
  Client client(clt, seq);
  lock_info& lock = cached_locks[lid];
  lock.status = lock_info::FREE;
  retry_queue.add(lid);
  return lock_protocol::OK;
}


bool operator==(const Client &lhs, const Client &rhs) {
  return lhs.clt == rhs.clt && lhs.seq == rhs.seq;
}
bool operator!=(const Client &lhs, const Client &rhs) {
  return not(lhs == rhs);
}