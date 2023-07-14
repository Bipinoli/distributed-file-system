#ifndef lock_server_cache_h
#define lock_server_cache_h

#include <string>
#include <map>
#include <queue>
#include "lock_protocol.h"
#include "rpc.h"
#include "slock.h"


template<class T>
class events_queue {
private:
    std::list<T> queue;
    pthread_mutex_t queue_mutex = PTHREAD_MUTEX_INITIALIZER;
    pthread_cond_t consume_signal = PTHREAD_COND_INITIALIZER;
public:
    void add(T t) {
      ScopedLock guard(&queue_mutex);
      queue.push_back(t);
      pthread_cond_signal(&consume_signal);
    }
    T consume() {
      ScopedLock guard(&queue_mutex);
      while (queue.empty()) {
        pthread_cond_wait(&consume_signal, &queue_mutex);
      }
      T retval = queue.front();
      queue.pop_front();
      return retval;
    }
};


class Client {
public:
    int clt;
    unsigned int seq;
    Client() {}
    Client(int clt, unsigned int seq) {
      this->clt = clt;
      this->seq = seq;
    }
};
bool operator==(const Client &lhs, const Client &rhs);
bool operator!=(const Client &lhs, const Client &rhs);


class lock_info {
public:
    enum Status { FREE, LOCKED, REVOKING };
    lock_info::Status status;
    Client owning_client;
    std::queue<Client> waiting_clients;
    lock_info() {
      status = FREE;
    }
};

#include "rsm.h"


class lock_server_cache {
 private:
  class rsm *rsm;
 public:
  lock_server_cache(class rsm *rsm = 0);
  std::map<int, rpcc *> clients;
  pthread_mutex_t cache_mutex = PTHREAD_MUTEX_INITIALIZER;
  std::map<lock_protocol::lockid_t, lock_info> cached_locks;
  events_queue<lock_protocol::lockid_t> revoke_queue;
  events_queue<lock_protocol::lockid_t> retry_queue;


  lock_server_cache();
  lock_protocol::status stat(lock_protocol::lockid_t, int &);
  void revoker();
  void retryer();

  lock_protocol::status subscribe(int clt, std::string dst, int &);
  lock_protocol::status acquire(int clt, lock_protocol::lockid_t lid, unsigned int seq, int &);
  lock_protocol::status release(int clt, lock_protocol::lockid_t lid, unsigned int seq, int &);
};

#endif
