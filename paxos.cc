#include "paxos.h"
#include "handle.h"
// #include <signal.h>
#include <stdio.h>
#include <chrono>
#include <thread>
#include "slock.h"

// This module implements the proposer and acceptor of the Paxos
// distributed algorithm as described by Lamport's "Paxos Made
// Simple".  To kick off an instance of Paxos, the caller supplies a
// list of nodes, a proposed value, and invokes the proposer.  If the
// majority of the nodes agree on the proposed value after running
// this instance of Paxos, the acceptor invokes the upcall
// paxos_commit to inform higher layers of the agreed value for this
// instance.


bool
operator> (const prop_t &a, const prop_t &b)
{
  return (a.n > b.n || (a.n == b.n && a.m > b.m));
}

bool
operator>= (const prop_t &a, const prop_t &b)
{
  return (a.n > b.n || (a.n == b.n && a.m >= b.m));
}

std::string
print_members(const std::vector<std::string> &nodes)
{
  std::string s;
  s.clear();
  for (unsigned i = 0; i < nodes.size(); i++) {
    s += nodes[i];
    if (i < (nodes.size()-1))
      s += ",";
  }
  return s;
}

bool isamember(std::string m, const std::vector<std::string> &nodes)
{
  for (unsigned i = 0; i < nodes.size(); i++) {
    if (nodes[i] == m) return 1;
  }
  return 0;
}

bool
proposer::isrunning()
{
  bool r;
  assert(pthread_mutex_lock(&pxs_mutex)==0);
  r = !stable;
  assert(pthread_mutex_unlock(&pxs_mutex)==0);
  return r;
}

// check if the servers in l2 contains a majority of servers in l1
bool
proposer::majority(const std::vector<std::string> &l1, 
		const std::vector<std::string> &l2)
{
  unsigned n = 0;

  for (unsigned i = 0; i < l1.size(); i++) {
    if (isamember(l1[i], l2))
      n++;
  }
  return n >= (l1.size() >> 1) + 1;
}

proposer::proposer(class paxos_change *_cfg, class acceptor *_acceptor, 
		   std::string _me)
  : cfg(_cfg), acc (_acceptor), me (_me), break1 (false), break2 (false), 
    stable (true)
{
  assert (pthread_mutex_init(&pxs_mutex, NULL) == 0);

}

void
proposer::setn()
{
  my_n.n = acc->get_n_h().n + 1 > my_n.n + 1 ? acc->get_n_h().n + 1 : my_n.n + 1;
}

bool
proposer::run(int instance, std::vector<std::string> c_nodes, std::string c_v)
{
  std::vector<std::string> accepts;
  std::vector<std::string> nodes;
  std::vector<std::string> nodes1;
  std::string v;
  bool r = false;

  pthread_mutex_lock(&pxs_mutex);
  printf("start: initiate paxos for %s w. i=%d v=%s stable=%d\n",
	 print_members(c_nodes).c_str(), instance, c_v.c_str(), stable);
  if (!stable) {  // already running proposer?
    printf("proposer::run: already running\n");
    pthread_mutex_unlock(&pxs_mutex);
    return false;
  }
  stable = false;
  setn();
  accepts.clear();
  nodes.clear();
  v.clear();
  nodes = c_nodes;
  if (prepare(instance, accepts, nodes, v)) {

    if (majority(c_nodes, accepts)) {
      printf("paxos::manager: received a majority of prepare responses\n");

      if (v.size() == 0) {
	v = c_v;
      }

      breakpoint1();

      nodes1 = accepts;
      accepts.clear();
      accept(instance, accepts, nodes1, v);

      if (majority(c_nodes, accepts)) {
	printf("paxos::manager: received a majority of accept responses\n");

	breakpoint2();

	decide(instance, accepts, v);
	r = true;
      } else {
	printf("paxos::manager: no majority of accept responses\n");
      }
    } else {
      printf("paxos::manager: no majority of prepare responses\n");
    }
  } else {
    printf("paxos::manager: prepare is rejected %d\n", stable);
  }
  stable = true;
  pthread_mutex_unlock(&pxs_mutex);
  return r;
}

bool
proposer::prepare(unsigned instance, std::vector<std::string> &accepts, 
         std::vector<std::string> nodes,
         std::string &v)
{
  // sleep for random delay i.e [10ms, 20ms] to avoid multiple prepare happening at the same time thereby both not getting the majority votes
  srand(time(NULL));
  int rand_delay = rand()%(20 + 1) + 10;
  std::this_thread::sleep_for(std::chrono::milliseconds(rand_delay));

  bool outdated_paxos = false;
  prop_t max_n = {0, std::string()};
  std::vector<std::thread> call_threads;
  pthread_mutex_unlock(&pxs_mutex);
  for (auto node : nodes) {
    call_threads.push_back(std::thread([=, &accepts, &v, &outdated_paxos, &max_n]() {
        paxos_protocol::preparearg a{instance, my_n};
        paxos_protocol::prepareres r;
        paxos_protocol::status ret;
        handle h(node);
        if (h.get_rpcc() && (h.get_rpcc()->call(paxos_protocol::preparereq, me, a, r, rpcc::to(1000)) == paxos_protocol::OK)) {
          ScopedLock guard(&pxs_mutex);
          if (r.oldinstance) {
            acc->commit(instance, r.v_a);
            outdated_paxos = true;
          } else if (r.accept) {
            accepts.push_back(node);
            if (r.n_a > max_n) {
              v = r.v_a;
              max_n = r.n_a;
            }
          }
        }
    }));
  }
  for (auto &t : call_threads)
    t.join();
  pthread_mutex_lock(&pxs_mutex);
  return !outdated_paxos;
}


void
proposer::accept(unsigned instance, std::vector<std::string> &accepts,
        std::vector<std::string> nodes, std::string v)
{
  std::vector<std::thread> call_threads;
  pthread_mutex_unlock(&pxs_mutex);
  for (auto node : nodes) {
    call_threads.push_back(std::thread([=, &accepts]() {
        paxos_protocol::acceptarg a{instance, my_n, v};
        paxos_protocol::status ret;
        int r = false;
        handle h(node);
        if (h.get_rpcc() && (h.get_rpcc()->call(paxos_protocol::acceptreq, me, a, r, rpcc::to(1000)) == paxos_protocol::OK) && r) {
          ScopedLock guard(&pxs_mutex);
          accepts.push_back(node);
        }
    }));
  }
  for (auto &t : call_threads)
    t.join();
  pthread_mutex_lock(&pxs_mutex);
}

void
proposer::decide(unsigned instance, std::vector<std::string> accepts, 
	      std::string v)
{
  std::vector<std::thread> call_threads;
  std::string nd = me;
  pthread_mutex_unlock(&pxs_mutex);
  acc->commit(instance, v);
  for (auto node : accepts) {
    if (node == nd)
      continue;
    call_threads.push_back(std::thread([=]() {
        paxos_protocol::decidearg a{instance, v};
        int r = false;
        handle h(node);
        if (h.get_rpcc())
          h.get_rpcc()->call(paxos_protocol::decidereq, me, a, r, rpcc::to(1000));
    }));
  }
  for (auto &t : call_threads)
    t.join();
  pthread_mutex_lock(&pxs_mutex);
}

acceptor::acceptor(class paxos_change *_cfg, bool _first, std::string _me, 
	     std::string _value)
  : cfg(_cfg), me (_me), instance_h(0)
{
  assert (pthread_mutex_init(&pxs_mutex, NULL) == 0);

  n_h.n = 0;
  n_h.m = me;
  n_a.n = 0;
  n_a.m = me;
  v_a.clear();

  l = new log (this, me);

  if (instance_h == 0 && _first) {
    values[1] = _value;
    l->loginstance(1, _value);
    instance_h = 1;
  }

  pxs = new rpcs(atoi(_me.c_str()));
  pxs->reg(paxos_protocol::preparereq, this, &acceptor::preparereq);
  pxs->reg(paxos_protocol::acceptreq, this, &acceptor::acceptreq);
  pxs->reg(paxos_protocol::decidereq, this, &acceptor::decidereq);
}

paxos_protocol::status
acceptor::preparereq(std::string src, paxos_protocol::preparearg a,
    paxos_protocol::prepareres &r)
{
  // handle a preparereq message from proposer
  ScopedLock guard(&pxs_mutex);
  if (a.instance <= instance_h)
    r = {true, false, n_a, values[a.instance]};
  else if (a.n > n_h) {
    n_h = a.n;
    r = {false, true, n_a, v_a};
    l->loghigh(n_h);
  } else
    r = {false, false, n_a, v_a};
  return paxos_protocol::OK;
}

paxos_protocol::status
acceptor::acceptreq(std::string src, paxos_protocol::acceptarg a, int &r)
{
  // handle an acceptreq message from proposer
  ScopedLock guard(&pxs_mutex);
  if (a.instance <= instance_h) {
    r = false;
  } else if (a.n >= n_h) {
    n_a = a.n;
    v_a = a.v;
    l->logprop(a.n, a.v);
    r = true;
  } else
    r = false;
  return paxos_protocol::OK;
}

paxos_protocol::status
acceptor::decidereq(std::string src, paxos_protocol::decidearg a, int &r)
{
  // handle an decide message from proposer
  if (a.instance <= instance_h) {
    return paxos_protocol::OK;
  } else {
    commit(a.instance, a.v);
    return paxos_protocol::OK;
  }
}

void
acceptor::commit_wo(unsigned instance, std::string value)
{
  //assume pxs_mutex is held
  printf("acceptor::commit: instance=%d has v= %s\n", instance, value.c_str());
  if (instance > instance_h) {
    printf("commit: highestaccepteinstance = %d\n", instance);
    values[instance] = value;
    l->loginstance(instance, value);
    instance_h = instance;
    n_h.n = 0;
    n_h.m = me;
    n_a.n = 0;
    n_a.m = me;
    v_a.clear();
    if (cfg) {
      pthread_mutex_unlock(&pxs_mutex);
      cfg->paxos_commit(instance, value);
      pthread_mutex_lock(&pxs_mutex);
    }
  }
}

void
acceptor::commit(unsigned instance, std::string value)
{
  pthread_mutex_lock(&pxs_mutex);
  commit_wo(instance, value);
  pthread_mutex_unlock(&pxs_mutex);
}

std::string
acceptor::dump()
{
  return l->dump();
}

void
acceptor::restore(std::string s)
{
  l->restore(s);
  l->logread();
}



// For testing purposes

// Call this from your code between phases prepare and accept of proposer
void
proposer::breakpoint1()
{
  if (break1) {
    printf("Dying at breakpoint 1!\n");
    exit(1);
  }
}

// Call this from your code between phases accept and decide of proposer
void
proposer::breakpoint2()
{
  if (break2) {
    printf("Dying at breakpoint 2!\n");
    exit(1);
  }
}

void
proposer::breakpoint(int b)
{
  if (b == 3) {
    printf("Proposer: breakpoint 1\n");
    break1 = true;
  } else if (b == 4) {
    printf("Proposer: breakpoint 2\n");
    break2 = true;
  }
}
