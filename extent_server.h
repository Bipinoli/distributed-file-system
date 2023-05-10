// this is the extent server

#ifndef extent_server_h
#define extent_server_h

#include <string>
#include <map>
#include "extent_protocol.h"

class extent_server {

 private:
  std::map<extent_protocol::extentid_t, std::string> storage;
  
  void put_data(extent_protocol::extentid_t id, std::string buf){
    storage[id] = buf;
  }

  std::string get_data(extent_protocol::extentid_t id){
    return storage[id];
  }

  void remove_data(extent_protocol::extentid_t id){
    storage.erase(id);
  }

 public:
  extent_server();

  int put(extent_protocol::extentid_t id, std::string, int &);
  int get(extent_protocol::extentid_t id, std::string &);
  int getattr(extent_protocol::extentid_t id, extent_protocol::attr &);
  int remove(extent_protocol::extentid_t id, int &);
};

#endif 







