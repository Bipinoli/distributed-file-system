// the extent server implementation

#include "extent_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

extent_server::extent_server() {}


int extent_server::put(extent_protocol::extentid_t id, std::string buf, int &)
{
  extent_protocol::attr file_attr;
  file_attr.size = buf.size();
  file_attr.atime = time(NULL);
  file_attr.mtime = time(NULL);
  file_attr.ctime = time(NULL);
  storage[id] = std::make_pair(buf, file_attr);
  return extent_protocol::OK;
}

int extent_server::get(extent_protocol::extentid_t id, std::string &buf)
{
  if(storage.find(id) == storage.end()){
    return extent_protocol::NOENT;
  }
  storage[id].second.atime = time(NULL);
  buf = storage[id].first;
  return extent_protocol::OK;
}

int extent_server::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a)
{
  // You replace this with a real implementation. We send a phony response
  // for now because it's difficult to get FUSE to do anything (including
  // unmount) if getattr fails.
  if(storage.find(id) == storage.end()){
    return extent_protocol::NOENT;
  }
  a = storage[id].second;
  return extent_protocol::OK;
}

int extent_server::remove(extent_protocol::extentid_t id, int &)
{
  if(storage.find(id) == storage.end()){
    return extent_protocol::NOENT;
  }
  storage.erase(id);
  return extent_protocol::OK;
}

