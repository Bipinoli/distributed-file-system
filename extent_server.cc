// the extent server implementation

#include "extent_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

extent_server::extent_server() {
  // root folder with id 1 need to exist
  int t;
  put(1, std::string(""), t);
}


int extent_server::put(extent_protocol::extentid_t id, std::string buf, int &)
{
  printf("extent_server: put id %016llx buffer %s\n", id, buf.c_str());
  extent_t extent;
  extent.data = buf;
  extent.attr.size = buf.size();
  extent.attr.atime = extent.attr.mtime = extent.attr.ctime = time(NULL);
  files[id] = extent;
  return extent_protocol::OK;
}

int extent_server::get(extent_protocol::extentid_t id, std::string &buf)
{
  printf("extent_server: get id %016llx\n", id);
  if (files.find(id) == files.end()) {
    printf("ERROR! extent_server: get id %016llx not found\n", id);
    return extent_protocol::NOENT;
  }
  extent_t& extent = files[id];
  buf = extent.data;
  extent.attr.atime = time(NULL);
  return extent_protocol::OK;
}

int extent_server::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a)
{
  printf("extent_server: getattr id %016llx\n", id);
  if (files.find(id) == files.end()) {
    printf("ERROR! extent_server: getattr id %016llx not found\n", id);
    return extent_protocol::NOENT;
  }
  extent_t extent = files[id];
  a.size = extent.attr.size;
  a.atime = extent.attr.atime;
  a.mtime = extent.attr.mtime;
  a.ctime = extent.attr.ctime;
  return extent_protocol::OK;
}

int extent_server::remove(extent_protocol::extentid_t id, int &)
{
  printf("extent_server: remove id %016llx\n", id);
  auto it = files.find(id);
  if (it == files.end()) {
    printf("ERROR! extent_server: remove id %016llx not found\n", id);
    return extent_protocol::NOENT;
  }
  files.erase(it);
  return extent_protocol::OK;
}

