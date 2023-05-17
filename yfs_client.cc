// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>


yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
  ec = new extent_client(extent_dst);

}

yfs_client::inum
yfs_client::n2i(std::string n)
{
  std::istringstream ist(n);
  unsigned long long finum;
  ist >> finum;
  return finum;
}

std::string
yfs_client::filename(inum inum)
{
  std::ostringstream ost;
  ost << inum;
  return ost.str();
}

bool
yfs_client::isfile(inum inum)
{
  if(inum & 0x80000000)
    return true;
  return false;
}

bool
yfs_client::isdir(inum inum)
{
  return ! isfile(inum);
}

int
yfs_client::getfile(inum inum, fileinfo &fin)
{
  int r = OK;


  printf("getfile %016llx\n", inum);
  extent_protocol::attr a;
  if (ec->getattr(inum, a) != extent_protocol::OK) {
    r = IOERR;
    goto release;
  }

  fin.atime = a.atime;
  fin.mtime = a.mtime;
  fin.ctime = a.ctime;
  fin.size = a.size;
  printf("getfile %016llx -> sz %llu\n", inum, fin.size);

 release:

  return r;
}

int
yfs_client::getdir(inum inum, dirinfo &din)
{
  int r = OK;


  printf("getdir %016llx\n", inum);
  extent_protocol::attr a;
  if (ec->getattr(inum, a) != extent_protocol::OK) {
    r = IOERR;
    goto release;
  }
  din.atime = a.atime;
  din.mtime = a.mtime;
  din.ctime = a.ctime;

 release:
  return r;
}

int
yfs_client::read(inum ino, size_t size, off_t offset, std::string &data){
  std::string buf;
  if(ec->get(ino, buf) != extent_protocol::OK){
    return yfs_client::IOERR;
  }
  if(offset < 0) {
    data = buf.substr(0, size);
  } else {
    if (offset+size > buf.size()) {
      data = buf.substr(offset);
      data.resize(size, '\0');
    } else {
      data = buf.substr(offset, size);
    }
  }
  return yfs_client::OK;
}

int
yfs_client::write(inum ino, off_t offset, std::string &data){
  std::string buf;
  if(ec->get(ino, buf) != extent_protocol::OK){
    return yfs_client::IOERR;
  }
  size_t offset_size = (size_t) offset;
  if(offset_size > buf.size()){
    buf.resize(offset);
    buf.append(data);
  } else {
    buf.replace(offset, data.size(), data);
  }
  ec->put(ino, buf);
  return yfs_client::OK;
}

int 
yfs_client::createnode(inum parent, std::string name, inum &newinum, bool isdirectory){
  std::string serializeddirectory;
  if (!isdir(parent) || ec->get(parent, serializeddirectory) != extent_protocol::OK) {
    return yfs_client::IOERR;
  }
  auto directory = unserializedirectoryentries(serializeddirectory);
  if (directory.find(name) == directory.end()) {
    newinum = getnewinum(isdirectory);
    if (ec->put(newinum, std::string("")) != extent_protocol::OK) {
      return yfs_client::IOERR;
    }
    directory[name] = newinum;
    if (ec->put(parent, serializedirectoryentries(directory)) != extent_protocol::OK) {
      return yfs_client::IOERR;
    }
    return yfs_client::OK;
  }
  if(isdirectory){
    return yfs_client::IOERR;
  }
  newinum = directory[name];
  return yfs_client::OK;
}

int
yfs_client::lookupino(inum parent, std::string name, inum &i) {
    std::string data;
    if (ec->get(parent, data) != extent_protocol::OK) {
      return yfs_client::IOERR;
    }
    auto directory = unserializedirectoryentries(data);
    auto file = directory.find(name);
    if (file == directory.end()) {
      return yfs_client::IOERR;
    }
    i = file->second;
    return yfs_client::OK;
}

int
yfs_client::unlink(inum parent, std::string unlinkeditem) {
  inum fileinum;
  std::string data;
  if (ec->get(parent, data) != extent_protocol::OK) {
    return yfs_client::IOERR;
  }
  auto dirdata = unserializedirectoryentries(data);
  auto e = dirdata.find(unlinkeditem);
  if (e == dirdata.end() || isdir(e->second)) {
    return yfs_client::IOERR;
  }
  fileinum = e->second;
  dirdata.erase(e);
  if (ec->put(parent, serializedirectoryentries(dirdata)) != extent_protocol::OK) {
    return yfs_client::IOERR;
  }
  ec->remove(fileinum);
  return yfs_client::OK;
}

int
yfs_client::getdirdata(inum dir, std::string &data) {
  return ec->get(dir, data);
}