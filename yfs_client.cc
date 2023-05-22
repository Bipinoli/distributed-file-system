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


std::string yfs_client::serialize(dirent_lst_t lst) {
  // encode as 'inum:folder1/folder2/../filename' separated by \n
  std::string retval;
  for (auto it: lst) {
    retval += "" + filename(it.inum) + ":" + it.name + "\n";
  }
  return retval;
}

yfs_client::dirent_lst_t yfs_client::unserialize(std::string serialized) {
  yfs_client::dirent_lst_t retval;
  std::stringstream ss(serialized);
  std::string part;
  while (getline(ss, part, '\n')) {
    // id:filename
    int pos_of_delim;
    for (int i=0; i<part.size(); i++) {
      if (part[i] == ':') {
        pos_of_delim = i;
        break;
      }
    }
    unsigned long long inum = n2i(part.substr(0, pos_of_delim));
    std::string name = part.substr(pos_of_delim+1, part.size());
    dirent ent = dirent(name, inum);
    retval.push_back(ent);
  }
  return retval;
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


int yfs_client::get_all_in_dir(yfs_client::inum parent, dirent_lst_t& dirent_lst) {
  if (!isdir(parent))
    return IOERR;
  std::string serialized;
  if (ec->get(parent, serialized) != OK) {
    return NOENT;
  }
  dirent_lst = yfs_client::unserialize(serialized);
  return OK;
}


int yfs_client::put_all_in_dir(inum parent, dirent_lst_t dirent_lst) {
  if (!isdir(parent))
    return IOERR;
  std::string searilized = yfs_client::serialize(dirent_lst);
  return ec->put(parent, searilized);
}


yfs_client::inum yfs_client::create_random_inum(bool is_dir) {
  unsigned long long upper32bits = rand();
  unsigned long long lower32bits = rand();
  unsigned long long random64bits = ((upper32bits << 32) | lower32bits);
  if (!is_dir) {
    // file: set the 32nd bit to 1
    return random64bits | 0x80000000;
  }
  // dir: set the 32nd bit to 0
  return random64bits & (~(1L << 31));
}


int yfs_client::create(inum parent, const char *name, int is_dir, inum &inum) {
  // 1. save new file/folder as a node
  inum = create_random_inum(is_dir);
  auto put_ret = ec->put(inum, name);
  if (put_ret != OK) {
    printf("ERROR! yfs_client::create put_ret failed! inum = %016llx name = %s\n\n", inum, name);
    return put_ret;
  }

  // 2. update the parent directory to reflect the new file
  dirent_lst_t dirent_lst;
  auto all_dir_ret = get_all_in_dir(parent, dirent_lst);
  if (all_dir_ret != OK) {
    printf("ERROR! yfs_client::create get_all_in_dir failed! parent = %016llx\n\n", parent);
    return all_dir_ret;
  }
  dirent_lst.push_back(dirent(name, inum));
  auto put_all_ret = put_all_in_dir(parent, dirent_lst);
  if (put_all_ret != OK) {
    printf("ERROR! yfs_client::create put_all_in_dir failed! parent = %016llx\n\n", parent);
    return put_all_ret;
  }

  return OK;
}


int yfs_client::lookup(inum parent, const char *name, inum &inum) {
  dirent_lst_t dirent_lst;
  auto ret = get_all_in_dir(parent, dirent_lst);
  if (ret != OK) {
    printf("ERROR! yfs_client::lookup get_all_in_dir failed! parent = %016llx\n\n", parent);
    return ret;
  }
  for (auto it: dirent_lst) {
    if (it.name == name) {
      inum = it.inum;
      return OK;
    }
  }
  return NOENT;
}


int yfs_client::readdir(inum parent, dirent_lst_t& dirent_lst) {
  auto ret = get_all_in_dir(parent, dirent_lst);
  if (ret != OK) {
    printf("ERROR! yfs_client::readdir get_all_in_dir failed! parent = %016llx\n\n", parent);
    return ret;
  }
  return OK;
}


int yfs_client::read(inum inum, off_t offset, size_t size, std::string& data) {
  std::string content;
  auto ret = ec->get(inum, content);
  if (ret != OK) {
    printf("ERROR! yfs_client::read ec->get failed! inum = %016llx\n\n", inum);
    return ret;
  }
  if (offset + size <= content.size()) {
    data = content.substr(offset + size);
  } else {
    data = content;
    data.resize(size, '\0');
  }
  return OK;
}


int yfs_client::write(inum inum, off_t offset, std::string data) {
  std::string content;
  auto ret = ec->get(inum, content);
  if (ret != OK) {
    printf("ERROR! yfs_client::write ec->get failed! inum = %016llx\n\n", inum);
    return ret;
  }
  // need to pad with \0 if still less than offset
  std::string new_content = content.substr(0, offset);
  new_content.resize(offset, '\0');
  new_content += data;
  auto put_ret = ec->put(inum, new_content);
  if (put_ret != OK) {
    printf("ERROR! yfs_client::write ec->put failed! inum = %016llx\n\n", inum);
    return put_ret;
  }
  return OK;
}


int yfs_client::resize(inum inum, int size) {
  std::string content;
  auto ret = ec->get(inum, content);
  if (ret != OK) {
    printf("ERROR! yfs_client::resize ec->get failed! inum = %016llx\n\n", inum);
    return ret;
  }
  content.resize(size, '\0');
  auto put_ret = ec->put(inum, content);
  if (put_ret != OK) {
    printf("ERROR! yfs_client::resize ec->put failed! inum = %016llx\n\n", inum);
    return put_ret;
  }
  return OK;
}