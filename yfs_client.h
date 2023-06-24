#ifndef yfs_client_h
#define yfs_client_h

#include <string>
//#include "yfs_protocol.h"
#include "extent_client.h"
#include "lock_client_cache.h"
#include <vector>


class yfs_client {
  extent_client *ec;
  lock_client_cache *lc;
 public:

  typedef unsigned long long inum;
  enum xxstatus { OK, RPCERR, NOENT, IOERR, FBIG };
  typedef int status;

  struct fileinfo {
    unsigned long long size;
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirinfo {
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirent {
    std::string name;
    unsigned long long inum;
    dirent() {};
    dirent(std::string name, unsigned long long inum) {
      this->name = name;
      this->inum = inum;
    }
  };
  typedef std::vector<dirent> dirent_lst_t;

 private:
  static std::string filename(inum);
  static inum n2i(std::string);

  std::string serialize(dirent_lst_t lst);
  dirent_lst_t unserialize(std::string serialized);

  int get_all_in_dir(inum parent, dirent_lst_t& dirent_lst);
  int put_all_in_dir(inum parent, dirent_lst_t dirent_lst);

  inum create_random_inum(bool is_dir);

public:

  yfs_client(std::string, std::string);

  bool isfile(inum);
  bool isdir(inum);
  inum ilookup(inum di, std::string name);

  int getfile(inum, fileinfo &);
  int getdir(inum, dirinfo &);

  int create(inum parent, const char *name, int is_dir, inum& inum);
  int lookup(inum parent, const char *name, inum& inum);
  int readdir(inum parent, dirent_lst_t& dirent_lst);
  int read(inum inum, off_t offset, size_t size, std::string& data);
  int write(inum inum, off_t offset, size_t size, std::string data);
  int resize(inum inum, int size);
  int unlink(inum parent, const char *name);

  void acquire_lock(inum inum);
  void release_lock(inum inum);
};

#endif 
