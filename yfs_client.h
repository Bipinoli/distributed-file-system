#ifndef yfs_client_h
#define yfs_client_h

#include <string>
//#include "yfs_protocol.h"
#include "extent_client.h"
#include <vector>


  class yfs_client {
  extent_client *ec;
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
  };

 private:
  static std::string filename(inum);
  static inum n2i(std::string);

  inum getnewinum(bool isdirectory){
    inum newinum = std::rand();
    if(isdirectory){
      newinum &= 0x7FFFFFFF;
    } else {
      newinum |= 0x80000000;
    }
    return newinum;
  }

  // http://stackoverflow.com/questions/14265581/parse-split-a-string-in-c-using-string-delimiter-standard-c
  static std::vector <std::string>
  split(std::string s, std::string delimiter) {
    size_t pos = 0;
    std::vector<std::string> output;
    std::string token;
    while ((pos = s.find(delimiter)) != std::string::npos) {
      token = s.substr(0, pos);
      output.push_back(token);
      s.erase(0, pos + delimiter.length());
    }
    output.push_back(s);
    return output;
  }

 public:

  yfs_client(std::string, std::string);

  bool isfile(inum);
  bool isdir(inum);
  inum ilookup(inum di, std::string name);

  int getfile(inum, fileinfo &);
  int getdir(inum, dirinfo &);

  int lookupino(inum, std::string, inum &);
  int createnode(inum parent, std::string name, inum &newInum, bool isDirectory);
  int getdirdata(inum dir, std::string &data);
  int write(inum, off_t off, std::string &data);
  int read(inum, size_t size, off_t off, std::string &data);
  int unlink(inum parent, std::string unlinkeditem);

  std::string
  serializedirectoryentries(std::map <std::string, yfs_client::inum> directory) {
    std::string result = "";
    for (auto const &iterator : directory) {
      std::string name = iterator.first;
      inum inum = iterator.second;
      result += name + std::string(";") + filename(inum) + std::string("\n");
    }
    return result;
  }

  std::map <std::string, yfs_client::inum>
  unserializedirectoryentries(std::string serializedDirectory) {
    std::map <std::string, yfs_client::inum> directories;
    for (const std::string &row : split(serializedDirectory, "\n")) {
      if (!row.empty()) {
        std::vector<std::string> entry = split(row, ";");
        std::string name = entry.at(0);
        inum inum = n2i(entry.at(1));
        directories[name] = inum;
      }
    }
    return directories;
  }
};

#endif 
