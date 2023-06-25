// RPC stubs for clients to talk to extent_server

#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <time.h>

// The calls assume that the caller holds a lock on the extent

extent_client::extent_client(std::string dst)
{
  pthread_mutex_init(&mutex_lock, NULL);
  sockaddr_in dstsock;
	make_sockaddr(dst.c_str(), &dstsock);
  cl = new rpcc(dstsock);
  if (cl->bind() != 0) {
    printf("extent_client: bind failed\n");
  }
}

extent_protocol::status
extent_client::get(extent_protocol::extentid_t eid, std::string &buf)
{
  pthread_mutex_lock(&mutex_lock);
  
  extent_protocol::status ret = extent_protocol::OK;
  
  // cache hit
  if (data.find(eid) != data.end()) {
    
    // not to be removed
    if(!to_be_removed[eid]){
      buf = data[eid];
    
    // to be removed
    } else {
      ret = extent_protocol::NOENT;
    }
  
  // cache miss
  } else {
    
    // retrieve the data from the server
    ret = cl->call(extent_protocol::get, eid, buf);
    data[eid] = buf;
    
    extent_protocol::attr &attr = attributes[eid];
    ret = cl->call(extent_protocol::getattr, eid, attr);

    is_dirty[eid] = false;
    to_be_removed[eid] = false;
  }
  
  attributes[eid].atime = time(nullptr);
  
  pthread_mutex_unlock(&mutex_lock);
  
  return ret;
}

extent_protocol::status
extent_client::getattr(extent_protocol::extentid_t eid, 
		       extent_protocol::attr &attr)
{
  pthread_mutex_lock(&mutex_lock);

  extent_protocol::status ret = extent_protocol::OK;

  // cache hit
  if (attributes.find(eid) != attributes.end()){

    // not to be removed
    if(!to_be_removed[eid]) {
      attr = attributes.at(eid);

    // to be removed
    } else {
      ret = extent_protocol::NOENT;
    }

  // cache miss 
  } else {
      
      // retrieve the attributes from the server
      extent_protocol::attr &a = attributes[eid];
      ret = cl->call(extent_protocol::getattr, eid, a);
      attr = a;
  }
  
  pthread_mutex_unlock(&mutex_lock);
  
  return ret;
}

extent_protocol::status
extent_client::put(extent_protocol::extentid_t eid, std::string buf)
{
  pthread_mutex_lock(&mutex_lock);
  
  extent_protocol::status ret = extent_protocol::OK;

  // insert data
  data[eid] = buf;

  // create and set attributes
  extent_protocol::attr newAttr;
  
  newAttr.size = buf.size();
  
  time_t currTime = time(nullptr);
  
  newAttr.atime = currTime;
  newAttr.mtime = currTime;
  newAttr.ctime = currTime;
  
  attributes[eid] = newAttr;

  // set dirty flag
  is_dirty[eid] = true;
  to_be_removed[eid] = false;

  pthread_mutex_unlock(&mutex_lock);
  
  return ret;
}

extent_protocol::status
extent_client::remove(extent_protocol::extentid_t eid)
{
  pthread_mutex_lock(&mutex_lock);
  
  extent_protocol::status ret = extent_protocol::OK;

  // set to be removed flag
  to_be_removed[eid] = true;

  pthread_mutex_unlock(&mutex_lock);
  
  return ret;
}

extent_protocol::status
extent_client::flush(extent_protocol::extentid_t eid) {
    pthread_mutex_lock(&mutex_lock);

    extent_protocol::status ret = extent_protocol::OK;
    
    int r;

    // cache hit
    if (data.find(eid) != data.end()) {
        if (to_be_removed[eid]) {
            while (cl->call(extent_protocol::remove, eid, r) != extent_protocol::OK);
        } else if (is_dirty[eid]) {
            while (cl->call(extent_protocol::put, eid, data[eid], r) != extent_protocol::OK);
        }

        data.erase(eid);
        attributes.erase(eid);
        to_be_removed.erase(eid);
        is_dirty.erase(eid);
    }

    pthread_mutex_unlock(&mutex_lock);
    
    return ret;
}