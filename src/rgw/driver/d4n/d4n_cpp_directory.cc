#include <errno.h>
#include <cpp_redis/cpp_redis>
#include "driver/d4n/d4n_cpp_directory.h"
#include <string>
#include <iostream>
#include <sstream>
#include <algorithm>
#include <vector>
#include <list>
#define dout_subsys ceph_subsys_rgw

namespace rgw { namespace d4n {

using namespace std;

void RGWBlockDirectory::findClient(std::string key, cpp_redis::client *client){

  std::string address = cct->_conf->rgw_filter_address;
  std::string host = address.substr(0, address.find(":"));
  size_t port = std::stoul(address.substr(address.find(":") + 1, address.length()));

  try {
	  client->connect(host, port, nullptr , 5000, 10, 1000);

  }  catch(exception &e) {
	ldout(cct,10) << __func__ <<"Redis client connected failed with " << e.what()<< dendl;

  }

}


std::string RGWBlockDirectory::buildIndex(CacheBlockCpp *ptr){
  return ptr->cacheObj.bucketName + "_" + ptr->cacheObj.objName + "_" + std::to_string(ptr->blockID) + "_" + std::to_string(ptr->size);
}

int RGWBlockDirectory::setValue(CacheBlockCpp *ptr){

  //creating the index based on bucket_name, obj_name, and chunk_id
  std::string key = buildIndex(ptr);
  cpp_redis::client client;
  findClient(key, &client);
  if (!(client.is_connected())){
	return -1;
  }
  std::string result;
  std::string endpoint;
  std::string local_host = cct->_conf->rgw_local_cache_address;
  int exist = 0;
  vector<std::string> keys;
  keys.push_back(key);
  try{
    client.exists(keys, [&exist](cpp_redis::reply &reply){
	 	if (reply.is_integer()){
		  exist = reply.as_integer();
		}
	});
	client.sync_commit(std::chrono::milliseconds(1000));
  }
  catch(exception &e) {
    exist = 0;
  }
	if (!exist) {
	ldout(cct,10) <<__func__<<" not in directory key:  " << key <<  dendl;
	vector<pair<std::string, std::string>> list;
	//creating a list of key's properties
	list.push_back(make_pair("blockID", std::to_string(ptr->blockID)));
	list.push_back(make_pair("version", ptr->version));
	list.push_back(make_pair("size", std::to_string(ptr->size)));
	list.push_back(make_pair("bucket_name", ptr->cacheObj.bucketName));
	list.push_back(make_pair("obj_name", ptr->cacheObj.objName));
	list.push_back(make_pair("creationTime", ptr->cacheObj.creationTime));
	list.push_back(make_pair("dirty", std::to_string(ptr->cacheObj.dirty)));
	list.push_back(make_pair("globalWeight", std::to_string(ptr->globalWeight)));

    for (auto const& host : ptr->hostsList) {
      if (endpoint.empty())
        endpoint = host + "_";
      else
        endpoint = endpoint + host + "_";
    }

    if (!endpoint.empty())
      endpoint.pop_back();

	list.push_back(make_pair("blockHosts", endpoint));
	//list.push_back(make_pair("accessCount", to_string(ptr->access_count)));
	client.hmset(key, list, [&result](cpp_redis::reply &reply){
		if (!reply.is_null())
		result = reply.as_string();
		});
	client.sync_commit(std::chrono::milliseconds(1000));
	if (result.find("OK") != std::string::npos)
	  ldout(cct,10) <<__func__<<" new key res  " << result <<dendl;
	else
	  ldout(cct,10) <<__func__<<" else key res  " << result <<dendl;
	return 0;
    
  } else { 
	std::string old_val;
	std::vector<std::string> fields;
	fields.push_back("blockHosts");
	try {
	  client.hmget(key, fields, [&old_val](cpp_redis::reply &reply){
		  if (reply.is_array()){
			auto arr = reply.as_array();
			if (!arr[0].is_null())
			  old_val = arr[0].as_string();
	  }});
	  client.sync_commit(std::chrono::milliseconds(1000));
	}
	catch(exception &e) {
	  return 0;
	}
  
	if (old_val.find(local_host) == std::string::npos){
	  std::string hosts = old_val +"_"+ endpoint;
	  vector<pair<std::string, std::string>> list;
	  list.push_back(make_pair("blockHosts", hosts));
	  client.hmset(key, list, [&result](cpp_redis::reply &reply){});
	  client.sync_commit(std::chrono::milliseconds(1000));
	  
	  std::string old_val;
	  std::vector<std::string> fields;
	  fields.push_back("hosts");
	  client.hmget(key, fields, [&old_val](cpp_redis::reply &reply){
          if (reply.is_array()){
            auto arr = reply.as_array();
            if (!arr[0].is_null())
              old_val = arr[0].as_string();
      }});
	  client.sync_commit(std::chrono::milliseconds(1000));
	    ldout(cct,10) <<__func__<<" after hmset " << key << " updated hostslist: " << old_val <<dendl;
	
	}
	return 0;
  }
}

} } 
