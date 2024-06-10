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

static const uint16_t crc16tab[256]= {
  0x0000,0x1021,0x2042,0x3063,0x4084,0x50a5,0x60c6,0x70e7,
  0x8108,0x9129,0xa14a,0xb16b,0xc18c,0xd1ad,0xe1ce,0xf1ef,
  0x1231,0x0210,0x3273,0x2252,0x52b5,0x4294,0x72f7,0x62d6,
  0x9339,0x8318,0xb37b,0xa35a,0xd3bd,0xc39c,0xf3ff,0xe3de,
  0x2462,0x3443,0x0420,0x1401,0x64e6,0x74c7,0x44a4,0x5485,
  0xa56a,0xb54b,0x8528,0x9509,0xe5ee,0xf5cf,0xc5ac,0xd58d,
  0x3653,0x2672,0x1611,0x0630,0x76d7,0x66f6,0x5695,0x46b4,
  0xb75b,0xa77a,0x9719,0x8738,0xf7df,0xe7fe,0xd79d,0xc7bc,
  0x48c4,0x58e5,0x6886,0x78a7,0x0840,0x1861,0x2802,0x3823,
  0xc9cc,0xd9ed,0xe98e,0xf9af,0x8948,0x9969,0xa90a,0xb92b,
  0x5af5,0x4ad4,0x7ab7,0x6a96,0x1a71,0x0a50,0x3a33,0x2a12,
  0xdbfd,0xcbdc,0xfbbf,0xeb9e,0x9b79,0x8b58,0xbb3b,0xab1a,
  0x6ca6,0x7c87,0x4ce4,0x5cc5,0x2c22,0x3c03,0x0c60,0x1c41,
  0xedae,0xfd8f,0xcdec,0xddcd,0xad2a,0xbd0b,0x8d68,0x9d49,
  0x7e97,0x6eb6,0x5ed5,0x4ef4,0x3e13,0x2e32,0x1e51,0x0e70,
  0xff9f,0xefbe,0xdfdd,0xcffc,0xbf1b,0xaf3a,0x9f59,0x8f78,
  0x9188,0x81a9,0xb1ca,0xa1eb,0xd10c,0xc12d,0xf14e,0xe16f,
  0x1080,0x00a1,0x30c2,0x20e3,0x5004,0x4025,0x7046,0x6067,
  0x83b9,0x9398,0xa3fb,0xb3da,0xc33d,0xd31c,0xe37f,0xf35e,
  0x02b1,0x1290,0x22f3,0x32d2,0x4235,0x5214,0x6277,0x7256,
  0xb5ea,0xa5cb,0x95a8,0x8589,0xf56e,0xe54f,0xd52c,0xc50d,
  0x34e2,0x24c3,0x14a0,0x0481,0x7466,0x6447,0x5424,0x4405,
  0xa7db,0xb7fa,0x8799,0x97b8,0xe75f,0xf77e,0xc71d,0xd73c,
  0x26d3,0x36f2,0x0691,0x16b0,0x6657,0x7676,0x4615,0x5634,
  0xd94c,0xc96d,0xf90e,0xe92f,0x99c8,0x89e9,0xb98a,0xa9ab,
  0x5844,0x4865,0x7806,0x6827,0x18c0,0x08e1,0x3882,0x28a3,
  0xcb7d,0xdb5c,0xeb3f,0xfb1e,0x8bf9,0x9bd8,0xabbb,0xbb9a,
  0x4a75,0x5a54,0x6a37,0x7a16,0x0af1,0x1ad0,0x2ab3,0x3a92,
  0xfd2e,0xed0f,0xdd6c,0xcd4d,0xbdaa,0xad8b,0x9de8,0x8dc9,
  0x7c26,0x6c07,0x5c64,0x4c45,0x3ca2,0x2c83,0x1ce0,0x0cc1,
  0xef1f,0xff3e,0xcf5d,0xdf7c,0xaf9b,0xbfba,0x8fd9,0x9ff8,
  0x6e17,0x7e36,0x4e55,0x5e74,0x2e93,0x3eb2,0x0ed1,0x1ef0
};

uint16_t crc16(const char *buf, int len) {
  int counter;
  uint16_t crc = 0;
  for (counter = 0; counter < len; counter++)
	crc = (crc<<8) ^ crc16tab[((crc>>8) ^ *buf++)&0x00FF];
  return crc;
}

unsigned int hash_slot(const char *key, int keylen) {
  int s, e; /* start-end indexes of { and } */

  /* Search the first occurrence of '{'. */
  for (s = 0; s < keylen; s++)
	if (key[s] == '{') break;

  /* No '{' ? Hash the whole key. This is the base case. */
  if (s == keylen) return crc16(key,keylen) & 16383;

  /* '{' found? Check if we have the corresponding '}'. */
  for (e = s+1; e < keylen; e++)
	if (key[e] == '}') break;

  /* No '}' or nothing between {} ? Hash the whole key. */
  if (e == keylen || e == s+1) return crc16(key,keylen) & 16383;

  /* If we are here there is both a { and a } on its right. Hash
   * what is in the middle between { and }. */
  return crc16(key+s+1,e-s-1) & 16383;
}

void RGWObjectDirectory::findClient(std::string key, cpp_redis::client *client){
  int slot = 0;
  ldout(cct,10) <<__func__<<": " << __LINE__ <<  dendl;
  slot = hash_slot(key.c_str(), key.size());
  int dirMasterCount = cct->_conf->rgw_directory_master_count;
  ldout(cct,10) <<__func__<<": " << __LINE__ << ": MasterCount is: " << dirMasterCount << dendl;
  int slotQuota = 16383/dirMasterCount; 
  ldout(cct,10) <<__func__<<": " << __LINE__ <<  dendl;

  std::string address = cct->_conf->rgw_filter_address;
  std::vector<std::string> host;
  std::vector<size_t> port;
  std::stringstream ss(address);
  for (int i = 0; i < dirMasterCount; i ++){ //host1:port1_host2:port2_....
    while (!ss.eof()) {
      std::string tmp;
      std::getline(ss, tmp, '_');
      host.push_back(tmp.substr(0, tmp.find(":")));
      port.push_back(std::stoul(tmp.substr(tmp.find(":") + 1, tmp.length())));
    }
  }
  ldout(cct,10) <<__func__<<": " << __LINE__ <<  dendl;
  cpp_redis::connect_state status;
  try {
    for (int i = 1; i <= dirMasterCount; i++){
      ldout(cct,10) <<__func__<<": " << __LINE__ << ": slot is: " << slot << " slotQuota is: " << slotQuota << dendl;
      if (slot < (slotQuota*i)){
        ldout(cct,10) <<__func__<<": " << __LINE__ <<  dendl;
	//client->connect(host[i-1], port[i-1], nullptr,   500, 10, 1000);
	
	client->connect(host[i-1], port[i-1], 
	               [&status](const std::string &host, std::size_t port, cpp_redis::connect_state statusC) {
			       if (statusC == cpp_redis::connect_state::dropped) {
				 status = statusC;
			       }
	               });
	
        if (status == cpp_redis::connect_state::dropped)
	  ldout(cct, 10) << "AMIN:DEBUG client disconnected from " << host[i-1] << ":" << port[i-1] << dendl;
	
	break;
      }
    }
  }
  catch(std::exception &e) {
    ldout(cct,10) << __func__ <<"Redis client connected failed!" << dendl;
  }
}

void RGWBlockDirectory::findClient(std::string key, cpp_redis::client *client){
  int slot = 0;
  ldout(cct,10) <<__func__<<": " << __LINE__ <<  dendl;
  slot = hash_slot(key.c_str(), key.size());
  int dirMasterCount = cct->_conf->rgw_directory_master_count;
  int slotQuota = 16383/dirMasterCount; 
  ldout(cct,10) <<__func__<<": " << __LINE__ <<  dendl;

  std::string address = cct->_conf->rgw_filter_address;
  std::vector<std::string> host;
  std::vector<size_t> port;
  std::stringstream ss(address);
  for (int i = 0; i < dirMasterCount; i ++){ //host1:port1_host2:port2_....
    while (!ss.eof()) {
      std::string tmp;
      std::getline(ss, tmp, '_');
      host.push_back(tmp.substr(0, tmp.find(":")));
      port.push_back(std::stoul(tmp.substr(tmp.find(":") + 1, tmp.length())));
    }
  }

  ldout(cct,10) <<__func__<<": " << __LINE__ <<  dendl;
  try {
    for (int i = 1; i <= dirMasterCount; i++){
      if (slot < (slotQuota*i)){
	client->connect(host[i-1], port[i-1], nullptr,   500, 10, 1000);
 	ldout(cct,10) <<__func__<<": " << __LINE__ <<  dendl;
	break;
      }
    }
  }
  catch(std::exception &e) {
    ldout(cct,10) << __func__ <<"Redis client connected failed!" << dendl;
  }
  ldout(cct,10) <<__func__<<": " << __LINE__ <<  dendl;
}

std::string RGWObjectDirectory::buildIndex(CacheObjectCpp *ptr){
  return ptr->bucketName + "_" + ptr->objName;
}


std::string RGWBlockDirectory::buildIndex(CacheBlockCpp *ptr){
  return ptr->cacheObj.bucketName + "_" + ptr->cacheObj.objName + "_" + std::to_string(ptr->blockID) + "_" + std::to_string(ptr->size);
}

int RGWObjectDirectory::exist_key(CacheObjectCpp *ptr){

  int result = 0;

  std::string key = buildIndex(ptr);
  ldout(cct,10) << __func__ << "  key " << key << dendl;
  cpp_redis::client client;
  findClient(key, &client);
  if (!(client.is_connected())){
    ldout(cct,10) << __func__ << ": " << __LINE__ << " Redis client is not connected!" << dendl;
    return -1;
  }
  try {
    std::vector<std::string> keys;
    keys.push_back(key);
    client.exists(keys, [&result](cpp_redis::reply &reply){
      if (reply.is_integer())
        result = reply.as_integer();
    });
    client.sync_commit(std::chrono::milliseconds(300));
    ldout(cct,10) << __func__ << ": " << __LINE__ << dendl;
  }
  catch(std::exception &e) {
    result =  0;
    ldout(cct,10) << __func__ << ": " << __LINE__ << ": key " << key <<" result:" <<result <<dendl;
  }
  
  ldout(cct,10) << __func__ << ": " << __LINE__ << ": key " << key <<" result:" <<result <<dendl;
  return result;
}

int RGWBlockDirectory::exist_key(CacheBlockCpp *ptr)
{
  int result = 0;
  std::string key = buildIndex(ptr);
  cpp_redis::client client;
  findClient(key, &client);
  if (!(client.is_connected())){
	return -1;
  }

  try {
    std::vector<std::string> keys;
    keys.push_back(key);
    client.exists(keys, [&result](cpp_redis::reply &reply){
      if (reply.is_integer())
	 result = reply.as_integer();
    });
    client.sync_commit(std::chrono::milliseconds(300));
    ldout(cct,10) << __func__ << " res dir " << result << " key " << key <<  dendl;
  }
  catch(std::exception &e) {
    ldout(cct,10) << __func__ << " Error" << " key " << key << dendl;
  }
  
  return result;
}

int RGWObjectDirectory::set(CacheObjectCpp *ptr, optional_yield y)
{
  //creating the index based on bucket_name, obj_name, and chunk_id
  ldout(cct,10) <<__func__<<": " << __LINE__ <<  dendl;
  std::string key = buildIndex(ptr);
  ldout(cct,10) <<__func__<<": " << __LINE__ << " key is: " << key <<  dendl;
  cpp_redis::client client;
  findClient(key, &client);
  if (!(client.is_connected())){
	return -1;
  }
  ldout(cct,10) <<__func__<<": " << __LINE__ <<  dendl;
  std::string result;
  std::string endpoint;
  std::string local_host = cct->_conf->rgw_local_cache_address;
  int exist = 0;
  ldout(cct,10) <<__func__<<": " << __LINE__ <<  dendl;

  std::vector<std::string> keys;
  keys.push_back(key);
  try{
    client.exists(keys, [&exist](cpp_redis::reply &reply){
	 	if (reply.is_integer()){
		  exist = reply.as_integer();
		}
	});
	client.sync_commit(std::chrono::milliseconds(300));
  }
  catch(std::exception &e) {
    exist = 0;
  }
  ldout(cct,10) <<__func__<<": " << __LINE__ <<  dendl;
	
  if (!exist) {
    ldout(cct,10) <<__func__<<" not in directory key:  " << key <<  dendl;

    std::vector<std::pair<std::string, std::string>> list;

    list.push_back(std::make_pair("bucketName", ptr->bucketName));
    list.push_back(std::make_pair("objName", ptr->objName));
    list.push_back(std::make_pair("creationTime", ptr->creationTime));
    list.push_back(std::make_pair("dirty", std::to_string(ptr->dirty)));
    list.push_back(std::make_pair("version", ptr->version));
    list.push_back(std::make_pair("size", std::to_string(ptr->size)));
    list.push_back(std::make_pair("in_lsvd", std::to_string(ptr->in_lsvd)));

    for (auto const& host : ptr->hostsList) {
      if (endpoint.empty())
        endpoint = host + "_";
      else
        endpoint = endpoint + host + "_";
    }
    if (!endpoint.empty())
      endpoint.pop_back();
    list.push_back(std::make_pair("objHosts", endpoint));

    for (auto& it : ptr->attrs) {
      list.push_back(std::make_pair(it.first, it.second.to_str()));
    }

    client.hmset(key, list, [&result](cpp_redis::reply &reply){
      if (!reply.is_null())
 	result = reply.as_string();
    });

    client.sync_commit(std::chrono::milliseconds(300));
    if (result.find("OK") != std::string::npos)
      ldout(cct,10) <<__func__<<" new key res  " << result <<dendl;
    else
      ldout(cct,10) <<__func__<<" else key res  " << result <<dendl;
  } else { 
    std::string old_val;
    std::vector<std::string> fields;
    fields.push_back("objHosts");
    try {
      client.hmget(key, fields, [&old_val](cpp_redis::reply &reply){
        if (reply.is_array()){
	  auto arr = reply.as_array();
	  if (!arr[0].is_null())
	    old_val = arr[0].as_string();
	}
      });
      client.sync_commit(std::chrono::milliseconds(300));
    }
    catch(std::exception &e) {
      return 0;
    }
  
    if (old_val.find(local_host) == std::string::npos){
      std::string hosts = old_val +"_"+ local_host;
      std::vector<std::pair<std::string, std::string>> list;
      list.push_back(std::make_pair("objHosts", hosts));

      client.hmset(key, list, [&result](cpp_redis::reply &reply){
        if (!reply.is_null())
 	  result = reply.as_string();
        });

      client.sync_commit(std::chrono::milliseconds(300));
	  
      ldout(cct,10) <<__func__<<" after hmset " << key << " updated hostslist: " << old_val <<dendl;
    }
  }
  return 0;
}

int RGWBlockDirectory::set(CacheBlockCpp *ptr, optional_yield y)
{
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

  std::vector<std::string> keys;
  keys.push_back(key);
  try{
    client.exists(keys, [&exist](cpp_redis::reply &reply){
	 	if (reply.is_integer()){
		  exist = reply.as_integer();
		}
	});
	client.sync_commit(std::chrono::milliseconds(300));
  }
  catch(std::exception &e) {
    exist = 0;
  }
	
  if (!exist) {
    std::vector<std::pair<std::string, std::string>> list;

    list.push_back(std::make_pair("blockID", std::to_string(ptr->blockID)));
    list.push_back(std::make_pair("version", ptr->version));
    list.push_back(std::make_pair("size", std::to_string(ptr->size)));
    list.push_back(std::make_pair("bucketName", ptr->cacheObj.bucketName));
    list.push_back(std::make_pair("objName", ptr->cacheObj.objName));
    list.push_back(std::make_pair("creationTime", ptr->cacheObj.creationTime));
    list.push_back(std::make_pair("dirty", std::to_string(ptr->cacheObj.dirty)));
    list.push_back(std::make_pair("globalWeight", std::to_string(ptr->globalWeight)));

    for (auto const& host : ptr->hostsList) {
      if (endpoint.empty())
        endpoint = host + "_";
      else
        endpoint = endpoint + host + "_";
    }

    if (!endpoint.empty())
      endpoint.pop_back();

    list.push_back(std::make_pair("blockHosts", endpoint));

    client.hmset(key, list, [&result](cpp_redis::reply &reply){
      if (!reply.is_null())
 	result = reply.as_string();
    });

    client.sync_commit(std::chrono::milliseconds(300));
    if (result.find("OK") != std::string::npos)
      ldout(cct,10) <<__func__<<" new key res  " << result <<dendl;
    else
      ldout(cct,10) <<__func__<<" else key res  " << result <<dendl;
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
	}
      });
      client.sync_commit(std::chrono::milliseconds(300));
    }
    catch(std::exception &e) {
      return 0;
    }
  
    if (old_val.find(local_host) == std::string::npos){
      std::string hosts = old_val +"_"+ local_host;
      std::vector<std::pair<std::string, std::string>> list;
      list.push_back(std::make_pair("blockHosts", hosts));

      client.hmset(key, list, [&result](cpp_redis::reply &reply){
        if (!reply.is_null())
 	  result = reply.as_string();
        });

      client.sync_commit(std::chrono::milliseconds(300));
	  
      ldout(cct,10) <<__func__<<" after hmset " << key << " updated hostslist: " << old_val <<dendl;
    }
  }
  return 0;
}

int RGWObjectDirectory::update_field(CacheObjectCpp *ptr, std::string field, std::string value, optional_yield y)
{
  std::vector<std::pair<std::string, std::string>> list;
  list.push_back(std::make_pair(field, value));

  //creating the index based on bucket_name, obj_name, and chunk_id
  std::string key = buildIndex(ptr);
  cpp_redis::client client;
  findClient(key, &client);
  if (!(client.is_connected())){
	return -1;
  }
  std::string result;
  int exist = 0;
  std::vector<std::string> keys;
  keys.push_back(key);
  try{
    client.exists(keys, [&exist](cpp_redis::reply &reply){
	 	if (reply.is_integer()){
		  exist = reply.as_integer();
		}
	});
	client.sync_commit(std::chrono::milliseconds(300));
  }
  catch(std::exception &e) {
    exist = 0;
  }
	
  if (!exist) 
    return -ENOENT;
  else{
    std::string values;
    std::vector<std::string> fields;
    fields.push_back(field);
    if (field == "objHosts"){ 
      std::string old_val;
      try {
        client.hmget(key, fields, [&old_val](cpp_redis::reply &reply){
          if (reply.is_array()){
	    auto arr = reply.as_array();
	    if (!arr[0].is_null())
	      old_val = arr[0].as_string();
	  }
        });
        client.sync_commit(std::chrono::milliseconds(300));
      }
      catch(std::exception &e) {
        return 0;
      }
      if (old_val.find(value) == std::string::npos){
        values = old_val +"_"+ value;
      }
    }
    else
      values = value;

    std::vector<std::pair<std::string, std::string>> list;
    list.push_back(std::make_pair(field, values));

    client.hmset(key, list, [&result](cpp_redis::reply &reply){
      if (!reply.is_null())
 	result = reply.as_string();
    });
    client.sync_commit(std::chrono::milliseconds(300));	  
  }

  return 0;

}

int RGWBlockDirectory::update_field(CacheBlockCpp *ptr, std::string field, std::string value, optional_yield y)
{
  std::vector<std::pair<std::string, std::string>> list;
  list.push_back(std::make_pair(field, value));

  //creating the index based on bucket_name, obj_name, and chunk_id
  std::string key = buildIndex(ptr);
  cpp_redis::client client;
  findClient(key, &client);
  if (!(client.is_connected())){
	return -1;
  }
  std::string result;
  int exist = 0;
  std::vector<std::string> keys;
  keys.push_back(key);
  try{
    client.exists(keys, [&exist](cpp_redis::reply &reply){
	 	if (reply.is_integer()){
		  exist = reply.as_integer();
		}
	});
	client.sync_commit(std::chrono::milliseconds(300));
  }
  catch(std::exception &e) {
    exist = 0;
  }
	
  if (!exist) 
    return -ENOENT;
  else{
    std::string values;
    std::vector<std::string> fields;
    fields.push_back(field);
    if (field == "blockHosts"){ 
      std::string old_val;
      try {
        client.hmget(key, fields, [&old_val](cpp_redis::reply &reply){
          if (reply.is_array()){
	    auto arr = reply.as_array();
	    if (!arr[0].is_null())
	      old_val = arr[0].as_string();
	  }
        });
        client.sync_commit(std::chrono::milliseconds(300));
      }
      catch(std::exception &e) {
        return 0;
      }
      if (old_val.find(value) == std::string::npos){
        values = old_val +"_"+ value;
      }
    }
    else
      values = value;

    std::vector<std::pair<std::string, std::string>> list;
    list.push_back(std::make_pair(field, values));

    client.hmset(key, list, [&result](cpp_redis::reply &reply){
      if (!reply.is_null())
 	result = reply.as_string();
    });
    client.sync_commit(std::chrono::milliseconds(300));	  
  }

  return 0;
}

int RGWObjectDirectory::del(CacheObjectCpp *ptr, optional_yield y)
{
  int result = 0;
  std::vector<std::string> keys;
  cpp_redis::client client;
  std::string key = buildIndex(ptr);
  keys.push_back(key);
  findClient(key, &client);
  if (!(client.is_connected())){
	return -1;
  }
  try {
	client.del(keys, [&result](cpp_redis::reply &reply){
		if  (reply.is_integer())
		{result = reply.as_integer();}
		});
	client.sync_commit(std::chrono::milliseconds(300));	
	ldout(cct,10) << __func__ << "DONE" << dendl;
	return result-1;
  }
  catch(std::exception &e) {
	ldout(cct,10) << __func__ << " Error" << " key " << key << dendl;
	return -1;
  }
}

int RGWBlockDirectory::del(CacheBlockCpp *ptr, optional_yield y)
{
  int result = 0;
  std::vector<std::string> keys;
  cpp_redis::client client;
  std::string key = buildIndex(ptr);
  keys.push_back(key);
  findClient(key, &client);
  if (!(client.is_connected())){
	return -1;
  }
  try {
	client.del(keys, [&result](cpp_redis::reply &reply){
		if  (reply.is_integer())
		{result = reply.as_integer();}
		});
	client.sync_commit(std::chrono::milliseconds(300));	
	ldout(cct,10) << __func__ << "DONE" << dendl;
	return result-1;
  }
  catch(std::exception &e) {
	ldout(cct,10) << __func__ << " Error" << " key " << key << dendl;
	return -1;
  }
}

int RGWObjectDirectory::get(CacheObjectCpp *ptr, optional_yield y)
{
  ldout(cct,10) <<__func__<<": " << __LINE__ <<  dendl;
  std::string key = buildIndex(ptr);
  ldout(cct,10) <<__func__<<": " << __LINE__ << " key is: " << key <<  dendl;
  cpp_redis::client client;
  findClient(key, &client);
  if (!(client.is_connected())){
	return -1;
  }
  ldout(cct,10) <<__func__<<": " << __LINE__ <<  dendl;
  std::string result;
  int exist = 0;
  ldout(cct,10) <<__func__<<": " << __LINE__ <<  dendl;

  std::vector<std::string> keys;
  keys.push_back(key);
    
  client.exists(keys, [&exist](cpp_redis::reply &reply){
	 	if (reply.is_integer()){
		  exist = reply.as_integer();
		}
	});
	client.sync_commit(std::chrono::milliseconds(300));

  ldout(cct,10) <<__func__<<": " << __LINE__ <<  ": exist: " << exist << dendl;
	
  if (exist) {
  //if (exist_key(ptr)){
    ldout(cct,10) << __func__ << ": " << __LINE__<< ": object: "<< key << " exists in directory!" << dendl;
    try{
	std::string obj_name;
	std::string bucket_name;
	std::string creationTime;
	std::string dirty;
	std::string version;
	std::string size;
	std::string in_lsvd;
	std::string objHosts;
	std::string attrs;

	//fields will be filled by the redis hmget functoin
	std::vector<std::string> fields;
	fields.push_back("bucketName");
	fields.push_back("objName");
	fields.push_back("creationTime");
	fields.push_back("dirty");
	fields.push_back("version");
	fields.push_back("size");
	fields.push_back("in_lsvd");
	fields.push_back("objHosts");
	fields.push_back(RGW_ATTR_ACL);

	client.hmget(key, fields, [&bucket_name, &obj_name, &creationTime, &dirty, &version, &size, &in_lsvd, &objHosts, &attrs](cpp_redis::reply& reply){
	  if (reply.is_array()){
	    auto arr = reply.as_array();
	    if (!arr[0].is_null()){
    	      bucket_name = arr[0].as_string();
	      obj_name = arr[1].as_string();
	      creationTime = arr[2].as_string();
	      dirty = arr[3].as_string();
	      version = arr[4].as_string();
 	      size = arr[5].as_string();
	      in_lsvd = arr[6].as_string();
  	      objHosts = arr[7].as_string();
	      attrs = arr[8].as_string();
	    }
	  }
	});

  ldout(cct,10) << __func__ << ": " << __LINE__<< ": object: "<< key << dendl;
	client.sync_commit(std::chrono::milliseconds(300));
  ldout(cct,10) << __func__ << ": " << __LINE__<< ": object: "<< key << dendl;
	  
	std::stringstream sloction(objHosts);
	std::string tmp;
  ldout(cct,10) << __func__ << ": " << __LINE__<< ": object: "<< key << dendl;
	
	ptr->objName = obj_name;
	ptr->bucketName = bucket_name;
	ptr->creationTime = creationTime;
	ptr->dirty = boost::lexical_cast<bool>(dirty);
	//host1_host2_host3_...
	while(getline(sloction, tmp, '_'))
	  ptr->hostsList.push_back(tmp);
  ldout(cct,10) << __func__ << ": " << __LINE__<< ": object: "<< key << dendl;

	ptr->version = version;
	ptr->size = std::stoull(size);
	ptr->in_lsvd = boost::lexical_cast<bool>(in_lsvd);
	ptr->attrs[RGW_ATTR_ACL] = buffer::list::static_from_string(attrs);
        ldout(cct,10) << __func__ << ": " << __LINE__<< ": objName: "<< obj_name << dendl;
        ldout(cct,10) << __func__ << ": " << __LINE__<< ": version: "<< version << dendl;
        ldout(cct,10) << __func__ << ": " << __LINE__<< ": size: "<< size << dendl;
    }
    catch(std::exception &e) {
      ldout(cct,10) << __func__ << " Error" << " key " << key << dendl;
	return -EINVAL;
    }
  }
  else{
  ldout(cct,10) << __func__ << ": " << __LINE__<< ": object: "<< key << dendl;
    return -ENOENT;  
  }
  ldout(cct,10) << __func__ << ": " << __LINE__<< ": object: "<< key << dendl;
  return 0;
}

int RGWBlockDirectory::get(CacheBlockCpp *ptr, optional_yield y)
{
  cpp_redis::client client;
  std::string key = buildIndex(ptr);
  ldout(cct,10) << __func__ << " object in func getValue "<< key << dendl;

  findClient(key, &client);
  if (!(client.is_connected())){
	return -1;
  }
  ldout(cct,10) <<__func__<<": " << __LINE__ <<  dendl;
  std::string result;
  int exist = 0;
  ldout(cct,10) <<__func__<<": " << __LINE__ <<  dendl;

  std::vector<std::string> keys;
  keys.push_back(key);
    
  client.exists(keys, [&exist](cpp_redis::reply &reply){
	 	if (reply.is_integer()){
		  exist = reply.as_integer();
		}
	});
	client.sync_commit(std::chrono::milliseconds(300));

  ldout(cct,10) <<__func__<<": " << __LINE__ <<  ": exist: " << exist << dendl;
	
  if (exist) {
  //if (exist_key(ptr)){
    try{
	std::string blockID;
	std::string version;
	std::string size;
	std::string globalWeight;
	std::string blockHosts;

	std::string obj_name;
	std::string bucket_name;
	std::string creationTime;
	std::string dirty;

	//fields will be filled by the redis hmget functoin
	std::vector<std::string> fields;

        fields.push_back("blockID");
        fields.push_back("version");
        fields.push_back("size");
	fields.push_back("bucketName");
	fields.push_back("objName");
	fields.push_back("creationTime");
	fields.push_back("dirty");
        fields.push_back("globalWeight");
        fields.push_back("blockHosts");

	client.hmget(key, fields, [&blockID, &version, &size, &bucket_name, &obj_name, &creationTime, &dirty, &globalWeight, &blockHosts](cpp_redis::reply& reply){
	  if (reply.is_array()){
	    auto arr = reply.as_array();
	    if (!arr[0].is_null()){
	      blockID = arr[0].as_string();
	      version = arr[1].as_string();
 	      size = arr[2].as_string();
    	      bucket_name = arr[3].as_string();
	      obj_name = arr[4].as_string();
	      creationTime  = arr[5].as_string();
	      dirty = arr[6].as_string();
	      globalWeight = arr[7].as_string();
  	      blockHosts = arr[8].as_string();
	    }
	  }
	});

	client.sync_commit(std::chrono::milliseconds(300));
	  
	std::stringstream sloction(blockHosts);
	std::string tmp;
	
	ptr->cacheObj.objName = obj_name;
	ptr->cacheObj.bucketName = bucket_name;
	ptr->cacheObj.creationTime = creationTime;
	ptr->cacheObj.dirty = boost::lexical_cast<bool>(dirty);
	ptr->dirty = boost::lexical_cast<bool>(dirty);

	//host1_host2_host3_...
	while(getline(sloction, tmp, '_'))
	  ptr->hostsList.push_back(tmp);

	ptr->blockID = boost::lexical_cast<uint64_t>(blockID);
	ptr->version = version;
	ptr->size = boost::lexical_cast<uint64_t>(size);
	ptr->globalWeight = boost::lexical_cast<int>(globalWeight);

        ldout(cct,10) << __func__ << ": " << __LINE__<< ": objName: "<< obj_name << dendl;
        ldout(cct,10) << __func__ << ": " << __LINE__<< ": version: "<< version << dendl;
        ldout(cct,10) << __func__ << ": " << __LINE__<< ": size: "<< size << dendl;
    }
    catch(std::exception &e) {
      ldout(cct,10) << __func__ << " Error" << " key " << key << dendl;
	return -EINVAL;
    }
  }
  else{
    return -ENOENT;  
  }
  return 0;
}

int RGWObjectDirectory::get_attr(CacheObjectCpp *ptr, const char* name, bufferlist &dest, optional_yield y)
{
  ldout(cct,10) <<__func__<<": " << __LINE__ <<  dendl;
  std::string key = buildIndex(ptr);
  ldout(cct,10) <<__func__<<": " << __LINE__ << " key is: " << key <<  dendl;
  cpp_redis::client client;
  findClient(key, &client);
  if (!(client.is_connected())){
	return -1;
  }
  ldout(cct,10) <<__func__<<": " << __LINE__ <<  dendl;
  std::string result;
  int exist = 0;
  ldout(cct,10) <<__func__<<": " << __LINE__ <<  dendl;

  std::vector<std::string> keys;
  keys.push_back(key);
    
  client.exists(keys, [&exist](cpp_redis::reply &reply){
	 	if (reply.is_integer()){
		  exist = reply.as_integer();
		}
	});
	client.sync_commit(std::chrono::milliseconds(300));

  ldout(cct,10) <<__func__<<": " << __LINE__ <<  ": exist: " << exist << dendl;
	
  if (exist) {
  //if (exist_key(ptr)){
    ldout(cct,10) << __func__ << ": " << __LINE__<< ": object: "<< key << dendl;
    try{
	std::string value;
	std::vector<std::string> fields;

        fields.push_back(name);

	client.hmget(key, fields, [&value](cpp_redis::reply& reply){
	  if (reply.is_array()){
	    auto arr = reply.as_array();
	    if (!arr[0].is_null()){
	      value = arr[0].as_string();
	    }
	  }
	});

	client.sync_commit(std::chrono::milliseconds(300));
	dest.append(value);
    }
    catch(std::exception &e) {
      ldout(cct,10) << __func__ << " Error" << " key " << key << dendl;
	return -EINVAL;
    }
  }
  else{
    ldout(cct,10) << __func__ << ": " << __LINE__<< ": object: "<< key << dendl;
    return -ENOENT;  
  }
  ldout(cct,10) << __func__ << ": " << __LINE__<< ": object: "<< key << dendl;
  return 0;
}

} }
