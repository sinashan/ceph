#include "d4n_directory.h"
#include <time.h>

#define dout_subsys ceph_subsys_rgw
#define dout_context g_ceph_context

namespace rgw { namespace d4n {

int ObjectDirectory::find_client(cpp_redis::client* client) {
  if (client->is_connected())
    return 0;

   if (addr.host == "" || addr.port == 0) {
    dout(10) << "RGW D4N Directory: D4N directory endpoint was not configured correctly" << dendl;
    return EDESTADDRREQ;
  }

  client->connect(addr.host, addr.port, nullptr);

  if (!client->is_connected())
    return ECONNREFUSED;

  return 0;
}

std::string ObjectDirectory::build_index(CacheObj* object) {
  return object->bucketName + "_" + object->objName;
}

int ObjectDirectory::exist_key(std::string key) {
  int result = 0;
  std::vector<std::string> keys;
  keys.push_back(key);
  
  if (!client.is_connected()) {
    return result;
  }

  try {
    client.exists(keys, [&result](cpp_redis::reply &reply) {
      if (reply.is_integer()) {
        result = reply.as_integer();
      }
    });
    
    client.sync_commit(std::chrono::milliseconds(1000));
  } catch(std::exception &e) {}

  return result;
}

int ObjectDirectory::set_value(CacheObj* object) {
  /* Creating the index based on objName */
  std::string result;
  std::string key = build_index(object);
  if (!client.is_connected()) { 
    find_client(&client);
  }

  /* Every set will be new */
  if (addr.host == "" || addr.port == 0) {
    dout(10) << "RGW D4N Directory: Directory endpoint not configured correctly" << dendl;
    return -2;
  }
    
  std::string endpoint = addr.host + ":" + std::to_string(addr.port);
  std::vector< std::pair<std::string, std::string> > list;
    
  /* Creating a list of the entry's properties */
  list.push_back(make_pair("key", key));
  list.push_back(make_pair("objName", object->objName));
  list.push_back(make_pair("bucketName", object->bucketName));
  list.push_back(make_pair("lastAccessTime", std::to_string(object->lastAccessTime)));
  list.push_back(make_pair("dirty", std::to_string(object->dirty)));
  list.push_back(make_pair("hosts", endpoint)); 

  try {
    client.hmset(key, list, [&result](cpp_redis::reply &reply) {
      if (!reply.is_null()) {
        result = reply.as_string();
      }
    });

    client.sync_commit(std::chrono::milliseconds(1000));

    if (result != "OK") {
      return -1;
    }
  } catch(std::exception &e) {
    return -1;
  }

  return 0;
}

int ObjectDirectory::get_value(CacheObj* object) {
  int keyExist = -2;
  std::string key = build_index(object);

  if (!client.is_connected()) {
    find_client(&client);
  }

  if (exist_key(key)) {
    std::string key;
    std::string objName;
    std::string bucketName;
    std::string lastAccessTime;
    std::string dirty;
    std::string hosts;
    std::vector<std::string> fields;

    fields.push_back("key");
    fields.push_back("objName");
    fields.push_back("bucketName");
    fields.push_back("lastAccessTime");
    fields.push_back("dirty");
    fields.push_back("hosts");

    try {
      client.hmget(key, fields, [&key, &objName, &bucketName, &lastAccessTime, &dirty, &hosts, &keyExist](cpp_redis::reply &reply) {
        if (reply.is_array()) {
	  auto arr = reply.as_array();

	  if (!arr[0].is_null()) {
	    keyExist = 0;
	    key = arr[0].as_string();
	    objName = arr[1].as_string();
	    bucketName = arr[2].as_string();
	    lastAccessTime = arr[3].as_string();
	    dirty = arr[4].as_string();
	    hosts = arr[5].as_string();
	  }
	}
      });

      client.sync_commit(std::chrono::milliseconds(1000));

      if (keyExist < 0) {
        return keyExist;
      }

      /* Currently, there can only be one host */
      object->objName = objName;
      object->bucketName = bucketName;

      struct std::tm tm;
      std::istringstream(lastAccessTime) >> std::get_time(&tm, "%T");
      strptime(lastAccessTime.c_str(), "%T", &tm); // Need to check formatting -Sam
      object->lastAccessTime = mktime(&tm);

      object->dirty = std::stoi(dirty);
    } catch(std::exception &e) {
      keyExist = -1;
    }
  }

  return keyExist;
}

int ObjectDirectory::del_value(CacheObj* object) {
  int result = 0;
  std::vector<std::string> keys;
  std::string key = build_index(object);
  keys.push_back(key);
  
  if (!client.is_connected()) {
    find_client(&client);
  }
  
  if (exist_key(key)) {
    try {
      client.del(keys, [&result](cpp_redis::reply &reply) {
        if (reply.is_integer()) {
          result = reply.as_integer();
        }
      });
	
      client.sync_commit(std::chrono::milliseconds(1000));	
      return result - 1;
    } catch(std::exception &e) {
      return -1;
    }
  } else {
    return -2;
  }
}

int BlockDirectory::find_client(cpp_redis::client* client) {
  if (client->is_connected())
    return 0;

   if (addr.host == "" || addr.port == 0) {
    dout(10) << "RGW D4N Directory: D4N directory endpoint was not configured correctly" << dendl;
    return EDESTADDRREQ;
  }

  client->connect(addr.host, addr.port, nullptr);

  if (!client->is_connected())
    return ECONNREFUSED;

  return 0;
}

std::string BlockDirectory::build_index(CacheBlock* block) {
  return block->cacheObj.bucketName + "_" + block->cacheObj.objName + "_" + boost::lexical_cast<std::string>(block->blockId) + "_" + std::to_string(block->size);
}

int BlockDirectory::exist_key(std::string key) {
  int result = 0;
  std::vector<std::string> keys;
  keys.push_back(key);
  
  if (!client.is_connected()) {
    return result;
  }

  try {
    client.exists(keys, [&result](cpp_redis::reply &reply) {
      if (reply.is_integer()) {
        result = reply.as_integer();
      }
    });
    
    client.sync_commit(std::chrono::milliseconds(1000));
  } catch(std::exception &e) {}

  return result;
}

int BlockDirectory::set_value(CacheBlock* block) {
  /* Creating the index based on objName */
  std::string result;
  std::string key = build_index(block);
  if (!client.is_connected()) { 
    find_client(&client);
  }

  /* Every set will be new */
  if (addr.host == "" || addr.port == 0) {
    dout(10) << "RGW D4N Directory: Directory endpoint not configured correctly" << dendl;
    return -2;
  }
    
  std::string endpoint = addr.host + ":" + std::to_string(addr.port);
  std::vector< std::pair<std::string, std::string> > list;
    
  /* Creating a list of the entry's properties */
  list.push_back(make_pair("key", key));
  list.push_back(make_pair("size", std::to_string(block->size)));
  list.push_back(make_pair("dirty", std::to_string(block->dirty)));
  list.push_back(make_pair("lastAccessTime", std::to_string(block->lastAccessTime)));
  list.push_back(make_pair("globalWeight", std::to_string(block->globalWeight)));
  list.push_back(make_pair("bucketName", block->cacheObj.bucketName));
  list.push_back(make_pair("objName", block->cacheObj.objName));
  list.push_back(make_pair("hosts", endpoint)); 

  try {
    client.hmset(key, list, [&result](cpp_redis::reply &reply) {
      if (!reply.is_null()) {
        result = reply.as_string();
      }
    });

    client.sync_commit(std::chrono::milliseconds(1000));

    if (result != "OK") {
      return -1;
    }
  } catch(std::exception &e) {
    return -1;
  }

  return 0;
}

int BlockDirectory::get_value(CacheBlock* block) {
  int keyExist = -2;
  std::string key = build_index(block);

  if (!client.is_connected()) {
    find_client(&client);
  }

  if (exist_key(key)) {
    std::string hosts;
    std::string size;
    std::string dirty;
    std::string lastAccessTime;
    std::string bucketName;
    std::string objName;
    std::vector<std::string> fields;

    fields.push_back("key");
    fields.push_back("hosts");
    fields.push_back("size");
    fields.push_back("dirty");
    fields.push_back("lastAccessTime");
    fields.push_back("bucketName");
    fields.push_back("objName");

    try {
      client.hmget(key, fields, [&key, &hosts, &size, &dirty, &lastAccessTime, &bucketName, &objName, &keyExist](cpp_redis::reply &reply) {
        if (reply.is_array()) {
	  auto arr = reply.as_array();

	  if (!arr[0].is_null()) {
	    keyExist = 0;
	    key = arr[0].as_string();
	    hosts = arr[1].as_string();
	    size = arr[2].as_string();
	    dirty = arr[3].as_string();
	    lastAccessTime = arr[4].as_string();
	    bucketName = arr[5].as_string();
	    objName = arr[6].as_string();
	  }
	}
      });

      client.sync_commit(std::chrono::milliseconds(1000));

      if (keyExist < 0 ) {
        return keyExist;
      }

      /* Currently, there can only be one host */ // update -Sam
      block->size = std::stoi(size);

      struct std::tm tm;
      std::istringstream(lastAccessTime) >> std::get_time(&tm, "%T");
      strptime(lastAccessTime.c_str(), "%T", &tm);
      block->lastAccessTime = mktime(&tm);

      block->dirty = std::stoi(dirty);
      block->cacheObj.bucketName = bucketName;
      block->cacheObj.objName = objName;
    } catch(std::exception &e) {
      keyExist = -1;
    }
  }

  return keyExist;
}

int BlockDirectory::del_value(CacheBlock* block) {
  int result = 0;
  std::vector<std::string> keys;
  std::string key = build_index(block);
  keys.push_back(key);
  
  if (!client.is_connected()) {
    find_client(&client);
  }
  
  if (exist_key(key)) {
    try {
      client.del(keys, [&result](cpp_redis::reply &reply) {
        if (reply.is_integer()) {
          result = reply.as_integer();
        }
      });
	
      client.sync_commit(std::chrono::milliseconds(1000));	
      return result - 1;
    } catch(std::exception &e) {
      return -1;
    }
  } else {
    return -2;
  }
}

int BlockDirectory::update_field(CacheBlock* block, std::string field, std::string value) { // represent in cache block too -Sam
  std::string result;
  std::string key = build_index(block);

  if (!client.is_connected()) {
    find_client(&client);
  }
  
  if (exist_key(key)) {
    if (field == "hostsList") {
      /* Append rather than overwrite */
      std::string hosts;

      try {
        client.hget(key, "hostsList", [&hosts](cpp_redis::reply& reply) {
          if (!reply.is_null()) {
            hosts = reply.as_string();
          }
        });

        client.sync_commit(std::chrono::milliseconds(1000));
      } catch(std::exception &e) {
        return -1;
      }
      
      value += "_";
      value += hosts;
    }

    /* Update cache block */ // Remove ones that aren't used -Sam
    if (field == "size")
      block->size = std::stoi(value);
    else if (field == "dirty")
      block->dirty = std::stoi(value);
    else if (field == "bucketName")
      block->cacheObj.bucketName = value;
    else if (field == "objName")
      block->cacheObj.objName = value;
    else if (field == "hostsList")
      block->hostsList.push_back(value);
    else if (field == "lastAccessTime"){
      struct std::tm tm;
      std::istringstream(value) >> std::get_time(&tm, "%T");
      strptime(value.c_str(), "%T", &tm);
      block->lastAccessTime = mktime(&tm);
    }

    std::vector< std::pair<std::string, std::string> > list;
    list.push_back(std::make_pair(field, value));

    try {
      client.hmset(key, list, [&result](cpp_redis::reply &reply) {
	if (!reply.is_null()) {
	  result = reply.as_string();
	}
      });

      client.sync_commit(std::chrono::milliseconds(1000));

      if (result != "OK") {
	return -1;
      }
    } catch(std::exception &e) {
      return -1;
    }
  }

  return 0;
}

int BlockDirectory::update_field(std::string key, std::string field, std::string value) {
  std::string result;

  if (!client.is_connected()) {
    find_client(&client);
  }
  
  if (exist_key(key)) {
    if (field == "hostsList") {
      /* Append rather than overwrite */
      std::string hosts;

      try {
        client.hget(key, "hostsList", [&hosts](cpp_redis::reply& reply) {
          if (!reply.is_null()) {
            hosts = reply.as_string();
          }
        });

        client.sync_commit(std::chrono::milliseconds(1000));
      } catch(std::exception &e) {
        return -1;
      }
      
      value += "_";
      value += hosts;
    }

    try {
      client.hset(key, field, value, [&result](cpp_redis::reply &reply) {
	if (!reply.is_null()) {
	  result = reply.as_string();
	}
      });

      client.sync_commit(std::chrono::milliseconds(1000));

      if (result != "OK") {
	return -1;
      }
    } catch(std::exception &e) {
      return -1;
    }
  }

  return 0;
}



std::string BlockDirectory::get_field(std::string key, std::string field){
  std::string value;

  if (!client.is_connected()) {
    find_client(&client);
  }
  
  if (exist_key(key)) {
    if (field == "hostsList") {
      /* Append rather than overwrite */
      std::string hosts;

      try {
        client.hget(key, "hostsList", [&hosts](cpp_redis::reply& reply) {
          if (!reply.is_null()) {
            hosts = reply.as_string();
          }
        });

        client.sync_commit(std::chrono::milliseconds(1000));
      } catch(std::exception &e) {
        return "";
      }
      
      value += "_";
      value += hosts;
    }
    else{
      try {
        client.hget(key, field, [&value](cpp_redis::reply& reply) {
          if (!reply.is_null()) {
            value = reply.as_string();
          }
        });

        client.sync_commit(std::chrono::milliseconds(1000));
      } catch(std::exception &e) {
        return "";
      }
    }
    return value;
  }
  else
    return "";
}

} } // namespace rgw::d4n
