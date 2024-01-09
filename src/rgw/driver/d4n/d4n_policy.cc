#include "../../../common/async/yield_context.h"
#include "d4n_policy.h"

#define dout_subsys ceph_subsys_rgw
#define dout_context g_ceph_context

namespace rgw { namespace d4n {

int CachePolicy::find_client(cpp_redis::client* client) {
  if (client->is_connected())
    return 0;

  if (get_addr().host == "" || get_addr().port == 0) {
    dout(10) << "RGW D4N Policy: D4N policy endpoint was not configured correctly" << dendl;
    return EDESTADDRREQ;
  }

  client->connect(get_addr().host, get_addr().port, nullptr);

  if (!client->is_connected())
    return ECONNREFUSED;

  return 0;
}

int CachePolicy::exist_key(std::string key) {
  int result = -1;
  std::vector<std::string> keys;
  keys.push_back(key);

  if (!client.is_connected()) { 
    find_client(&client);
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

int LFUDAPolicy::init(CephContext *_cct, const DoutPrefixProvider* dpp, rgw::sal::Driver *_driver) {
      cct = _cct;
      dir->init(_cct);
      addr.host = cct->_conf->rgw_d4n_host;
      addr.port = cct->_conf->rgw_d4n_port;
      driver = _driver;
      
      tc = std::thread(&CachePolicy::cleaning, this, dpp);
      tc.detach();

      return 0;
}

int LFUDAPolicy::set_dirty(std::string key, int dirty, optional_yield y) {
  int result = 0;
  if (!client.is_connected()) { 
    find_client(&client);
  }

  try {
    client.hset(key, "dirty", std::to_string(dirty), [&result](cpp_redis::reply& reply) {
      if (!reply.is_null()) {
	result = reply.as_integer();
      }
    }); 

    client.sync_commit(std::chrono::milliseconds(1000));
  } catch(std::exception &e) {
    return -1;
  }

  return result;
}



int LFUDAPolicy::set_age(int age, const DoutPrefixProvider* dpp) {
  int result = 0;
        ldpp_dout(dpp, 20) << "LFUDAPolicy: " << __func__ << __LINE__ << dendl;

  if (!client.is_connected()) { 
    find_client(&client);
  }
  try {
    client.hset("lfuda", "age", std::to_string(age), [&result](cpp_redis::reply& reply) {
      if (!reply.is_null()) {
	result = reply.as_integer(); 
      }
    }); 

        ldpp_dout(dpp, 20) << "LFUDAPolicy: " << __func__ << __LINE__ << dendl;
    client.sync_commit();
  } catch(std::exception &e) {
    return -1;
  }

        ldpp_dout(dpp, 20) << "LFUDAPolicy: " << __func__ << __LINE__ << dendl;
  return 0;
}

int LFUDAPolicy::get_age(const DoutPrefixProvider* dpp) {
  int ret = 0;
  int age = -1;
        ldpp_dout(dpp, 20) << "LFUDAPolicy: " << __func__ << __LINE__ << dendl;

  if (!client.is_connected()) { 
    find_client(&client);
  }
  try {
    client.hexists("lfuda", "age", [&ret](cpp_redis::reply& reply) {
      if (!reply.is_null()) {
        ret = reply.as_integer();
      }
    });

    client.sync_commit(std::chrono::milliseconds(1000));
  } catch(std::exception &e) {
    return -1;
  }

        ldpp_dout(dpp, 20) << "LFUDAPolicy: " << __func__ << __LINE__ << dendl;
  if (!ret) {
        ldpp_dout(dpp, 20) << "LFUDAPolicy: " << __func__ << __LINE__ << dendl;
    ret = set_age(0, dpp); /* Initialize age */
        ldpp_dout(dpp, 20) << "LFUDAPolicy: " << __func__ << __LINE__ << dendl;

    if (!ret) {
        ldpp_dout(dpp, 20) << "LFUDAPolicy: " << __func__ << __LINE__ << dendl;
      return 0; /* Success */
    } else {
        ldpp_dout(dpp, 20) << "LFUDAPolicy: " << __func__ << __LINE__ << dendl;
      return -1;
    };
  }

        ldpp_dout(dpp, 20) << "LFUDAPolicy: " << __func__ << __LINE__ << dendl;
  try {
        ldpp_dout(dpp, 20) << "LFUDAPolicy: " << __func__ << __LINE__ << dendl;
    client.hget("lfuda", "age", [&age](cpp_redis::reply& reply) {
      if (!reply.is_null()) {
        age = std::stoi(reply.as_string());
      }
    });

    client.sync_commit(std::chrono::milliseconds(1000));
        ldpp_dout(dpp, 20) << "LFUDAPolicy: " << __func__ << __LINE__ << dendl;
  } catch(std::exception &e) {
        ldpp_dout(dpp, 20) << "LFUDAPolicy: " << __func__ << __LINE__ << dendl;
    return -1;
  }
        ldpp_dout(dpp, 20) << "LFUDAPolicy: " << __func__ << __LINE__ << dendl;

  return age;
}

int LFUDAPolicy::set_local_weight_sum(size_t weight, optional_yield y) {
  int result = 0;
  if (!client.is_connected()) { 
    find_client(&client);
  }

  try {
    client.hset("lfuda", "localWeights", std::to_string(weight), [&result](cpp_redis::reply& reply) {
      if (!reply.is_null()) {
	result = reply.as_integer();
      }
    }); 

    client.sync_commit(std::chrono::milliseconds(1000));
  } catch(std::exception &e) {
    return -1;
  }

  return result;
}

int LFUDAPolicy::get_local_weight_sum(optional_yield y) {
  /* FIXME: for having several remote caches we need this part
    req.push("HEXISTS", dir->cct->_conf->rgw_local_cache_address, "localWeights");

  if (!std::get<0>(resp).value()) {
    int sum = 0;
    for (auto& entry : b_entries_map)
      sum += entry.second->localWeight; 
 
    if (set_local_weight_sum(sum, y) < 0) { // Initialize
      return -1;
    } else {
      return sum;
    }
  }
  */

  int weight = -1;

  if (!client.is_connected()) { 
    find_client(&client);
  }
  try {
    client.hget("lfuda", "localWeights", [&weight](cpp_redis::reply& reply) {
      if (!reply.is_null()) {
	weight = reply.as_integer();
      }
    });

    client.sync_commit(std::chrono::milliseconds(1000));
  } catch(std::exception &e) {
    return -1;
  }

  return weight;
}



int LFUDAPolicy::set_global_weight(std::string key, int weight) {
  int result = 0;

  if (!client.is_connected()) { 
    find_client(&client);
  }
  try {
    client.hset(key, "globalWeight", std::to_string(weight), [&result](cpp_redis::reply& reply) {
      if (!reply.is_null()) {
	result = reply.as_integer();
      }
    }); 

    client.sync_commit();
  } catch(std::exception &e) {
    return -1;
  }

  return result - 1;
}

int LFUDAPolicy::get_global_weight(std::string key) {
  int weight = -1;

  if (!client.is_connected()) { 
    find_client(&client);
  }
  try {
    client.hget(key, "globalWeight", [&weight](cpp_redis::reply& reply) {
      if (!reply.is_null()) {
	weight = reply.as_integer();
      }
    });

    client.sync_commit(std::chrono::milliseconds(1000));
  } catch(std::exception &e) {
    return -1;
  }

  return weight;
}

int LFUDAPolicy::set_min_avg_weight(size_t weight, std::string cacheLocation) {
  int result = 0;

  if (!client.is_connected()) { 
    find_client(&client);
  }
  try {
    client.hset("lfuda", "minAvgWeight:cache", cacheLocation, [&result](cpp_redis::reply& reply) {
      if (!reply.is_null()) {
	result = reply.as_integer();
      }
    }); 

    client.sync_commit();
  } catch(std::exception &e) {
    return -1;
  }

  if (result == 1) {
    result = 0;
    try {
      client.hset("lfuda", "minAvgWeight:weight", std::to_string(weight), [&result](cpp_redis::reply& reply) {
	if (!reply.is_null()) {
	  result = reply.as_integer();
	}
      }); 

      client.sync_commit();
    } catch(std::exception &e) {
      return -1;
    }
  }

  return result - 1;
}

int LFUDAPolicy::get_min_avg_weight() {
  int ret = 0;
  int weight = -1;

  if (!client.is_connected()) { 
    find_client(&client);
  }
  try {
    client.hexists("lfuda", "minAvgWeight:cache", [&ret](cpp_redis::reply& reply) {
      if (!reply.is_null()) {
        ret = reply.as_integer();
      }
    });

    client.sync_commit(std::chrono::milliseconds(1000));
  } catch(std::exception &e) {
    return -1;
  }

  if (!ret) {
    ret = set_min_avg_weight(INT_MAX, ""/* local cache location or keep empty? */); /* Initialize minimum average weight */

    if (!ret) {
      return INT_MAX; /* Success */
    } else {
      return -1;
    };
  }

  try {
    client.hget("lfuda", "minAvgWeight:weight", [&weight](cpp_redis::reply& reply) {
      if (!reply.is_null()) {
        weight = std::stoi(reply.as_string());
      }
    });

    client.sync_commit(std::chrono::milliseconds(1000));
  } catch(std::exception &e) {
    return -1;
  }

  return weight;
}

CacheBlock* LFUDAPolicy::find_victim(const DoutPrefixProvider* dpp) {
  const std::lock_guard l(lfuda_lock);
  if (entries_heap.empty())
    return nullptr;

  /* Get victim cache block */
  std::string key = entries_heap.top()->key;
  CacheBlock* victim = new CacheBlock();

  victim->cacheObj.bucketName = key.substr(0, key.find('_')); 
  key.erase(0, key.find('_') + 1);
  victim->cacheObj.objName = key.substr(0, key.find('_'));
  victim->blockId = entries_heap.top()->offset;
  victim->size = entries_heap.top()->len;

  if (dir->get_value(victim) < 0) {
    return nullptr;
  }

  return victim;
}

void LFUDAPolicy::shutdown() {
  dir->shutdown();
}

int LFUDAPolicy::eviction(const DoutPrefixProvider* dpp, uint64_t size, optional_yield y) {
  return 0;
#if 0
  CacheBlock* victim = find_victim(dpp);

  if (victim == nullptr) {
    ldpp_dout(dpp, 10) << "RGW D4N Policy: Could not find victim block" << dendl;
    return -1;
  }

    std::string key = victim->cacheObj.bucketName + "_" + victim->cacheObj.objName + "_" + std::to_string(victim->blockId) + "_" + std::to_string(victim->size);
    auto it = b_entries_map.find(key);
    if (it == b_entries_map.end()) {
      delete victim;
      return -1;
    }

    int avgWeight = get_local_weight_sum(y) / b_entries_map.size();
    if (avgWeight < 0) {
      delete victim;
      return -1;
    }

    if (victim->hostsList.size() == 1 && victim->hostsList[0] == dir->get_addr().host + ":" + std::to_string(dir->get_addr().port)) { /* Last copy */
      if (victim->globalWeight) {
	const std::lock_guard l(lfuda_lock);
	it->second->localWeight += victim->globalWeight;
        (*it->second->handle)->localWeight = it->second->localWeight;
	entries_heap.increase(it->second->handle);

	if (cacheDriver->set_attr(dpp, key, "user.rgw.localWeight", std::to_string(it->second->localWeight), y) < 0) 
	  return -1;

	victim->globalWeight = 0;
	if (dir->update_field(victim, "globalWeight", std::to_string(victim->globalWeight)) < 0) {
	  delete victim;
	  return -1;
        }
      }

      if (it->second->localWeight > avgWeight) {
	// TODO: push victim block to remote cache
	// add remote cache host to host list
      }
    }

    victim->globalWeight += it->second->localWeight;
    if (dir->update_field(victim, "globalWeight", std::to_string(victim->globalWeight)) < 0) {
      delete victim;
      return -1;
    }

    if (dir->remove_host(victim, dir->get_addr().host + ":" + std::to_string(dir->get_addr().port)) < 0) {
      delete victim;
      return -1;
    }

    delete victim;

    //FIXME: needs SSD driver to implement it.
    /*
    if (cacheDriver->del(dpp, key, y) < 0) 
      return -1;
      */

    ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): Block " << key << " has been evicted." << dendl;

    int weight = (avgWeight * b_entries_map.size()) - it->second->localWeight;
    if (set_local_weight_sum((weight > 0) ? weight : 0, y) < 0)
      return -1;

    int age = get_age();
    age = std::max(it->second->localWeight, age);
    if (set_age(age, y) < 0)
      return -1;

    erase(dpp, key, y);
    freeSpace = cacheDriver->get_free_space(dpp);
  }
  
  return 0;
#endif
}

std::string LFUDAPolicy::update(const DoutPrefixProvider* dpp, std::string& key, uint64_t offset, uint64_t len, std::string version, int dirty, time_t lastAccessTime, const rgw_user user, optional_yield y)
{
        ldpp_dout(dpp, 20) << "LFUDAPolicy: " << __func__ << __LINE__ << dendl;

  //std::unique_ptr<rgw::sal::D4NFilterBucket> bucket(r_bucket);
  ldpp_dout(dpp, 20) << "LFUDAPolicy: " << __func__ << __LINE__ << " user name is: " << user << dendl;

  using handle_type = boost::heap::fibonacci_heap<LFUDAEntry*, boost::heap::compare<EntryComparator<LFUDAEntry>>>::handle_type;

  
        ldpp_dout(dpp, 20) << "LFUDAPolicy: " << __func__ << __LINE__ << dendl;
  int age = 0; 
  int localWeight = age;
  /*
  int age = get_age(dpp); 
        ldpp_dout(dpp, 20) << "LFUDAPolicy: " << __func__ << __LINE__ << dendl;
  int localWeight = age;
  auto entry = find_entry(key);
        ldpp_dout(dpp, 20) << "LFUDAPolicy: " << __func__ << __LINE__ << dendl;
  if (entry != nullptr) { 
        ldpp_dout(dpp, 20) << "LFUDAPolicy: " << __func__ << __LINE__ << dendl;
    entry->localWeight += age;
        ldpp_dout(dpp, 20) << "LFUDAPolicy: " << __func__ << __LINE__ << dendl;
    localWeight = entry->localWeight;
        ldpp_dout(dpp, 20) << "LFUDAPolicy: " << __func__ << __LINE__ << dendl;
  }  
  */
  erase(dpp, key, y);
  
        ldpp_dout(dpp, 20) << "LFUDAPolicy: " << __func__ << __LINE__ << dendl;
  const std::lock_guard l(lfuda_lock);
        ldpp_dout(dpp, 20) << "LFUDAPolicy: " << __func__ << __LINE__ << dendl;
  LFUDAEntry *e = new LFUDAEntry(key, offset, len, version, dirty, lastAccessTime, user, localWeight);
        ldpp_dout(dpp, 20) << "LFUDAPolicy: " << __func__ << __LINE__ << dendl;
  handle_type handle = entries_heap.push(e);
        ldpp_dout(dpp, 20) << "LFUDAPolicy: " << __func__ << __LINE__ << dendl;
  e->set_handle(handle);
        ldpp_dout(dpp, 20) << "LFUDAPolicy: " << __func__ << __LINE__ << dendl;
  b_entries_map.emplace(key, e);
        ldpp_dout(dpp, 20) << "LFUDAPolicy: " << __func__ << __LINE__ << dendl;

  /* This part has been moved to d4n driver
  if (cacheDriver->set_attr(dpp, key, "user.rgw.localWeight", std::to_string(localWeight), y) < 0) 
    ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): CacheDriver set_attr method failed." << dendl;
  */
/*
  auto localWeights = get_local_weight_sum(y);
        ldpp_dout(dpp, 20) << "LFUDAPolicy: " << __func__ << __LINE__ << dendl;
  localWeights += localWeight;
        ldpp_dout(dpp, 20) << "LFUDAPolicy: " << __func__ << __LINE__ << dendl;
  if (set_local_weight_sum(localWeights, y) < 0)
    ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): Failed to update sum of local weights for the cache backend." << dendl;
        ldpp_dout(dpp, 20) << "LFUDAPolicy: " << __func__ << __LINE__ << dendl;
*/
 return std::to_string(localWeight);
}


void LFUDAPolicy::updateObj(const DoutPrefixProvider* dpp, std::string& key, std::string version, int dirty, uint64_t size, time_t lastAccessTime, const rgw_user user, optional_yield y)
{
  ldpp_dout(dpp, 20) << "LFUDAPolicy: " << __func__ << __LINE__ << dendl;
  //using handle_type = boost::heap::fibonacci_heap<LFUDAEntry*, boost::heap::compare<EntryComparator<LFUDAEntry>>>::handle_type;

  
        ldpp_dout(dpp, 20) << "LFUDAPolicy: " << __func__ << __LINE__ << dendl;

  eraseObj(dpp, key, y);
  
        ldpp_dout(dpp, 20) << "LFUDAPolicy: " << __func__ << __LINE__ << dendl;
  const std::lock_guard l(lfuda_lock);
        ldpp_dout(dpp, 20) << "LFUDAPolicy: " << __func__ << __LINE__ << dendl;
  LFUDAObjEntry *e = new LFUDAObjEntry(key, version, dirty, size, lastAccessTime, user);
        ldpp_dout(dpp, 20) << "LFUDAPolicy: " << __func__ << __LINE__ << dendl;
  //handle_type handle = entries_heap.push(e);
        ldpp_dout(dpp, 20) << "LFUDAPolicy: " << __func__ << __LINE__ << dendl;
  //e->set_handle(handle);
        ldpp_dout(dpp, 20) << "LFUDAPolicy: " << __func__ << __LINE__ << dendl;
  o_entries_map.emplace(key, e);
        ldpp_dout(dpp, 20) << "LFUDAPolicy: " << __func__ << __LINE__ << dendl;

  return;
}

bool LFUDAPolicy::erase(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y)
{
  const std::lock_guard l(lfuda_lock);
  auto p = b_entries_map.find(key);
  if (p == b_entries_map.end()) {
    return false;
  }

  /*
  auto localWeights = get_local_weight_sum(y);
  localWeights -= p->second->localWeight;
  if (set_local_weight_sum(localWeights, y) < 0)
    ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): Failed to update sum of local weights for the cache backend." << dendl;
  */
  b_entries_map.erase(p);
  entries_heap.erase(p->second->handle);

  return true;
}


bool LFUDAPolicy::eraseObj(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y)
{
  const std::lock_guard l(lfuda_lock);
  auto p = o_entries_map.find(key);
  if (p == o_entries_map.end()) {
    return false;
  }

  o_entries_map.erase(p);

  return true;
}

void LFUDAPolicy::cleaning(const DoutPrefixProvider* dpp)
{
  const int interval = cct->_conf->rgw_d4n_cache_cleaning_interval;
  while(true){
    ldpp_dout(dpp, 20) << __func__ << " : " << " Cache cleaning!" << dendl;
    std::string name = ""; 
    std::string b_name = ""; 
    std::string key = ""; 
    //off_t ofs = 0; 
    uint64_t len = 0;
    rgw::sal::Attrs obj_attrs;
    int count = 0;

    for (auto it = o_entries_map.begin(); it != o_entries_map.end(); it++){
      if ((it->second->dirty == 1) && (std::difftime(time(NULL), it->second->lastAccessTime) > interval)){ //if block is dirty and written more than interval seconds ago
    	ldpp_dout(dpp, 20) << "AMIN: " << __func__ << " : " << " object to clean is: " << it->first << dendl;
	name = it->first;
	rgw_user c_rgw_user = it->second->user;

    	  ldpp_dout(dpp, 10) << "AMIN: " << __func__ << " : "  << __LINE__ << " User name is: " << c_rgw_user << dendl;
	size_t pos = 0;
	std::string delimiter = "_";
	while ((pos = name.find(delimiter)) != std::string::npos) {
    	  ldpp_dout(dpp, 10) << "AMIN: " << __func__ << " : "  << __LINE__ << " Name is: " << name << dendl;
	  if (count == 0){
	    b_name = name.substr(0, pos);
    	    name.erase(0, pos + delimiter.length());
    	    ldpp_dout(dpp, 10) << "AMIN: " << __func__ << " : "  << __LINE__ << " bucket is: " << b_name << dendl;
	  }
	  /*
	  else
	  {
	    key = name.substr(0, pos);
    	    name.erase(0, pos + delimiter.length());
    	    ldpp_dout(dpp, 10) << "AMIN: " << __func__ << " : "  << __LINE__ << " object is: " << key << dendl;
    	    ldpp_dout(dpp, 10) << "AMIN: " << __func__ << " : " << " Key is: " << key << dendl;
	  }
	  */
	  count ++;
	}
	key = name;

	//writing data to the backend
	//we need to create an atomic_writer
 	rgw_obj_key c_obj_key = rgw_obj_key(key); 		
    	ldpp_dout(dpp, 10) << "AMIN: " << __func__ << " : "  << __LINE__ << " c_obj_key is: " << c_obj_key << dendl;

	std::unique_ptr<rgw::sal::User> c_user = driver->get_user(c_rgw_user);
    	ldpp_dout(dpp, 10) << "AMIN: " << __func__ << " : "  << __LINE__ << " c_user NAME is: " << c_user->get_id() << dendl;
	std::unique_ptr<rgw::sal::Bucket> c_bucket;

        rgw_bucket c_rgw_bucket = rgw_bucket(c_rgw_user.to_str(), b_name, "");

	int ret = driver->get_bucket(dpp, c_user.get(), c_rgw_bucket, &c_bucket, null_yield);
    	ldpp_dout(dpp, 10) << "AMIN: " << __func__ << " : "  << __LINE__ << " c_bucket NAME " << c_bucket->get_name() << dendl;

	std::unique_ptr<rgw::sal::Object> c_obj = c_bucket->get_object(c_obj_key);
    	ldpp_dout(dpp, 10) << "AMIN: " << __func__ << " : "  << __LINE__ << " c_obj NAME is: " << c_obj->get_key().get_oid() << dendl;

	//driver->set_write_to_backend(true); //set operation to cleaning
	std::unique_ptr<rgw::sal::Writer> processor =  driver->get_atomic_writer(dpp,
				  null_yield,
				  c_obj.get(),
				  c_rgw_user,
				  NULL,
				  0,
				  "AMIN_TEST_ID");
	
        ldpp_dout(dpp, 10) << "AMIN: " << __func__ << " : "  << __LINE__ << " c_obj NAME is: " << c_obj->get_key().get_oid() << dendl;


  	int op_ret = processor->prepare(null_yield);
  	if (op_ret < 0) {
    	  ldpp_dout(dpp, 20) << "processor->prepare() returned ret=" << op_ret << dendl;
    	  break;
  	}

	std::string prefix = "D_"+b_name+"_"+key;
	off_t lst = it->second->size;
  	off_t fst = 0;
  	off_t ofs = 0;

	/*
  	rgw::sal::DataProcessor *filter = processor.get();
	do {
           ldpp_dout(dpp, 20) << "AMIN: " << __func__ << __LINE__ << dendl;
    	  ceph::bufferlist data;
    	  if (fst >= lst){
      	    break;
    	  }
    	  off_t cur_lst = std::min<off_t>(fst + cct->_conf->rgw_max_chunk_size, lst);
    	  std::string oid_in_cache = prefix+"_"+std::to_string(fst)+"_"+std::to_string(cur_lst);  	  
    	  cacheDriver->get(dpp, oid_in_cache, fst, cur_lst, data, obj_attrs, null_yield);
    	  len = data.length();
    	  fst += len;

    	  ldpp_dout(dpp, 10) << "AMIN: " << __func__ << " : " << "Data is: " << data.to_str() << dendl;
    	  ldpp_dout(dpp, 10) << "AMIN: " << __func__ << " : " << "Data length: " << data.length() << dendl;
    	  if (len == 0) {
      	    break;
   	  }

    	  op_ret = filter->process(std::move(data), ofs);
    	  if (op_ret < 0) {
      	    ldpp_dout(dpp, 20) << "processor->process() returned ret="
          	<< op_ret << dendl;
      	    return;
    	  }

    	  ofs += len;
  	} while (len > 0);

  	op_ret = filter->process({}, ofs);
	*/
    	ceph::bufferlist bl;
    	std::string oid_in_cache = prefix+"_"+std::to_string(ofs)+"_"+std::to_string(lst);  	  
    	cacheDriver->get(dpp, oid_in_cache, ofs, lst, bl, obj_attrs, null_yield);
    	op_ret = processor->process(std::move(bl), ofs);


	rgw::sal::Attrs attrs;
	std::vector<std::string> storageClass = {"storageClass"};
  	const req_context rctx{dpp, null_yield, nullptr};
	attrs[RGW_ATTR_STORAGE_CLASS] = bufferlist::static_from_string(storageClass[0]);

  	c_obj->set_obj_size(lst);
        op_ret = processor->complete(lst, "", nullptr, ceph::real_time(), attrs,
                               ceph::real_time(), nullptr, nullptr,
                               nullptr, nullptr, nullptr,
                               rctx);
	//d4nDriver->set_write_to_backend(false); //set operation to cleaning

	//data is clean now, updating in-memory metadata
	it->second->dirty = 0;
        //set_dirty(it->first, 0, null_yield);	

      }
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(interval));
  }
}

int LRUPolicy::exist_key(std::string key)
{
  const std::lock_guard l(lru_lock);
  if (b_entries_map.count(key) != 0) {
      return true;
    }
    return false;
}

int LRUPolicy::eviction(const DoutPrefixProvider* dpp, uint64_t size, optional_yield y)
{
#if 0
  const std::lock_guard l(lru_lock);
  uint64_t freeSpace = cacheDriver->get_free_space(dpp);

  while (freeSpace < size) {
    auto p = entries_lru_list.front();
    b_entries_map.erase(b_entries_map.find(p.key));
    entries_lru_list.pop_front_and_dispose(Entry_delete_disposer());
    auto ret = cacheDriver->delete_data(dpp, p.key, y);
    if (ret < 0) {
      ldpp_dout(dpp, 10) << __func__ << "(): Failed to delete data from the cache backend: " << ret << dendl;
      return ret;
    }

    freeSpace = cacheDriver->get_free_space(dpp);
  }
#endif

  return 0;

}

std::string LRUPolicy::update(const DoutPrefixProvider* dpp, std::string& key, uint64_t offset, uint64_t len, std::string version, int dirty, time_t lastAccessTime, const rgw_user user, optional_yield y)
{
  erase(dpp, key, y);
  const std::lock_guard l(lru_lock);
  Entry *e = new Entry(key, offset, len);
  entries_lru_list.push_back(*e);
  b_entries_map.emplace(key, e);
  return "";
}

void LRUPolicy::updateObj(const DoutPrefixProvider* dpp, std::string& key, std::string version, int dirty, uint64_t size, time_t lastAccessTime, const rgw_user user, optional_yield y)
{
  eraseObj(dpp, key, y);
  const std::lock_guard l(lru_lock);
  ObjEntry *e = new ObjEntry(key, size);
  o_entries_map.emplace(key, e);
  return;
}


bool LRUPolicy::erase(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y)
{
  const std::lock_guard l(lru_lock);
  auto p = b_entries_map.find(key);
  if (p == b_entries_map.end()) {
    return false;
  }
  b_entries_map.erase(p);
  entries_lru_list.erase_and_dispose(entries_lru_list.iterator_to(*(p->second)), Entry_delete_disposer());
  return true;
}

bool LRUPolicy::eraseObj(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y)
{
  const std::lock_guard l(lru_lock);
  auto p = o_entries_map.find(key);
  if (p == o_entries_map.end()) {
    return false;
  }
  o_entries_map.erase(p);
  return true;
}


void LRUPolicy::shutdown() {
  dir->shutdown();
}

/*
int PolicyDriver::init() {
  if (policyName == "lfuda") {
    cachePolicy = new LFUDAPolicy(cacheDriver);
    return 0;
  } else if (policyName == "lru") {
    cachePolicy = new LRUPolicy(cacheDriver);
    return 0;
  }
  return -1;
}
*/

} } // namespace rgw::d4n
