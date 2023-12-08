#include "../../../common/async/yield_context.h"
#include "d4n_policy.h"

#define dout_subsys ceph_subsys_rgw
#define dout_context g_ceph_context

namespace rgw { namespace d4n {

int CachePolicy::find_client(const DoutPrefixProvider* dpp, cpp_redis::client* client) {
  if (client->is_connected())
    return 0;

  if (get_addr().host == "" || get_addr().port == 0) {
    ldpp_dout(dpp, 10) << "RGW D4N Policy: D4N policy endpoint was not configured correctly" << dendl;
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

int LFUDAPolicy::set_age(int age) {
  int result = 0;

  try {
    client.hset("lfuda", "age", std::to_string(age), [&result](cpp_redis::reply& reply) {
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

int LFUDAPolicy::get_age() {
  int ret = 0;
  int age = -1;

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

  if (!ret) {
    ret = set_age(0); /* Initialize age */

    if (!ret) {
      return 0; /* Success */
    } else {
      return -1;
    };
  }

  try {
    client.hget("lfuda", "age", [&age](cpp_redis::reply& reply) {
      if (!reply.is_null()) {
        age = std::stoi(reply.as_string());
      }
    });

    client.sync_commit(std::chrono::milliseconds(1000));
  } catch(std::exception &e) {
    return -1;
  }

  return age;
}

int LFUDAPolicy::set_local_weight_sum(size_t weight, optional_yield y) {
  int result = 0;

  try {
    client.hset("lfuda", "localWeights", std::to_string(weight), [&result](cpp_redis::reply& reply) {
      if (!reply.is_null()) {
	result = reply.as_integer();
      }
    }); 

    client.sync_commit();
  } catch(std::exception &e) {
    return -1;
  }

  return result;
}

int LFUDAPolicy::get_local_weight_sum(optional_yield y) {
  /* FIXME: AMIN: for having several remote caches we need this part
    req.push("HEXISTS", dir->cct->_conf->rgw_local_cache_address, "localWeights");

  if (!std::get<0>(resp).value()) {
    int sum = 0;
    for (auto& entry : entries_map)
      sum += entry.second->localWeight; 
 
    if (set_local_weight_sum(sum, y) < 0) { // Initialize
      return -1;
    } else {
      return sum;
    }
  }
  */

  int weight = -1;

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
    auto it = entries_map.find(key);
    if (it == entries_map.end()) {
      delete victim;
      return -1;
    }

    int avgWeight = get_local_weight_sum(y) / entries_map.size();
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

    //FIXME: AMIN needs SSD driver to implement it.
    /*
    if (cacheDriver->del(dpp, key, y) < 0) 
      return -1;
      */

    ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): Block " << key << " has been evicted." << dendl;

    int weight = (avgWeight * entries_map.size()) - it->second->localWeight;
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

void LFUDAPolicy::update(const DoutPrefixProvider* dpp, std::string& key, uint64_t offset, uint64_t len, std::string version, int dirty, time_t lastAccessTime, optional_yield y)
{
  using handle_type = boost::heap::fibonacci_heap<LFUDAEntry*, boost::heap::compare<EntryComparator<LFUDAEntry>>>::handle_type;

  int age = get_age(); 
  int localWeight = age;
  auto entry = find_entry(key);
  if (entry != nullptr) { 
    entry->localWeight += age;
    localWeight = entry->localWeight;
  }  

  erase(dpp, key, y);
  
  const std::lock_guard l(lfuda_lock);
  LFUDAEntry *e = new LFUDAEntry(key, offset, len, version, dirty, lastAccessTime, localWeight);
  handle_type handle = entries_heap.push(e);
  e->set_handle(handle);
  entries_map.emplace(key, e);

  if (cacheDriver->set_attr(dpp, key, "user.rgw.localWeight", std::to_string(localWeight), y) < 0) 
    ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): CacheDriver set_attr method failed." << dendl;

  auto localWeights = get_local_weight_sum(y);
  localWeights += localWeight;
  if (set_local_weight_sum(localWeights, y) < 0)
    ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): Failed to update sum of local weights for the cache backend." << dendl;
}

bool LFUDAPolicy::erase(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y)
{
  const std::lock_guard l(lfuda_lock);
  auto p = entries_map.find(key);
  if (p == entries_map.end()) {
    return false;
  }

  auto localWeights = get_local_weight_sum(y);
  localWeights -= p->second->localWeight;
  if (set_local_weight_sum(localWeights, y) < 0)
    ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): Failed to update sum of local weights for the cache backend." << dendl;

  entries_map.erase(p);
  entries_heap.erase(p->second->handle);

  return true;
}

void LFUDAPolicy::cleaning(const DoutPrefixProvider* dpp)
{
  int interval = cct->_conf->rgw_d4n_cache_cleaning_interval;
  while(true){
    ldpp_dout(dpp, 20) << __func__ << " : " << " Cache cleaning!" << dendl;
     
    for (auto it = entries_map.begin(); it != entries_map.end(); it++){
      if ((it->second->dirty == 1) && (std::difftime(time(NULL), it->second->lastAccessTime) > interval)){ //if block is dirty and written more than interval seconds ago
        ldpp_dout(dpp, 20) << "AMIN: " << __func__ << " : " << " Cache cleaning object: " << it->first << dendl;
	//FIXME: we have found the object, we need to update map and heap and read it from the cache
        cacheDriver->cleaning(dpp);
      }
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  }
}

int LRUPolicy::exist_key(std::string key)
{
  const std::lock_guard l(lru_lock);
  if (entries_map.count(key) != 0) {
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
    entries_map.erase(entries_map.find(p.key));
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

void LRUPolicy::update(const DoutPrefixProvider* dpp, std::string& key, uint64_t offset, uint64_t len, std::string version, int dirty, time_t lastAccessTime, optional_yield y)
{
  erase(dpp, key);
  const std::lock_guard l(lru_lock);
  Entry *e = new Entry(key, offset, len);
  entries_lru_list.push_back(*e);
  entries_map.emplace(key, e);
}

bool LRUPolicy::erase(const DoutPrefixProvider* dpp, const std::string& key)
{
  const std::lock_guard l(lru_lock);
  auto p = entries_map.find(key);
  if (p == entries_map.end()) {
    return false;
  }
  entries_map.erase(p);
  entries_lru_list.erase_and_dispose(entries_lru_list.iterator_to(*(p->second)), Entry_delete_disposer());
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
