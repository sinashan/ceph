#include "d4n_policy.h"

#include <boost/lexical_cast.hpp>
#include "../../../common/async/yield_context.h"
#include "common/async/blocked_completion.h"
#include "common/dout.h" 

namespace rgw { namespace d4n {

// initiate a call to async_exec() on the connection's executor
struct initiate_exec {
  std::shared_ptr<boost::redis::connection> conn;

  using executor_type = boost::redis::connection::executor_type;
  executor_type get_executor() const noexcept { return conn->get_executor(); }

  template <typename Handler, typename Response>
  void operator()(Handler handler, const boost::redis::request& req, Response& resp)
  {
    auto h = boost::asio::consign(std::move(handler), conn);
    return boost::asio::dispatch(get_executor(), [c=conn, &req, &resp, h=std::move(h)] {
      c->async_exec(req, resp, std::move(h));
    });
  }
};

template <typename Response, typename CompletionToken>
auto async_exec(std::shared_ptr<connection> conn,
                const boost::redis::request& req,
                Response& resp, CompletionToken&& token)
{
  return boost::asio::async_initiate<CompletionToken,
         void(boost::system::error_code, std::size_t)>(
      initiate_exec{std::move(conn)}, token, req, resp);
}

template <typename T>
void redis_exec(std::shared_ptr<connection> conn, boost::system::error_code& ec, boost::redis::request& req, boost::redis::response<T>& resp, optional_yield y)
{
  if (y) {
    auto yield = y.get_yield_context();
    async_exec(std::move(conn), req, resp, yield[ec]);
  } else {
    async_exec(std::move(conn), req, resp, ceph::async::use_blocked[ec]);
  }
}

int LFUDAPolicy::set_age(int age, optional_yield y) {
  try {
    boost::system::error_code ec;
    response<int> resp;
    request req;
    req.push("HSET", "lfuda", "age", std::to_string(age));

    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      return -ec.value();
    }

    return std::get<0>(resp).value(); /* Returns number of fields set */
  } catch (std::exception &e) {
    return -EINVAL;
  }
}

int LFUDAPolicy::get_age(optional_yield y) {
  response<int> resp;

  try {
    boost::system::error_code ec;
    request req;
    req.push("HEXISTS", "lfuda", "age");

    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      return -ec.value();
    }
  } catch (std::exception &e) {
    return -EINVAL;
  }

  if (!std::get<0>(resp).value()) {
    if (set_age(1, y)) /* Initialize age */
      return 1;
    else
      return -ENOENT;
  }

  try { 
    boost::system::error_code ec;
    response<std::string> value;
    request req;
    req.push("HGET", "lfuda", "age");
      
    redis_exec(conn, ec, req, value, y);

    if (ec) {
      return -ec.value();
    }

    return std::stoi(std::get<0>(value).value());
  } catch (std::exception &e) {
    return -EINVAL;
  }
}

int LFUDAPolicy::set_local_weight_sum(int weight, optional_yield y) {
  weight = weight > 0 ? weight : 0;

  try {
    boost::system::error_code ec;
    response<int> resp;
    request req;
    req.push("HSET", dir->cct->_conf->rgw_local_cache_address, "localWeights", std::to_string(weight));

    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      return -ec.value();
    }

    return std::get<0>(resp).value(); /* Returns number of fields set */
  } catch (std::exception &e) {
    return -EINVAL;
  }
}

int LFUDAPolicy::get_local_weight_sum(optional_yield y) {
  response<int> resp;

  try {
    boost::system::error_code ec;
    request req;
    req.push("HEXISTS", dir->cct->_conf->rgw_local_cache_address, "localWeights");

    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      return -ec.value();
    }
  } catch (std::exception &e) {
    return -EINVAL;
  }

  if (!std::get<0>(resp).value()) {
    int sum = 0;
    for (auto& entry : entries_map)
      sum += entry.second->localWeight; 
 
    if (int ret = set_local_weight_sum(sum, y) < 0) { /* Initialize */ 
      return ret;
    } else {
      return sum;
    }
  }

  try { 
    boost::system::error_code ec;
    response<std::string> value;
    request req;
    req.push("HGET", dir->cct->_conf->rgw_local_cache_address, "localWeights");
      
    redis_exec(conn, ec, req, value, y);

    if (ec) {
      return -ec.value();
    }

    return std::stoi(std::get<0>(value).value());
  } catch (std::exception &e) {
    return -EINVAL;
  }
}

CacheBlock* LFUDAPolicy::get_victim_block(const DoutPrefixProvider* dpp, optional_yield y) {
  const std::lock_guard l(lfuda_lock);
  if (entries_heap.empty())
    return nullptr;

  /* Get victim cache block */
  std::string key = entries_heap.top()->key;
  CacheBlock* victim = new CacheBlock();

  victim->cacheObj.bucketName = key.substr(0, key.find('_')); 
  key.erase(0, key.find('_') + 1);
  victim->cacheObj.objName = key.substr(0, key.find('_'));
  victim->blockID = entries_heap.top()->offset;
  victim->size = entries_heap.top()->len;

  if (dir->get(victim, y) < 0) {
    return nullptr;
  }

  return victim;
}

int LFUDAPolicy::exist_key(std::string key) {
  const std::lock_guard l(lfuda_lock);
  if (entries_map.count(key) != 0) {
    return true;
  }

  return false;
}

#if 0
int LFUDAPolicy::get_block(const DoutPrefixProvider* dpp, CacheBlock* block, rgw::cache::CacheDriver* cacheDriver, optional_yield y) {
  response<std::string> resp;
  int age = get_age(y);

  if (exist_key(build_index(block->cacheObj.bucketName, block->cacheObj.objName, block->blockID, block->size))) { /* Local copy */
    auto it = entries_map.find(build_index(block->cacheObj.bucketName, block->cacheObj.objName, block->blockID, block->size));
    it->second->localWeight += age;
    return cacheDriver->set_attr(dpp, block->cacheObj.objName, "localWeight", std::to_string(it->second->localWeight), y);
  } else {
    if (eviction(dpp, block->size, cacheDriver, y) < 0)
      return -1; 

    int exists = dir->exist_key(block, y);
    if (exists > 0) { /* Remote copy */
      if (dir->get(block, y) < 0) {
	return -1;
      } else {
	if (!block->hostsList.empty()) { 
	  block->globalWeight += age;
	  
	  if (dir->update_field(block, "globalWeight", std::to_string(block->globalWeight), y) < 0) {
	    return -1;
	  } else {
	    return 0;
	  }
	} else {
          return -1;
        }
      }
    } else if (!exists) { /* No remote copy */
      // localWeight += age;
      //return cacheDriver->set_attr(dpp, block->cacheObj.objName, "localWeight", std::to_string(it->second->localWeight), y);
      return 0;
    } else {
      return -1;
    }
  }
}
#endif

int LFUDAPolicy::eviction(const DoutPrefixProvider* dpp, uint64_t size, optional_yield y) {
  uint64_t freeSpace = cacheDriver->get_free_space(dpp);

  while (freeSpace < size) { // TODO: Think about parallel reads and writes; can this turn into an infinite loop? 
    CacheBlock* victim = get_victim_block(dpp, y);

    if (victim == nullptr) {
      ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): Could not retrieve victim block." << dendl;
      delete victim;
      return -ENOENT;
    }

    const std::lock_guard l(lfuda_lock);
    std::string key = entries_heap.top()->key;
    auto it = entries_map.find(key);
    if (it == entries_map.end()) {
      delete victim;
      return -ENOENT;
    }

    int avgWeight = get_local_weight_sum(y);
    if (avgWeight < 0) {
      delete victim;
      return avgWeight;
    }

    avgWeight /= entries_map.size();

    if (victim->hostsList.size() == 1 && victim->hostsList[0] == dir->cct->_conf->rgw_local_cache_address) { /* Last copy */
      if (victim->globalWeight) {
	it->second->localWeight += victim->globalWeight;
        (*it->second->handle)->localWeight = it->second->localWeight;
	entries_heap.increase(it->second->handle);

	if (int ret = cacheDriver->set_attr(dpp, key, "user.rgw.localWeight", std::to_string(it->second->localWeight), y) < 0) { 
	  delete victim;
	  return ret;
        }

	victim->globalWeight = 0;
	if (int ret = dir->update_field(victim, "globalWeight", std::to_string(victim->globalWeight), y) < 0) {
	  delete victim;
	  return ret;
        }
      }

      if (it->second->localWeight > avgWeight) {
	// TODO: push victim block to remote cache
	// add remote cache host to host list
      }
    }

    victim->globalWeight += it->second->localWeight;
    if (int ret = dir->update_field(victim, "globalWeight", std::to_string(victim->globalWeight), y) < 0) {
      delete victim;
      return ret;
    }

    if (int ret = dir->remove_host(victim, dir->cct->_conf->rgw_local_cache_address, y) < 0) {
      delete victim;
      return ret;
    }

    delete victim;

    if (int ret = cacheDriver->del(dpp, key, y) < 0) 
      return ret;

    ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): Block " << key << " has been evicted." << dendl;

    int weight = (avgWeight * entries_map.size()) - it->second->localWeight;
    if (int ret = set_local_weight_sum(weight, y) < 0)
      return ret;

    int age = get_age(y);
    age = std::max(it->second->localWeight, age);
    if (int ret = set_age(age, y) < 0)
      return ret;

    erase(dpp, key, y);
    freeSpace = cacheDriver->get_free_space(dpp);
  }
  
  return 0;
}

void LFUDAPolicy::update(const DoutPrefixProvider* dpp, std::string& key, uint64_t offset, uint64_t len, std::string version, bool dirty, time_t creationTime, const rgw_user user, optional_yield y)
{
  using handle_type = boost::heap::fibonacci_heap<LFUDAEntry*, boost::heap::compare<EntryComparator<LFUDAEntry>>>::handle_type;
  const std::lock_guard l(lfuda_lock);

  int age = get_age(y); 
  if (age < 0) {
    ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): LFUDAPolicy get_age method failed." << dendl;
    return;
  }
  int localWeight = age;
  auto entry = find_entry(key);
  if (entry != nullptr) { 
    entry->localWeight += age;
    localWeight = entry->localWeight;
  }  

  erase(dpp, key, y);
  
  LFUDAEntry *e = new LFUDAEntry(key, offset, len, version, dirty, creationTime, user, localWeight);
  handle_type handle = entries_heap.push(e);
  e->set_handle(handle);
  entries_map.emplace(key, e);

  std::string oid_in_cache = key;
  if (dirty == true)
    oid_in_cache = "D_"+key;

  if (cacheDriver->set_attr(dpp, oid_in_cache, "user.rgw.localWeight", std::to_string(localWeight), y) < 0) 
    ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): CacheDriver set_attr method failed." << dendl;

  int localWeights = get_local_weight_sum(y);
  if (localWeights < 0) {
    ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): Failed to retrieve sum of local weights for the cache backend." << dendl;
    return;
  }

  localWeights += ((localWeight < 0) ? 0 : localWeight);
  if (set_local_weight_sum(localWeights, y) < 0)
    ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): Failed to update sum of local weights for the cache backend." << dendl;
}

void LFUDAPolicy::updateObj(const DoutPrefixProvider* dpp, std::string& key, std::string version, bool dirty, uint64_t size, time_t creationTime, const rgw_user user, std::string& etag, optional_yield y)
{
  eraseObj(dpp, key, y);
  
  const std::lock_guard l(lfuda_lock);
  LFUDAObjEntry *e = new LFUDAObjEntry(key, version, dirty, size, creationTime, user, etag);
  o_entries_map.emplace(key, e);
}


bool LFUDAPolicy::erase(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y)
{
  auto p = entries_map.find(key);
  if (p == entries_map.end()) {
    return false;
  }

  int localWeights = get_local_weight_sum(y);
  if (localWeights < 0) {
    ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): Failed to retrieve sum of local weights for the cache backend." << dendl;
    return false;
  }

  localWeights -= ((p->second->localWeight < 0) ? 0 : p->second->localWeight);
  if (set_local_weight_sum(localWeights, y) < 0) {
    ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): Failed to update sum of local weights for the cache backend." << dendl;
    return false;
  }

  entries_heap.erase(p->second->handle);
  entries_map.erase(p);

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
    uint64_t len = 0;
    rgw::sal::Attrs obj_attrs;
    int count = 0;

    for (auto it = o_entries_map.begin(); it != o_entries_map.end(); it++){
      //if ((it->second->dirty == true) && (std::difftime(time(NULL), it->second->creationTime) > interval)){ //if block is dirty and written more than interval seconds ago
      ldpp_dout(dpp, 20) << "AMIN: "<< __func__ << " : " << " Cache cleaning!" << "Finding a dirty object ... " << dendl;
      ldpp_dout(dpp, 20) << "AMIN: "<< __func__ << " : " << " Cache cleaning! dirty is: " << it->second->dirty << dendl;
      if ((it->second->dirty == true) && (std::difftime(time(NULL), it->second->creationTime) > interval)){ //if block is dirty and written more than interval seconds ago
	name = it->first;
        ldpp_dout(dpp, 20) << "AMIN: "<< __func__ << " : " << " Cache cleaning!" << "Object name is: " << name <<  dendl;
	rgw_user c_rgw_user = it->second->user;

	size_t pos = 0;
	std::string delimiter = "_";
	while ((pos = name.find(delimiter)) != std::string::npos) {
	  if (count == 0){
	    b_name = name.substr(0, pos);
    	    name.erase(0, pos + delimiter.length());
	  }
	  count ++;
	}
	key = name;

	//writing data to the backend
	//we need to create an atomic_writer
 	rgw_obj_key c_obj_key = rgw_obj_key(key); 		
	std::unique_ptr<rgw::sal::User> c_user = driver->get_user(c_rgw_user);

	std::unique_ptr<rgw::sal::Bucket> c_bucket;
        rgw_bucket c_rgw_bucket = rgw_bucket(c_rgw_user.tenant, b_name, "");

	RGWBucketInfo c_bucketinfo;
	c_bucketinfo.bucket = c_rgw_bucket;
	c_bucketinfo.owner = c_rgw_user;
	
	
    	ldpp_dout(dpp, 10) << __func__ << " : "  << __LINE__ << ": bucket name is " << b_name << dendl;
    	int ret = driver->load_bucket(dpp, c_rgw_bucket, &c_bucket, null_yield);
	if (ret < 0) {
      	  ldpp_dout(dpp, 10) << __func__ << "(): load_bucket() returned ret=" << ret << dendl;
      	  break;
        }
    	ldpp_dout(dpp, 10) << __func__ << " : "  << __LINE__ << ": bucket name is " << c_bucket->get_name() << dendl;
	/*
    	c_bucketinfo = bucket->get_info();
    	ldpp_dout(dpp, 10) << __func__ << " : "  << __LINE__ << ": bucket name is " << b_name << dendl;
	c_bucket = driver->get_bucket(c_bucketinfo);
    	ldpp_dout(dpp, 10) << __func__ << " : "  << __LINE__ << ": bucket name is " << c_bucket->get_name() << dendl;
	*/
	/*
 	int ret = driver->get_bucket(dpp, nullptr, c_rgw_bucket, &c_bucket, null_yield);
	if (ret < 0){
    		ldpp_dout(dpp, 10) << __func__ << " : "  << __LINE__ << " cleaning get_bucket() failed for Bucket: " << b_name << dendl;
		continue;
	}
	*/

	std::unique_ptr<rgw::sal::Object> c_obj = c_bucket->get_object(c_obj_key);

	std::unique_ptr<rgw::sal::Writer> processor =  driver->get_atomic_writer(dpp,
				  null_yield,
				  c_obj.get(),
				  c_user->get_id(),
				  NULL,
				  0,
				  "");

  	int op_ret = processor->prepare(null_yield);
  	if (op_ret < 0) {
    	  ldpp_dout(dpp, 20) << "processor->prepare() returned ret=" << op_ret << dendl;
    	  break;
  	}

	std::string prefix = b_name+"_"+key;
	off_t lst = it->second->size;
  	off_t fst = 0;
  	off_t ofs = 0;

	
  	rgw::sal::DataProcessor *filter = processor.get();
	do {
    	  ceph::bufferlist data;
    	  if (fst >= lst){
      	    break;
    	  }
    	  off_t cur_lst = std::min<off_t>(fst + cct->_conf->rgw_max_chunk_size, lst);
    	  std::string oid_in_cache = "D_" + prefix + "_" + std::to_string(fst) + "_" + std::to_string(cur_lst);  	  
    	  std::string new_oid_in_cache = prefix + "_" + std::to_string(fst) + "_" + std::to_string(cur_lst);
      	  ldpp_dout(dpp, 20) << __func__  << ": " << __LINE__ << ": oid_in_cache is: " << oid_in_cache << dendl;
    	  cacheDriver->get(dpp, oid_in_cache, fst, cur_lst, data, obj_attrs, null_yield);
    	  len = data.length();
    	  fst += len;

    	  if (len == 0) {
      	    break;
   	  }

      	  ldpp_dout(dpp, 20) << __func__  << ": " << __LINE__ << ": data is: " << data.to_str() << dendl;
    	  op_ret = filter->process(std::move(data), ofs);
    	  if (op_ret < 0) {
      	    ldpp_dout(dpp, 20) << "processor->process() returned ret="
          	<< op_ret << dendl;
      	    return;
    	  }

          ldpp_dout(dpp, 20) << "AMIN: " << __func__ << __LINE__ << dendl;
  	  rgw::d4n::CacheBlock block;
    	  block.cacheObj.bucketName = c_obj->get_bucket()->get_name();
    	  block.cacheObj.objName = c_obj->get_key().get_oid();
      	  block.size = len;
     	  block.blockID = ofs;
          ldpp_dout(dpp, 20) << "AMIN: " << __func__ << __LINE__ << dendl;
	  op_ret = dir->update_field(&block, "dirty", "false", null_yield); 
    	  if (op_ret < 0) {
      	    ldpp_dout(dpp, 20) << "updating dirty flag in Block directory failed!" << dendl;
      	    return;
    	  }

    	  cacheDriver->rename(dpp, oid_in_cache, new_oid_in_cache, null_yield);

          ldpp_dout(dpp, 20) << "AMIN: " << __func__ << __LINE__ << dendl;
    	  ofs += len;
  	} while (len > 0);

        ldpp_dout(dpp, 20) << "AMIN: " << __func__ << __LINE__ << dendl;
  	op_ret = filter->process({}, ofs);
        ldpp_dout(dpp, 20) << "AMIN: " << __func__ << __LINE__ << ": etag:" << it->second->etag << dendl;
	
  	const req_context rctx{dpp, null_yield, nullptr};
	ceph::real_time mtime = ceph::real_clock::from_time_t(it->second->creationTime);
        op_ret = processor->complete(lst, it->second->etag, &mtime, ceph::real_clock::from_time_t(it->second->creationTime), obj_attrs,
                               ceph::real_time(), nullptr, nullptr,
                               nullptr, nullptr, nullptr,
                               rctx, rgw::sal::FLAG_LOG_OP);

        ldpp_dout(dpp, 20) << "AMIN: " << __func__ << __LINE__ << dendl;
	//data is clean now, updating in-memory metadata
	it->second->dirty = false;
        //FIXME: AMIN:  should we update Object	Directory too?
      }
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(interval));
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

  return 0;
}

void LRUPolicy::update(const DoutPrefixProvider* dpp, std::string& key, uint64_t offset, uint64_t len, std::string version, bool dirty, time_t creationTime, const rgw_user user, optional_yield y)
{
  const std::lock_guard l(lru_lock);
  _erase(dpp, key, y);
  Entry *e = new Entry(key, offset, len, version, dirty, creationTime, user);
  entries_lru_list.push_back(*e);
  entries_map.emplace(key, e);
}

void LRUPolicy::updateObj(const DoutPrefixProvider* dpp, std::string& key, std::string version, bool dirty, uint64_t size, time_t creationTime, const rgw_user user, std::string& etag, optional_yield y)
{
  eraseObj(dpp, key, y);
  const std::lock_guard l(lru_lock);
  ObjEntry *e = new ObjEntry(key, version, dirty, size, creationTime, user, etag);
  o_entries_map.emplace(key, e);
  return;
}


bool LRUPolicy::erase(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y)
{
  const std::lock_guard l(lru_lock);
  return _erase(dpp, key, y);
}

bool LRUPolicy::_erase(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y)
{
  auto p = entries_map.find(key);
  if (p == entries_map.end()) {
    return false;
  }
  entries_map.erase(p);
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

} } // namespace rgw::d4n
