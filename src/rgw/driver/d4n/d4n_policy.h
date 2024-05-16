#pragma once

#include <boost/asio/awaitable.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/heap/fibonacci_heap.hpp>
#include <boost/system/detail/errc.hpp>

#include "d4n_directory.h"
#include "rgw_sal_d4n.h"
#include "rgw_cache_driver.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw::sal {
  class D4NFilterObject;
}

namespace rgw { namespace d4n {

namespace asio = boost::asio;
namespace sys = boost::system;


class CachePolicy {
  protected:
    struct Entry : public boost::intrusive::list_base_hook<> {
      std::string key;
      uint64_t offset;
      uint64_t len;
      std::string version;
      bool dirty;
      time_t creationTime;
      rgw_user user;
      Entry(std::string& key, uint64_t offset, uint64_t len, std::string version, bool dirty, time_t creationTime, rgw_user user) : key(key), offset(offset), 
                                                                                     len(len), version(version), dirty(dirty), creationTime(creationTime), user(user) {}
    };

   
    //The disposer object function
    struct Entry_delete_disposer {
      void operator()(Entry *e) {
        delete e;
      }
    };

    struct ObjEntry : public boost::intrusive::list_base_hook<> {
      std::string key;
      std::string version;
      bool dirty;
      uint64_t size;
      time_t creationTime;
      rgw_user user;
      std::string etag;
      ObjEntry(std::string& key, std::string version, bool dirty, uint64_t size, time_t creationTime, rgw_user user, std::string& etag) : key(key), version(version), dirty(dirty), size(size), creationTime(creationTime), user(user), etag(etag) {}
    };

    struct ObjEntry_delete_disposer {
      void operator()(ObjEntry *e) {
        delete e;
      }
    };

  public:
    CachePolicy() {}
    virtual ~CachePolicy() = default; 

    virtual int init(CephContext *cct, const DoutPrefixProvider* dpp, asio::io_context& io_context, rgw::sal::Driver *_driver) = 0;
    //virtual int init(CephContext *cct, const DoutPrefixProvider* dpp, rgw::sal::Driver *_driver) = 0;
    virtual int exist_key(std::string key) = 0;
    virtual int eviction(const DoutPrefixProvider* dpp, uint64_t size, optional_yield y) = 0;
    virtual void update(const DoutPrefixProvider* dpp, std::string& key, uint64_t offset, uint64_t len, std::string version, bool dirty, time_t creationTime, const rgw_user user, optional_yield y) = 0;
    virtual void updateObj(const DoutPrefixProvider* dpp, std::string& key, std::string version, bool dirty, uint64_t size, time_t creationTime, const rgw_user user, std::string& etag, optional_yield y) = 0;
    virtual bool erase(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y) = 0;
    virtual bool eraseObj(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y) = 0;
    virtual void cleaning(const DoutPrefixProvider* dpp) = 0;
};

class LFUDAPolicy : public CachePolicy {
  private:
    template<typename T>
    struct EntryComparator {
      bool operator()(T* const e1, T* const e2) const {
	return e1->localWeight > e2->localWeight;
      }
    }; 

    struct LFUDAEntry : public Entry {
      int localWeight;
      using handle_type = boost::heap::fibonacci_heap<LFUDAEntry*, boost::heap::compare<EntryComparator<LFUDAEntry>>>::handle_type;
      handle_type handle;

      LFUDAEntry(std::string& key, uint64_t offset, uint64_t len, std::string& version, bool dirty, time_t creationTime, rgw_user user, int localWeight) : Entry(key, offset, len, version, dirty, creationTime, user), localWeight(localWeight) {}
      
      void set_handle(handle_type handle_) { handle = handle_; } 
    };

    struct LFUDAObjEntry : public ObjEntry {
      LFUDAObjEntry(std::string& key, std::string& version, bool dirty, uint64_t size, time_t creationTime, rgw_user user, std::string& etag) : ObjEntry(key, version, dirty, size, creationTime, user, etag) {}
    };

    using Heap = boost::heap::fibonacci_heap<LFUDAEntry*, boost::heap::compare<EntryComparator<LFUDAEntry>>>;
    Heap entries_heap;
    std::unordered_map<std::string, LFUDAEntry*> entries_map;
    std::unordered_map<std::string, LFUDAObjEntry*> o_entries_map;
    std::mutex lfuda_lock;

    int age = 1, weightSum = 0, postedSum = 0;
    optional_yield y = null_yield;

    //net::io_context& io;
    std::shared_ptr<connection> conn;
    BlockDirectory* dir;
    rgw::cache::CacheDriver* cacheDriver;
    std::optional<asio::steady_timer> rthread_timer;
    rgw::sal::Driver *driver;
    std::thread tc;

    int sendRemote(const DoutPrefixProvider* dpp, CacheBlock *victim, std::string remoteCacheAddress, std::string key, bufferlist* out_bl, optional_yield y);
    int getMinAvgWeight(const DoutPrefixProvider* dpp, int minAvgWeight, std::string cache_address, optional_yield y);
    CacheBlock* get_victim_block(const DoutPrefixProvider* dpp, optional_yield y);
    int age_sync(const DoutPrefixProvider* dpp, optional_yield y); 
    int local_weight_sync(const DoutPrefixProvider* dpp, optional_yield y); 
    asio::awaitable<void> redis_sync(const DoutPrefixProvider* dpp, optional_yield y);
    void rthread_stop() {
      std::lock_guard l{lfuda_lock};

      if (rthread_timer) {
	rthread_timer->cancel();
      }
    }
    LFUDAEntry* find_entry(std::string key) { 
      auto it = entries_map.find(key); 
      if (it == entries_map.end())
        return nullptr;
      return it->second;
    }

  public:
    LFUDAPolicy(std::shared_ptr<connection>& conn, rgw::cache::CacheDriver* cacheDriver) : CachePolicy(), 
    //LFUDAPolicy(net::io_context& io_context, rgw::cache::CacheDriver* cacheDriver) : CachePolicy(), 
											   //io(io_context),
											   conn(conn), 
											   cacheDriver(cacheDriver)
    {
      //conn = std::make_shared<connection>(boost::asio::make_strand(io_context));
      //dir = new BlockDirectory{io};
      dir = new BlockDirectory{conn};
    }
    ~LFUDAPolicy() {
      rthread_stop();
      //shutdown();
      delete dir;
    } 

    virtual int init(CephContext *cct, const DoutPrefixProvider* dpp, asio::io_context& io_context, rgw::sal::Driver *_driver) override; 
    /*
    virtual int init(CephContext *cct, const DoutPrefixProvider* dpp, rgw::sal::Driver *_driver){

      std::string address = cct->_conf->rgw_filter_address;
      config cfg;
      cfg.addr.host = address.substr(0, address.find(":"));
      cfg.addr.port = address.substr(address.find(":") + 1, address.length());
      cfg.clientname = "D4N.Policy";

      if (!cfg.addr.host.length() || !cfg.addr.port.length()) {
	ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): Endpoint was not configured correctly." << dendl;
	return -EDESTADDRREQ;
      }

      dir->init(cct, dpp);
      conn->async_run(cfg, {}, net::consign(net::detached, conn));

      driver = _driver;

      // Spawn write cache cleaning thread 
      if (dpp->get_cct()->_conf->d4n_writecache_enabled == true){
        tc = std::thread(&CachePolicy::cleaning, this, dpp);
        tc.detach();
      }

      int result = 0;
      response<int, int, int, int> resp;

       try {
        boost::system::error_code ec;
        request req;
        req.push("HEXISTS", "lfuda", "age"); 
        req.push("HSET", "lfuda", "minLocalWeights_sum", std::to_string(weightSum)); // New cache node will always have the minimum average weight 
        req.push("HSET", "lfuda", "minLocalWeights_size", std::to_string(entries_map.size()));
        req.push("HSET", "lfuda", "minLocalWeights_address", dpp->get_cct()->_conf->rgw_local_cache_address);
  
    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    result = std::min(std::get<1>(resp).value(), std::min(std::get<2>(resp).value(), std::get<3>(resp).value()));
  } catch (std::exception &e) {
    ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  if (!std::get<0>(resp).value()) { // Only set maximum age if it doesn't exist 
    try {
      boost::system::error_code ec;
      response<int> value;
      request req;
      req.push("HSET", "lfuda", "age", age);
    
      redis_exec(conn, ec, req, value, y);

      if (ec) {
	ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "() ERROR: " << ec.what() << dendl;
	return -ec.value();
      }

      result = std::min(result, std::get<0>(value).value());
    } catch (std::exception &e) {
      ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "() ERROR: " << e.what() << dendl;
      return -EINVAL;
    }
  }

  // Spawn redis sync thread
    asio::co_spawn(io_context.get_executor(),
		   redis_sync(dpp, y), asio::detached);



      return 0;
    } 
    */

    virtual int exist_key(std::string key) override;
    virtual int eviction(const DoutPrefixProvider* dpp, uint64_t size, optional_yield y) override;
    virtual void update(const DoutPrefixProvider* dpp, std::string& key, uint64_t offset, uint64_t len, std::string version, bool dirty, time_t creationTime, const rgw_user user, optional_yield y) override;
    virtual void updateObj(const DoutPrefixProvider* dpp, std::string& key, std::string version, bool dirty, uint64_t size, time_t creationTime, const rgw_user user, std::string& etag, optional_yield y) override;
    virtual bool erase(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y) override;
    virtual bool eraseObj(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y) override;
    virtual void cleaning(const DoutPrefixProvider* dpp) override;
    void save_y(optional_yield y) { this->y = y; }
    //void shutdown();
};

class LRUPolicy : public CachePolicy {
  private:
    typedef boost::intrusive::list<Entry> List;

    std::unordered_map<std::string, Entry*> entries_map;
    std::unordered_map<std::string, ObjEntry*> o_entries_map;
    std::mutex lru_lock;
    List entries_lru_list;
    rgw::cache::CacheDriver* cacheDriver;

    bool _erase(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y);

  public:
    LRUPolicy(rgw::cache::CacheDriver* cacheDriver) : cacheDriver{cacheDriver} {}

    virtual int init(CephContext *cct, const DoutPrefixProvider* dpp, asio::io_context& io_context, rgw::sal::Driver* _driver) override { return 0; } 
    virtual int exist_key(std::string key) override;
    virtual int eviction(const DoutPrefixProvider* dpp, uint64_t size, optional_yield y) override;
    virtual void update(const DoutPrefixProvider* dpp, std::string& key, uint64_t offset, uint64_t len, std::string version, bool dirty, time_t creationTime, const rgw_user user, optional_yield y) override;
    virtual void updateObj(const DoutPrefixProvider* dpp, std::string& key, std::string version, bool dirty, uint64_t size, time_t creationTime, const rgw_user user, std::string& etag, optional_yield y) override;
    virtual bool erase(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y) override;
    virtual bool eraseObj(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y) override;
    virtual void cleaning(const DoutPrefixProvider* dpp) override {}
};

class PolicyDriver {
  private:
    std::string policyName;
    CachePolicy* cachePolicy;

  public:
    PolicyDriver(std::shared_ptr<connection>& conn, rgw::cache::CacheDriver* cacheDriver, std::string _policyName) : policyName(_policyName) 
    //PolicyDriver(net::io_context& io_context, rgw::cache::CacheDriver* cacheDriver, std::string _policyName) : policyName(_policyName)
    {
      if (policyName == "lfuda") {
	cachePolicy = new LFUDAPolicy(conn, cacheDriver);
	//cachePolicy = new LFUDAPolicy(io_context, cacheDriver);
      } else if (policyName == "lru") {
	cachePolicy = new LRUPolicy(cacheDriver);
      }
    }
    ~PolicyDriver() {
      delete cachePolicy;
    }

    CachePolicy* get_cache_policy() { return cachePolicy; }
    std::string get_policy_name() { return policyName; }
};

} } // namespace rgw::d4n
