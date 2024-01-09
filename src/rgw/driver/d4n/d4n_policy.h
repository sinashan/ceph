#ifndef CEPH_D4NPOLICY_H
#define CEPH_D4NPOLICY_H

#include <string>
#include <iostream>
#include <thread>
#include <chrono>
#include <cpp_redis/cpp_redis>
#include "rgw_common.h"
#include "d4n_directory.h"
#include "rgw_sal_d4n.h"
#include "../../rgw_redis_driver.h"
#include <boost/heap/fibonacci_heap.hpp>

namespace rgw::sal {
  class D4NFilterDriver;
  class D4NFilterBucket;
}

namespace rgw { namespace d4n {

class CachePolicy {
  protected:
    struct Entry : public boost::intrusive::list_base_hook<> {
      std::string key;
      uint64_t offset;
      uint64_t len;
      std::string version;
      int dirty;
      time_t lastAccessTime;
      rgw_user user;
      Entry(std::string& key, uint64_t offset, uint64_t len, std::string version, int dirty, time_t lastAccessTime, rgw_user user) : key(key), offset(offset), 
                                                                                     len(len), version(version), dirty(dirty), lastAccessTime(lastAccessTime), user(user) {}
    };

    struct ObjEntry : public boost::intrusive::list_base_hook<> {
      std::string key;
      std::string version;
      int dirty;
      uint64_t size;
      time_t lastAccessTime;
      rgw_user user;
      ObjEntry(std::string& key, std::string version, int dirty, uint64_t size, time_t lastAccessTime, rgw_user user) : key(key), version(version), dirty(dirty), size(size), lastAccessTime(lastAccessTime), user(user) {}
    };

    
    //The disposer object function
    struct Entry_delete_disposer {
      void operator()(Entry *e) {
        delete e;
      }
    };

    struct ObjEntry_delete_disposer {
      void operator()(ObjEntry *e) {
        delete e;
      }
    };


  private:
    cpp_redis::client client;

  public:
    CephContext* cct;
    Address addr;
    std::thread tc;
    rgw::sal::Driver *driver; 

    CachePolicy() : addr() {}
    virtual ~CachePolicy() = default;

    virtual int init(CephContext *_cct, const DoutPrefixProvider* dpp, rgw::sal::Driver* driver) {return 0;}

    int find_client(cpp_redis::client* client);
    virtual int exist_key(std::string key) = 0;
    //virtual int get_block(const DoutPrefixProvider* dpp, CacheBlock* block, rgw::cache::CacheDriver* cacheNode) = 0;
    virtual int eviction(const DoutPrefixProvider* dpp, uint64_t size, optional_yield y) = 0;

    virtual bool erase(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y) = 0;
    virtual bool eraseObj(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y) = 0;

    virtual std::string update(const DoutPrefixProvider* dpp, std::string& key, uint64_t offset, uint64_t len, std::string version, int dirty, time_t lastAccessTime, const rgw_user user, optional_yield y) = 0;
    virtual void updateObj(const DoutPrefixProvider* dpp, std::string& key, std::string version, int dirty, uint64_t size, time_t lastAccessTime, const rgw_user user, optional_yield y) = 0;

    virtual void cleaning(const DoutPrefixProvider* dpp) = 0;
    virtual Address get_addr() { return addr; }
    virtual void shutdown() = 0;
};

class LFUDAPolicy : public CachePolicy {
  private:
    cpp_redis::client client;
    BlockDirectory* dir;
    rgw::cache::CacheDriver* cacheDriver;

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

      LFUDAEntry(std::string& key, uint64_t offset, uint64_t len, std::string& version, int dirty, time_t lastAccessTime, rgw_user user, int localWeight) : Entry(key, offset, len, version, dirty, lastAccessTime, user),
                                                                                                            localWeight(localWeight) {}

      void set_handle(handle_type handle_) { handle = handle_; }
    };

    struct LFUDAObjEntry : public ObjEntry {
      LFUDAObjEntry(std::string& key, std::string& version, int dirty, uint64_t size, time_t lastAccessTime, rgw_user user) : ObjEntry(key, version, dirty, size, lastAccessTime, user) {}

    };


    using Heap = boost::heap::fibonacci_heap<LFUDAEntry*, boost::heap::compare<EntryComparator<LFUDAEntry>>>;
    Heap entries_heap;
    std::unordered_map<std::string, LFUDAEntry*> b_entries_map;
    std::unordered_map<std::string, LFUDAObjEntry*> o_entries_map;
    std::mutex lfuda_lock;

  public:
    LFUDAPolicy(rgw::cache::CacheDriver* _cacheDriver) : CachePolicy(), cacheDriver(_cacheDriver)
    {
      dir = new BlockDirectory{};
    }

    ~LFUDAPolicy() {
      //shutdown();
      delete dir;
    }

    virtual int init(CephContext *_cct, const DoutPrefixProvider* dpp, rgw::sal::Driver *_driver) override;
    int set_dirty(std::string key, int dirty, optional_yield y);
    int set_age(int age, const DoutPrefixProvider* dpp);
    int get_age(const DoutPrefixProvider* dpp);
    int set_global_weight(std::string key, int weight);
    int get_global_weight(std::string key);
    int set_local_weight_sum(size_t weight, optional_yield y);
    int get_local_weight_sum(optional_yield y);
    int set_min_avg_weight(size_t weight, std::string cacheLocation);
    int get_min_avg_weight();
    CacheBlock* find_victim(const DoutPrefixProvider* dpp);
    virtual bool erase(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y) override ;
    virtual bool eraseObj(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y) override;

    virtual void shutdown() override;

    void set_local_weight(std::string& key, int localWeight);
    LFUDAEntry* find_entry(std::string key) {
      auto it = b_entries_map.find(key);
      if (it == b_entries_map.end())
        return nullptr;
      return it->second;
    }

    LFUDAObjEntry* find_obj_entry(std::string key) {
      auto it = o_entries_map.find(key);
      if (it == o_entries_map.end())
        return nullptr;
      return it->second;
    }


    //virtual int find_client(const DoutPrefixProvider* dpp, cpp_redis::client* client) override { return CachePolicy::find_client(dpp, client); }
    virtual int exist_key(std::string key) override { return CachePolicy::exist_key(key); }
    //virtual int get_block(const DoutPrefixProvider* dpp, CacheBlock* block, rgw::cache::CacheDriver* cacheNode) override;
    virtual int eviction(const DoutPrefixProvider* dpp, uint64_t size, optional_yield y) override;
    virtual std::string update(const DoutPrefixProvider* dpp, std::string& key, uint64_t offset, uint64_t len, std::string version, int dirty, time_t lastAccessTime, const rgw_user user, optional_yield y) override;
    virtual void updateObj(const DoutPrefixProvider* dpp, std::string& key, std::string version, int dirty, uint64_t size, time_t lastAccessTime, const rgw_user user, optional_yield y) override;
    virtual void cleaning(const DoutPrefixProvider* dpp) override;
    rgw::cache::CacheDriver* get_cacheDriver(){return cacheDriver;}
};

class LRUPolicy : public CachePolicy {
  public:
    struct Entry : public boost::intrusive::list_base_hook<> {
      std::string key;
      uint64_t offset;
      uint64_t len;
      Entry(std::string& key, uint64_t offset, uint64_t len) : key(key), offset(offset), len(len) {}
    };

    struct ObjEntry : public boost::intrusive::list_base_hook<> {
      std::string key;
      uint64_t size;
      ObjEntry(std::string& key, uint64_t size) : key(key), size(size) {}
    };


    LRUPolicy(rgw::cache::CacheDriver* _cacheDriver) : CachePolicy(), cacheDriver(_cacheDriver)
    {
      dir = new BlockDirectory{};
    }

    ~LRUPolicy() {
      //shutdown();
      delete dir;
    }

    virtual int init(CephContext *_cct, const DoutPrefixProvider* dpp, rgw::sal::Driver* _driver) {return 0;}
 
  private:
    std::mutex lru_lock;
    //The disposer object function
    struct Entry_delete_disposer {
      void operator()(Entry *e) {
	      delete e;
      }
    };

    struct ObjEntry_delete_disposer {
      void operator()(ObjEntry *e) {
	      delete e;
      }
    };


    BlockDirectory* dir;
    rgw::cache::CacheDriver* cacheDriver;

    typedef boost::intrusive::list<Entry> List;
    List entries_lru_list;
    std::unordered_map<std::string, Entry*> b_entries_map;
    std::unordered_map<std::string, ObjEntry*> o_entries_map;
  public:
    //virtual int find_client(const DoutPrefixProvider* dpp, cpp_redis::client* client) override { return 0; };
    virtual int exist_key(std::string key) override;
    //virtual int get_block(const DoutPrefixProvider* dpp, CacheBlock* block, rgw::cache::CacheDriver* cacheNode) override;
    virtual int eviction(const DoutPrefixProvider* dpp, uint64_t size, optional_yield y) override;

    virtual std::string update(const DoutPrefixProvider* dpp, std::string& key, uint64_t offset, uint64_t len, std::string version, int dirty, time_t lastAccessTime, const rgw_user user, optional_yield y) override;
    virtual void updateObj(const DoutPrefixProvider* dpp, std::string& key, std::string version, int dirty, uint64_t size, time_t lastAccessTime, const rgw_user user, optional_yield y) override;

    virtual bool erase(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y) override;
    virtual bool eraseObj(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y) override;

    virtual void cleaning(const DoutPrefixProvider* dpp) override {}
    rgw::cache::CacheDriver* get_cacheDriver(){return cacheDriver;}
    virtual void shutdown() override;
};

class PolicyDriver {
  private:
    std::string policyName;
    CachePolicy* cachePolicy;

  public:

    PolicyDriver(rgw::cache::CacheDriver *cacheDriver, std::string _policyName) : policyName(_policyName) 
    {
      if (policyName == "lfuda") {
	cachePolicy = new LFUDAPolicy(cacheDriver);
      } else if (policyName == "lru") {
	cachePolicy = new LRUPolicy(cacheDriver);
      }
    }

    ~PolicyDriver() {
      delete cachePolicy;
    }

    //int init();

    CachePolicy* get_cache_policy() { return cachePolicy; }
    std::string get_policy_name() { return policyName; }
};

} } // namespace rgw::d4n

#endif
