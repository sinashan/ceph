#pragma once

#include <string>
#include <iostream>
#include <thread>
#include <boost/heap/fibonacci_heap.hpp>
#include "d4n_directory.h"
#include "rgw_sal_d4n.h"
#include "rgw_cache_driver.h"

namespace rgw::sal {
  class D4NFilterObject;
}

namespace rgw { namespace d4n {

class CachePolicy {
  protected:
    struct Entry : public boost::intrusive::list_base_hook<> {
      std::string key;
      uint64_t offset;
      uint64_t len;
      std::string version;
      bool dirty;
      std::string creationTime;
      rgw_user user;
      Entry(std::string& key, uint64_t offset, uint64_t len, std::string version, bool dirty, std::string creationTime, rgw_user user) : key(key), offset(offset), 
                                                                                     len(len), version(version), dirty(dirty), creationTime(creationTime), user(user) {}
    };

    struct ObjEntry : public boost::intrusive::list_base_hook<> {
      std::string key;
      std::string version;
      bool dirty;
      uint64_t size;
      std::string creationTime;
      rgw_user user;
      ObjEntry(std::string& key, std::string version, bool dirty, uint64_t size, std::string creationTime, rgw_user user) : key(key), version(version), dirty(dirty), size(size), creationTime(creationTime), user(user) {}
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


  public:
    std::thread tc;
    rgw::sal::Driver *driver;
    CephContext* cct;
 
    CachePolicy() {}
    virtual ~CachePolicy() = default; 

    virtual void init(CephContext *_cct, const DoutPrefixProvider* dpp, rgw::sal::Driver* driver) = 0;
    virtual int exist_key(std::string key) = 0;
    virtual int eviction(const DoutPrefixProvider* dpp, uint64_t size, optional_yield y) = 0;
    virtual void update(const DoutPrefixProvider* dpp, std::string& key, uint64_t offset, uint64_t len, std::string version, bool dirty, std::string creationTime, const rgw_user user, optional_yield y) = 0;
    virtual void updateObj(const DoutPrefixProvider* dpp, std::string& key, std::string version, bool dirty, uint64_t size, std::string creationTime, const rgw_user user, optional_yield y) = 0;
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

      LFUDAEntry(std::string& key, uint64_t offset, uint64_t len, std::string& version, bool dirty, std::string creationTime, rgw_user user, int localWeight) : Entry(key, offset, len, version, dirty, creationTime, user), localWeight(localWeight) {}
      
      void set_handle(handle_type handle_) { handle = handle_; } 
    };

    struct LFUDAObjEntry : public ObjEntry {
      LFUDAObjEntry(std::string& key, std::string& version, bool dirty, uint64_t size, std::string creationTime, rgw_user user) : ObjEntry(key, version, dirty, size, creationTime, user) {}
    };


    using Heap = boost::heap::fibonacci_heap<LFUDAEntry*, boost::heap::compare<EntryComparator<LFUDAEntry>>>;
    Heap entries_heap;
    std::unordered_map<std::string, LFUDAEntry*> b_entries_map;
    std::unordered_map<std::string, LFUDAObjEntry*> o_entries_map;
    std::mutex lfuda_lock;

    std::shared_ptr<connection> conn;
    BlockDirectory* dir;
    rgw::cache::CacheDriver* cacheDriver;

    int set_age(int age, optional_yield y);
    int get_age(optional_yield y);
    int set_local_weight_sum(int weight, optional_yield y);
    int get_local_weight_sum(optional_yield y);
    CacheBlock* get_victim_block(const DoutPrefixProvider* dpp, optional_yield y);

  public:
    LFUDAPolicy(std::shared_ptr<connection>& conn, rgw::cache::CacheDriver* cacheDriver) : CachePolicy(), 
                                                                         conn(conn), 
                                                                         cacheDriver(cacheDriver)
    {
      dir = new BlockDirectory{conn};
    }
    ~LFUDAPolicy() {
      delete dir;
    } 

    virtual void init(CephContext *_cct, const DoutPrefixProvider* dpp, rgw::sal::Driver *_driver) override;
    virtual int exist_key(std::string key) override;
    //virtual int get_block(const DoutPrefixProvider* dpp, CacheBlock* block, rgw::cache::CacheDriver* cacheNode, optional_yield y) override;
    virtual int eviction(const DoutPrefixProvider* dpp, uint64_t size, optional_yield y) override;
    virtual void update(const DoutPrefixProvider* dpp, std::string& key, uint64_t offset, uint64_t len, std::string version, bool dirty, std::string creationTime, const rgw_user user, optional_yield y) override;
    virtual void updateObj(const DoutPrefixProvider* dpp, std::string& key, std::string version, bool dirty, uint64_t size, std::string creationTime, const rgw_user user, optional_yield y) override;
    virtual bool erase(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y) override;
    virtual bool eraseObj(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y) override;
    virtual void cleaning(const DoutPrefixProvider* dpp) override;

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
};

class LRUPolicy : public CachePolicy {
  private:
    typedef boost::intrusive::list<Entry> List;

    std::unordered_map<std::string, Entry*> b_entries_map;
    std::unordered_map<std::string, ObjEntry*> o_entries_map;
    std::mutex lru_lock;
    List entries_lru_list;
    rgw::cache::CacheDriver* cacheDriver;

    bool _erase(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y);

  public:
    LRUPolicy(rgw::cache::CacheDriver* cacheDriver) : cacheDriver{cacheDriver} {}

    virtual void init(CephContext *_cct, const DoutPrefixProvider* dpp, rgw::sal::Driver* _driver) {}
    virtual int exist_key(std::string key) override;
    virtual int eviction(const DoutPrefixProvider* dpp, uint64_t size, optional_yield y) override;
    virtual bool erase(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y) override;
    virtual bool eraseObj(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y) override;
    virtual void update(const DoutPrefixProvider* dpp, std::string& key, uint64_t offset, uint64_t len, std::string version, bool dirty, std::string creationTime, const rgw_user user, optional_yield y) override;
    virtual void updateObj(const DoutPrefixProvider* dpp, std::string& key, std::string version, bool dirty, uint64_t size, std::string creationTime, const rgw_user user, optional_yield y) override;
    virtual void cleaning(const DoutPrefixProvider* dpp) override {}
};

class PolicyDriver {
  private:
    std::string policyName;
    CachePolicy* cachePolicy;

  public:
    PolicyDriver(std::shared_ptr<connection>& conn, rgw::cache::CacheDriver* cacheDriver, std::string _policyName) : policyName(_policyName) 
    {
      if (policyName == "lfuda") {
	cachePolicy = new LFUDAPolicy(conn, cacheDriver);
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
