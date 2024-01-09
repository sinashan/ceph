// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2022 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "rgw_redis_driver.h"
#include "rgw_ssd_driver.h"
#include "rgw_sal_d4n.h"

#define dout_subsys ceph_subsys_rgw
#define dout_context g_ceph_context

namespace rgw { namespace sal {

using namespace std;

static inline User* nextUser(User* t)
{
  if (!t)
    return nullptr;

  return dynamic_cast<FilterUser*>(t)->get_next();
}

static inline Bucket* nextBucket(Bucket* t)
{
  if (!t)
    return nullptr;

  return dynamic_cast<FilterBucket*>(t)->get_next();
}

static inline Object* nextObject(Object* t)
{
  if (!t)
    return nullptr;
  
  return dynamic_cast<FilterObject*>(t)->get_next();
}

D4NFilterDriver::D4NFilterDriver(Driver* _next) : FilterDriver(_next) 
{
  rgw::cache::Partition partition_info;
  partition_info.location = g_conf()->rgw_d4n_l1_datacache_persistent_path;
  partition_info.name = "d4n";
  partition_info.type = "read-cache";
  partition_info.size = g_conf()->rgw_d4n_l1_datacache_size;

  cacheDriver = new rgw::cache::SSDDriver(partition_info);
  objDir = new rgw::d4n::ObjectDirectory();
  blockDir = new rgw::d4n::BlockDirectory();
  cacheBlock = new rgw::d4n::CacheBlock();
  policyDriver = new rgw::d4n::PolicyDriver(cacheDriver, "lfuda");
}

 D4NFilterDriver::~D4NFilterDriver()
 {
    delete cacheDriver;
    delete objDir; 
    delete blockDir; 
    delete cacheBlock;
    delete policyDriver;
}

int D4NFilterDriver::initialize(CephContext *cct, const DoutPrefixProvider *dpp)
{
  FilterDriver::initialize(cct, dpp);
  this->cct = cct;
  cacheDriver->initialize(cct, dpp);

  objDir->init(cct);
  blockDir->init(cct);

  policyDriver->get_cache_policy()->init(cct, dpp, next);

  return 0;
}

std::unique_ptr<User> D4NFilterDriver::get_user(const rgw_user &u)
{
  std::unique_ptr<User> user = next->get_user(u);

  return std::make_unique<D4NFilterUser>(std::move(user), this);
}

int D4NFilterDriver::get_bucket(const DoutPrefixProvider* dpp, User* u, const rgw_bucket& b, std::unique_ptr<Bucket>* bucket_out, optional_yield y)
{
  std::unique_ptr<Bucket> nb;
  int ret;
  
  ldpp_dout(dpp, 20) << " AMIN: " << __func__ << " : " << __LINE__ << dendl;
  
  User* nu = nextUser(u);
  ret = next->get_bucket(dpp, nu, b, &nb, y);
  if (ret != 0)
    return ret;

/*
  rgw_placement_rule placement_rule;
  placement_rule.name = "";
  placement_rule.storage_class = "";
  std::string swift_ver_location = "";
  RGWAccessControlPolicy policy;
  Attrs attrs;
  RGWBucketInfo info;
  obj_version ep_objv;
  bool exclusive = false;
  bool obj_lock_enabled = false;
  bool existed;
  RGWEnv env;
  req_info req_info(this->cct, &env);
  

  ret = nu->create_bucket(dpp, b, "", placement_rule, swift_ver_location, nullptr, policy, attrs, info, ep_objv, exclusive, obj_lock_enabled, &existed, req_info, &nb, y);
  ldpp_dout(dpp, 20) << "AMIN: " << __func__ << " return is: " << ret << dendl;
*/

  D4NFilterBucket* fb = new D4NFilterBucket(std::move(nb), nu, this);

 /*
  info.bucket = b;
  info.owner = u->get_id();
 */
  bucket_out->reset(fb);
  ldpp_dout(dpp, 20) << " AMIN: " << __func__ << " : " << __LINE__ << dendl;

  return 0;
}

/*
int D4NFilterDriver::get_bucket(User* u, const RGWBucketInfo& i, std::unique_ptr<Bucket>* bucket)
{
  return this->get_bucket(this->dpp, u, i.bucket,
				bucket, null_yield);
}
*/

int D4NFilterDriver::get_bucket(const DoutPrefixProvider* dpp, User* u, const std::string& tenant, const std::string& name, std::unique_ptr<Bucket>* bucket, optional_yield y)
{
  return this->get_bucket(dpp, u, rgw_bucket(tenant,
					name, ""),
				bucket, y);
}



std::unique_ptr<Object> D4NFilterBucket::get_object(const rgw_obj_key& k)
{
  std::unique_ptr<Object> o = next->get_object(k);

  return std::make_unique<D4NFilterObject>(std::move(o), this, driver);
}

int D4NFilterUser::create_bucket(const DoutPrefixProvider* dpp,
                              const rgw_bucket& b,
                              const std::string& zonegroup_id,
                              rgw_placement_rule& placement_rule,
                              std::string& swift_ver_location,
                              const RGWQuotaInfo * pquota_info,
                              const RGWAccessControlPolicy& policy,
                              Attrs& attrs,
                              RGWBucketInfo& info,
                              obj_version& ep_objv,
                              bool exclusive,
                              bool obj_lock_enabled,
                              bool* existed,
                              req_info& req_info,
                              std::unique_ptr<Bucket>* bucket_out,
                              optional_yield y)
{
  std::unique_ptr<Bucket> nb;
  int ret;

  ret = next->create_bucket(dpp, b, zonegroup_id, placement_rule, swift_ver_location, pquota_info, policy, attrs, info, ep_objv, exclusive, obj_lock_enabled, existed, req_info, &nb, y);
  if (ret < 0)
    return ret;

  Bucket* fb = new D4NFilterBucket(std::move(nb), this, driver);
  bucket_out->reset(fb);
  return 0;
}

int D4NFilterObject::copy_object(User* user,
                              req_info* info,
                              const rgw_zone_id& source_zone,
                              rgw::sal::Object* dest_object,
                              rgw::sal::Bucket* dest_bucket,
                              rgw::sal::Bucket* src_bucket,
                              const rgw_placement_rule& dest_placement,
                              ceph::real_time* src_mtime,
                              ceph::real_time* mtime,
                              const ceph::real_time* mod_ptr,
                              const ceph::real_time* unmod_ptr,
                              bool high_precision_time,
                              const char* if_match,
                              const char* if_nomatch,
                              AttrsMod attrs_mod,
                              bool copy_if_newer,
                              Attrs& attrs,
                              RGWObjCategory category,
                              uint64_t olh_epoch,
                              boost::optional<ceph::real_time> delete_at,
                              std::string* version_id,
                              std::string* tag,
                              std::string* etag,
                              void (*progress_cb)(off_t, void *),
                              void* progress_data,
                              const DoutPrefixProvider* dpp,
                              optional_yield y)
{
  /* Build cache block copy */
  rgw::d4n::CacheBlock* copyCacheBlock = new rgw::d4n::CacheBlock(); // How will this copy work in lfuda? -Sam

  copyCacheBlock->hostsList.push_back(driver->get_cache_block()->hostsList[0]); 
  copyCacheBlock->size = driver->get_cache_block()->size;
  copyCacheBlock->dirty = driver->get_cache_block()->dirty;
  copyCacheBlock->lastAccessTime = driver->get_cache_block()->lastAccessTime;
  copyCacheBlock->globalWeight = driver->get_cache_block()->globalWeight; // Do we want to reset the global weight? -Sam
  copyCacheBlock->cacheObj.bucketName = dest_bucket->get_name();
  copyCacheBlock->cacheObj.objName = dest_object->get_key().get_oid();
  copyCacheBlock->cacheObj.dirty = driver->get_cache_block()->cacheObj.dirty;
  copyCacheBlock->cacheObj.lastAccessTime = driver->get_cache_block()->cacheObj.lastAccessTime;
  
  int copy_valueReturn = driver->get_block_dir()->set_value(copyCacheBlock);

  if (copy_valueReturn < 0) {
    ldpp_dout(dpp, 20) << "D4N Filter: Block directory copy operation failed." << dendl;
  } else {
    ldpp_dout(dpp, 20) << "D4N Filter: Block directory copy operation succeeded." << dendl;
  }

  delete copyCacheBlock;

  /* Append additional metadata to attributes */
  rgw::sal::Attrs baseAttrs = this->get_attrs();
  buffer::list bl;

  bl.append(to_iso_8601(*mtime));
  baseAttrs.insert({"mtime", bl});
  bl.clear();
  
  if (version_id != NULL) { 
    bl.append(*version_id);
    baseAttrs.insert({"version_id", bl});
    bl.clear();
  }
 
  if (!etag->empty()) {
    bl.append(*etag);
    baseAttrs.insert({"etag", bl});
    bl.clear();
  }

  if (attrs_mod == rgw::sal::ATTRSMOD_REPLACE) { /* Replace */
    rgw::sal::Attrs::iterator iter;

    for (const auto& pair : attrs) {
      iter = baseAttrs.find(pair.first);
    
      if (iter != baseAttrs.end()) {
        iter->second = pair.second;
      } else {
        baseAttrs.insert({pair.first, pair.second});
      }
    }
  } else if (attrs_mod == rgw::sal::ATTRSMOD_MERGE) { /* Merge */
    baseAttrs.insert(attrs.begin(), attrs.end()); 
  }

  /*
  int copy_attrsReturn = driver->get_cache_driver()->copy_attrs(this->get_key().get_oid(), dest_object->get_key().get_oid(), &baseAttrs);

  if (copy_attrsReturn < 0) {
    ldpp_dout(dpp, 20) << "D4N Filter: Cache copy attributes operation failed." << dendl;
  } else {
    int copy_dataReturn = driver->get_cache_driver()->copy_data(this->get_key().get_oid(), dest_object->get_key().get_oid());

    if (copy_dataReturn < 0) {
      ldpp_dout(dpp, 20) << "D4N Filter: Cache copy data operation failed." << dendl;
    } else {
      ldpp_dout(dpp, 20) << "D4N Filter: Cache copy object operation succeeded." << dendl;
    }
  }*/

  return next->copy_object(user, info, source_zone,
                           nextObject(dest_object),
                           nextBucket(dest_bucket),
                           nextBucket(src_bucket),
                           dest_placement, src_mtime, mtime,
                           mod_ptr, unmod_ptr, high_precision_time, if_match,
                           if_nomatch, attrs_mod, copy_if_newer, attrs,
                           category, olh_epoch, delete_at, version_id, tag,
                           etag, progress_cb, progress_data, dpp, y);
}

int D4NFilterObject::set_obj_attrs(const DoutPrefixProvider* dpp, Attrs* setattrs,
                            Attrs* delattrs, optional_yield y) 
{
  if (setattrs != NULL) {
    /* Ensure setattrs and delattrs do not overlap */
    if (delattrs != NULL) {
      for (const auto& attr : *delattrs) {
        if (std::find(setattrs->begin(), setattrs->end(), attr) != setattrs->end()) {
          delattrs->erase(std::find(delattrs->begin(), delattrs->end(), attr));
        }
      }
    }

    int update_attrsReturn = driver->get_cache_driver()->set_attrs(dpp, this->get_key().get_oid(), *setattrs, y);

    if (update_attrsReturn < 0) {
      ldpp_dout(dpp, 20) << "D4N Filter: Cache set object attributes operation failed." << dendl;
    } else {
      ldpp_dout(dpp, 20) << "D4N Filter: Cache set object attributes operation succeeded." << dendl;
    }
  }

  if (delattrs != NULL) {
    Attrs::iterator attr;
    Attrs currentattrs = this->get_attrs();

    /* Ensure all delAttrs exist */
    for (const auto& attr : *delattrs) {
      if (std::find(currentattrs.begin(), currentattrs.end(), attr) == currentattrs.end()) {
	delattrs->erase(std::find(delattrs->begin(), delattrs->end(), attr));
      }
    }

    int del_attrsReturn = driver->get_cache_driver()->delete_attrs(dpp, this->get_key().get_oid(), *delattrs, y);

    if (del_attrsReturn < 0) {
      ldpp_dout(dpp, 20) << "D4N Filter: Cache delete object attributes operation failed." << dendl;
    } else {
      ldpp_dout(dpp, 20) << "D4N Filter: Cache delete object attributes operation succeeded." << dendl;
    }
  }

  return next->set_obj_attrs(dpp, setattrs, delattrs, y);  
}

int D4NFilterObject::get_obj_attrs(optional_yield y, const DoutPrefixProvider* dpp,
                                rgw_obj* target_obj)
{
  rgw::sal::Attrs attrs;
  int get_attrsReturn = driver->get_cache_driver()->get_attrs(dpp, this->get_key().get_oid(), attrs, y);

  if (get_attrsReturn < 0) {
    ldpp_dout(dpp, 20) << "D4N Filter: Cache get object attributes operation failed." << dendl;

    return next->get_obj_attrs(y, dpp, target_obj);
  } else {
    int set_attrsReturn = this->set_attrs(attrs);
    
    if (set_attrsReturn < 0) {
      ldpp_dout(dpp, 20) << "D4N Filter: Cache get object attributes operation failed." << dendl;

      return next->get_obj_attrs(y, dpp, target_obj);
    } else {
      ldpp_dout(dpp, 20) << "D4N Filter: Cache get object attributes operation succeeded." << dendl;
  
      return 0;
    }
  }
}

int D4NFilterObject::modify_obj_attrs(const char* attr_name, bufferlist& attr_val,
                               optional_yield y, const DoutPrefixProvider* dpp) 
{
  Attrs update;
  update[(std::string)attr_name] = attr_val;
  int update_attrsReturn = driver->get_cache_driver()->update_attrs(dpp, this->get_key().get_oid(), update, y);

  if (update_attrsReturn < 0) {
    ldpp_dout(dpp, 20) << "D4N Filter: Cache modify object attribute operation failed." << dendl;
  } else {
    ldpp_dout(dpp, 20) << "D4N Filter: Cache modify object attribute operation succeeded." << dendl;
  }

  return next->modify_obj_attrs(attr_name, attr_val, y, dpp);  
}

int D4NFilterObject::delete_obj_attrs(const DoutPrefixProvider* dpp, const char* attr_name,
                               optional_yield y) 
{
  buffer::list bl;
  Attrs delattr;
  delattr.insert({attr_name, bl});
  Attrs currentattrs = this->get_attrs();
  rgw::sal::Attrs::iterator attr = delattr.begin();

  /* Ensure delAttr exists */
  if (std::find_if(currentattrs.begin(), currentattrs.end(),
        [&](const auto& pair) { return pair.first == attr->first; }) != currentattrs.end()) {
    int delAttrReturn = driver->get_cache_driver()->delete_attrs(dpp, this->get_key().get_oid(), delattr, y);

    if (delAttrReturn < 0) {
      ldpp_dout(dpp, 20) << "D4N Filter: Cache delete object attribute operation failed." << dendl;
    } else {
      ldpp_dout(dpp, 20) << "D4N Filter: Cache delete object attribute operation succeeded." << dendl;
    }
  } else 
    return next->delete_obj_attrs(dpp, attr_name, y);  

  return 0;
}

std::unique_ptr<Object> D4NFilterDriver::get_object(const rgw_obj_key& k)
{
  ldout(cct, 20) << "AMIN: " << __func__ << __LINE__ << ": key is: "  << k << dendl;
  std::unique_ptr<Object> o = next->get_object(k);
  ldout(cct, 20) << "AMIN: " << __func__ << __LINE__ << ": key is: "  << k << dendl;

  return std::make_unique<D4NFilterObject>(std::move(o), this);
}

std::unique_ptr<Writer> D4NFilterDriver::get_atomic_writer(const DoutPrefixProvider *dpp,
				  optional_yield y,
				  rgw::sal::Object* obj,
				  const rgw_user& owner,
				  const rgw_placement_rule *ptail_placement_rule,
				  uint64_t olh_epoch,
				  const std::string& unique_tag)
{
  std::unique_ptr<Writer> writer = next->get_atomic_writer(dpp, y, nextObject(obj),
							   owner, ptail_placement_rule,
							   olh_epoch, unique_tag);

  ldout(cct, 20) << "AMIN: " << __func__ << __LINE__ << ": key is: "  << obj->get_name() << dendl;
  return std::make_unique<D4NFilterWriter>(std::move(writer), this, obj, dpp, true);
}

std::unique_ptr<Object::ReadOp> D4NFilterObject::get_read_op()
{
  std::unique_ptr<ReadOp> r = next->get_read_op();
  return std::make_unique<D4NFilterReadOp>(std::move(r), this);
}

std::unique_ptr<Object::DeleteOp> D4NFilterObject::get_delete_op()
{
  std::unique_ptr<DeleteOp> d = next->get_delete_op();
  return std::make_unique<D4NFilterDeleteOp>(std::move(d), this);
}

int D4NFilterObject::D4NFilterReadOp::prepare(optional_yield y, const DoutPrefixProvider* dpp)
{
  rgw::sal::Attrs attrs;
  int getObjReturn = source->driver->get_cache_driver()->get_attrs(dpp, 
		                                                         source->get_key().get_oid(), 
					   				 attrs, y);

  next->params.mod_ptr = params.mod_ptr;
  next->params.unmod_ptr = params.unmod_ptr;
  next->params.high_precision_time = params.high_precision_time;
  next->params.mod_zone_id = params.mod_zone_id;
  next->params.mod_pg_ver = params.mod_pg_ver;
  next->params.if_match = params.if_match;
  next->params.if_nomatch = params.if_nomatch;
  next->params.lastmod = params.lastmod;
  int ret = next->prepare(y, dpp);
  
  if (getObjReturn < 0) {
    ldpp_dout(dpp, 20) << "D4N Filter: Cache get object operation failed." << dendl;
  } else {
    /* Set metadata locally */
    RGWQuotaInfo quota_info;
    RGWObjState* astate;
    source->get_obj_state(dpp, &astate, y);

    for (auto it = attrs.begin(); it != attrs.end(); ++it) {
      if (it->second.length() > 0) { // or return? -Sam
	if (it->first == "mtime") {
	  parse_time(it->second.c_str(), &astate->mtime);
	  attrs.erase(it->first);
	} else if (it->first == "object_size") {
	  source->set_obj_size(std::stoull(it->second.c_str()));
	  attrs.erase(it->first);
	} else if (it->first == "accounted_size") {
	  astate->accounted_size = std::stoull(it->second.c_str());
	  attrs.erase(it->first);
	} else if (it->first == "epoch") {
	  astate->epoch = std::stoull(it->second.c_str());
	  attrs.erase(it->first);
	} else if (it->first == "version_id") {
	  source->set_instance(it->second.c_str());
	attrs.erase(it->first);
      } else if (it->first == "source_zone_short_id") {
	astate->zone_short_id = static_cast<uint32_t>(std::stoul(it->second.c_str()));
	attrs.erase(it->first);
      } else if (it->first == "user_quota.max_size") {
        quota_info.max_size = std::stoull(it->second.c_str());
	attrs.erase(it->first);
      } else if (it->first == "user_quota.max_objects") {
        quota_info.max_objects = std::stoull(it->second.c_str());
	attrs.erase(it->first);
      } else if (it->first == "max_buckets") {
        source->get_bucket()->get_owner()->set_max_buckets(std::stoull(it->second.c_str()));
	attrs.erase(it->first);
      }
    }
    }

    source->get_bucket()->get_owner()->set_info(quota_info);
    source->set_obj_state(*astate);
   
    /* Set attributes locally */
    int set_attrsReturn = source->set_attrs(attrs);

    if (set_attrsReturn < 0) {
      ldpp_dout(dpp, 20) << "D4N Filter: Cache set object operation failed." << dendl;
    } else {
      ldpp_dout(dpp, 20) << "D4N Filter: Cache set object operation succeeded." << dendl;
    }   
  }

  //versioned objects have instance set to versionId, and get_oid() returns oid containing instance, hence using id tag as version for non versioned objects only
  if (! this->source->have_instance()) {
    RGWObjState* state = nullptr;
    if (this->source->get_obj_state(dpp, &state, y) == 0) {
      auto it = state->attrset.find(RGW_ATTR_ID_TAG);
      if (it != state->attrset.end()) {
        bufferlist bl = it->second;
        this->source->set_object_version(bl.c_str());
        ldpp_dout(dpp, 20) << __func__ << "id tag version is: " << this->source->get_object_version() << dendl;
      } else {
        ldpp_dout(dpp, 20) << __func__ << "Failed to find id tag" << dendl;
      }
    }
  }

  return ret;
}

void D4NFilterObject::D4NFilterReadOp::cancel() {
  aio->drain();
}

int D4NFilterObject::D4NFilterReadOp::drain(const DoutPrefixProvider* dpp) {
  auto c = aio->wait();
  while (!c.empty()) {
    int r = flush(dpp, std::move(c));
    if (r < 0) {
      cancel();
      return r;
    }
    c = aio->wait();
  }
  return flush(dpp, std::move(c));
}

int D4NFilterObject::D4NFilterReadOp::flush(const DoutPrefixProvider* dpp, rgw::AioResultList&& results) {
  int r = rgw::check_for_errors(results);

  if (r < 0) {
    return r;
  }

  std::list<bufferlist> bl_list;

  auto cmp = [](const auto& lhs, const auto& rhs) { return lhs.id < rhs.id; };
  results.sort(cmp); // merge() requires results to be sorted first
  completed.merge(results, cmp); // merge results in sorted order

  ldpp_dout(dpp, 20) << "D4NFilterObject::In flush:: " << dendl;

  while (!completed.empty() && completed.front().id == offset) {
    auto bl = std::move(completed.front().data);

    ldpp_dout(dpp, 20) << "D4NFilterObject::flush:: calling handle_data for offset: " << offset << " bufferlist length: " << bl.length() << dendl;

    bl_list.push_back(bl);
    offset += bl.length();
    int r = client_cb->handle_data(bl, 0, bl.length());
    if (r < 0) {
      return r;
    }
    completed.pop_front_and_dispose(std::default_delete<rgw::AioResultEntry>{});
  }

  ldpp_dout(dpp, 20) << "D4NFilterObject::returning from flush:: " << dendl;
  return 0;
}

int D4NFilterObject::D4NFilterReadOp::iterate(const DoutPrefixProvider* dpp, int64_t ofs, int64_t end,
                        RGWGetDataCB* cb, optional_yield y) 
{
  const uint64_t window_size = g_conf()->rgw_get_obj_window_size;
  std::string prefix;
  std::string version = "";
  /* //FIXME: uncomment this and apply it to write operation
  std::string version = source->get_object_version(); 
  if (version.empty()) { //for versioned objects, get_oid() returns an oid with versionId added
    prefix = source->get_bucket()->get_name() + "_" + source->get_key().get_oid();
  } else {
    prefix = source->get_bucket()->get_name() + "_" + version + "_" + source->get_key().get_oid();
  }
  */
  prefix = source->get_bucket()->get_name() + "_" + source->get_key().get_oid();


  ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << "prefix: " << prefix << " ofs: " << ofs << " end: " << end << dendl;

  this->client_cb = cb;
  this->cb->set_client_cb(cb, dpp, &y);
  this->cb->set_prefix(prefix);

  /* This algorithm stores chunks for ranged requests also in the cache, which might be smaller than obj_max_req_size
     One simplification could be to overwrite the smaller chunks with a bigger chunk of obj_max_req_size, and to serve requests for smaller
     chunks using the larger chunk, but all corner cases need to be considered like the last chunk which might be smaller than obj_max_req_size
     and also ranged requests where a smaller chunk is overwritten by a larger chunk size != obj_max_req_size */

  uint64_t obj_max_req_size = g_conf()->rgw_get_obj_max_req_size;
  uint64_t start_part_num = 0;
  uint64_t part_num = ofs/obj_max_req_size; //part num of ofs wrt start of the object
  uint64_t adjusted_start_ofs = part_num*obj_max_req_size; //in case of ranged request, adjust the start offset to the beginning of a chunk/ part
  uint64_t diff_ofs = ofs - adjusted_start_ofs; //difference between actual offset and adjusted offset
  off_t len = (end - adjusted_start_ofs) + 1;
  uint64_t num_parts = (len%obj_max_req_size) == 0 ? len/obj_max_req_size : (len/obj_max_req_size) + 1; //calculate num parts based on adjusted offset
  //len_to_read is the actual length read from a part/ chunk in cache, while part_len is the length of the chunk/ part in cache 
  uint64_t cost = 0, len_to_read = 0, part_len = 0;

  aio = rgw::make_throttle(window_size, y);

  ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << "obj_max_req_size " << obj_max_req_size << 
  " num_parts " << num_parts << " adjusted_start_offset: " << adjusted_start_ofs << " len: " << len << dendl;

  this->offset = ofs;

  do {
    uint64_t id = adjusted_start_ofs, read_ofs = 0; //read_ofs is the actual offset to start reading from the current part/ chunk
    if (start_part_num == (num_parts - 1)) {
      len_to_read = len;
      part_len = len;
      cost = len;
    } else {
      len_to_read = obj_max_req_size;
      cost = obj_max_req_size;
      part_len = obj_max_req_size;
    }
    if (start_part_num == 0) {
      len_to_read -= diff_ofs;
      id += diff_ofs;
      read_ofs = diff_ofs;
    }

    ceph::bufferlist bl;
//    std::string oid_in_cache = source->get_bucket()->get_name() + "_" + oid + "_" + std::to_string(adjusted_start_ofs) + "_" + std::to_string(part_len);

    std::string oid_in_cache = prefix + "_" + std::to_string(adjusted_start_ofs) + "_" + std::to_string(part_len);
    /* FIXME : uncomment this
    if (version.empty()) {
      version = source->get_instance();
    }
    */
    std::string key = oid_in_cache;
    int dirty = 0;
    auto lastAccessTime = time(NULL);
    ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): READ FROM CACHE: oid=" << oid_in_cache << " length to read is: " << len_to_read << " part num: " << start_part_num << 
    " read_ofs: " << read_ofs << " part len: " << part_len << dendl;

    if (source->driver->get_policy_driver()->get_cache_policy()->exist_key(oid_in_cache)) { 
        ldpp_dout(dpp, 20) << "D4NFilterObject: " << __func__ << __LINE__ << "key is: " << key << dendl;
      // Read From Cache
      //
      // check if the data is dirty and if yes, add "D" to the beggining of the oid
      if (source->driver->get_block_dir()->get_field(oid_in_cache, "dirty") == "1"){ 
        key = "D_" + oid_in_cache; //we keep track of dirty data in the cache for the metadata failure case
	dirty =  1;
      }

      ldpp_dout(dpp, 20) << "D4NFilterObject: " << __func__ << __LINE__ << "key is: " << key << dendl;
      auto completed = source->driver->get_cache_driver()->get_async(dpp, y, aio.get(), key, read_ofs, len_to_read, cost, id); 

        ldpp_dout(dpp, 20) << "D4NFilterObject: " << __func__ << __LINE__ << " bucket OWNER name is: " << source->get_bucket()->get_owner()->get_id() << dendl;
      std::string localWeight = source->driver->get_policy_driver()->get_cache_policy()->update(dpp, oid_in_cache, adjusted_start_ofs, part_len, version, dirty, lastAccessTime, source->get_bucket()->get_owner()->get_id(), y);

      //if (source->driver->get_cache_driver()->set_attr(dpp, key, "user.rgw.localWeight", localWeight, y) < 0) 
      //  ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): CacheDriver set_attr method failed." << dendl;

      ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Info: flushing data for oid: " << key << dendl;

      auto r = flush(dpp, std::move(completed));

      if (r < 0) {
        drain(dpp);
        ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Error: failed to flush, r= " << r << dendl;
        return r;
      }
    } else {
        ldpp_dout(dpp, 20) << "D4NFilterObject: " << __func__ << __LINE__ << dendl;
      //oid_in_cache = source->get_bucket()->get_name() + "_" + oid + "_" + std::to_string(adjusted_start_ofs) + "_" + std::to_string(obj_max_req_size);
      oid_in_cache = prefix + "_" + std::to_string(adjusted_start_ofs) + "_" + std::to_string(obj_max_req_size);
      //for ranged requests, for last part, the whole part might exist in the cache
       ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): READ FROM CACHE: oid=" << oid_in_cache << " length to read is: " << len_to_read << " part num: " << start_part_num << 
      " read_ofs: " << read_ofs << " part len: " << part_len << dendl;

      if ((part_len != obj_max_req_size) && source->driver->get_policy_driver()->get_cache_policy()->exist_key(oid_in_cache)) {
        // Read From Cache
        ldpp_dout(dpp, 20) << "D4NFilterObject: " << __func__ << __LINE__ << "key is: " << key << dendl;
        if (source->driver->get_block_dir()->get_field(oid_in_cache, "dirty") == "1"){
          key = "D_" + oid_in_cache;
	  dirty = 1;
	}

        ldpp_dout(dpp, 20) << "D4NFilterObject: " << __func__ << __LINE__ << "key is: " << key << dendl;
        auto completed = source->driver->get_cache_driver()->get_async(dpp, y, aio.get(), key, read_ofs, len_to_read, cost, id);  
        ldpp_dout(dpp, 20) << "D4NFilterObject: " << __func__ << __LINE__ << dendl;

        ldpp_dout(dpp, 20) << "D4NFilterObject: " << __func__ << __LINE__ << " bucket name is: " << source->get_bucket()->get_name() << dendl;
        std::string localWeight = source->driver->get_policy_driver()->get_cache_policy()->update(dpp, oid_in_cache, adjusted_start_ofs, obj_max_req_size, version, dirty, lastAccessTime, source->get_bucket()->get_owner()->get_id(), y);
        //if (source->driver->get_cache_driver()->set_attr(dpp, key, "user.rgw.localWeight", localWeight, y) < 0) 
        //  ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): CacheDriver set_attr method failed." << dendl;

        ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Info: flushing data for oid: " << key << dendl;

        auto r = flush(dpp, std::move(completed));

        if (r < 0) {
          drain(dpp);
          ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Error: failed to flush, r= " << r << dendl;
          return r;
        }

      } else {
        ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Info: draining data for oid: " << oid_in_cache << dendl;

        auto r = drain(dpp);

        if (r < 0) {
          ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Error: failed to drain, r= " << r << dendl;
          return r;
        }

        break;
      }
    }

    if (start_part_num == (num_parts - 1)) {
      ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Info: draining data for oid: " << oid_in_cache << dendl;
      return drain(dpp);
    } else {
      adjusted_start_ofs += obj_max_req_size;
    }

    start_part_num += 1;
    len -= obj_max_req_size;
  } while (start_part_num < num_parts);

  ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Fetching object from backend store" << dendl;

  Attrs obj_attrs;
  if (source->has_attrs()) {
    obj_attrs = source->get_attrs();
  }

  if (source->is_compressed() || obj_attrs.find(RGW_ATTR_CRYPT_MODE) != obj_attrs.end() || !y) {
    ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Skipping writing to cache" << dendl;
    this->cb->bypass_cache_write();
  }

  if (start_part_num == 0) {
    this->cb->set_ofs(ofs);
  } else {
    this->cb->set_ofs(adjusted_start_ofs);
    ofs = adjusted_start_ofs; // redundant? -Sam
  }

  this->cb->set_ofs(ofs);
  auto r = next->iterate(dpp, ofs, end, this->cb.get(), y);
  
  if (r < 0) {
    ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Error: failed to fetch object from backend store, r= " << r << dendl;
    return r;
  }
  
  return this->cb->flush_last_part();

}

int D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::flush_last_part()
{
  last_part = true;
  return handle_data(bl_rem, 0, bl_rem.length());
}

int D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::handle_data(bufferlist& bl, off_t bl_ofs, off_t bl_len)
{
  auto rgw_get_obj_max_req_size = g_conf()->rgw_get_obj_max_req_size;

  if (!last_part && bl.length() <= rgw_get_obj_max_req_size) {
    auto r = client_cb->handle_data(bl, bl_ofs, bl_len);

    if (r < 0) {
      return r;
    }
  }

  //Accumulating data from backend store into rgw_get_obj_max_req_size sized chunks and then writing to cache
  if (write_to_cache) {
    const std::lock_guard l(d4n_get_data_lock);
    rgw::d4n::CacheBlock block, existing_block;
    rgw::d4n::BlockDirectory* blockDir = source->driver->get_block_dir();
    block.hostsList.push_back(blockDir->get_addr().host + ":" + std::to_string(blockDir->get_addr().port));
    block.cacheObj.objName = source->get_key().get_oid();
    block.cacheObj.bucketName = source->get_bucket()->get_name();
    //block.cacheObj.creationTime = std::chrono::system_clock::to_time_t(source->get_mtime()); //FIXME: uncomment it and make it work
    block.cacheObj.dirty = 0;
    int dirty = 0;

    //populating fields needed for building directory index
    existing_block.cacheObj.objName = block.cacheObj.objName;
    existing_block.cacheObj.bucketName = block.cacheObj.bucketName;
    Attrs attrs; // empty attrs for cache sets
    /* FIXME: uncomment this
    std::string version = source->get_object_version();
    if (version.empty()) {
      version = source->get_instance();
    }
    */
    std::string version = "";
    ldpp_dout(dpp, 20) << __func__ << ": version stored in update method is: " << version << dendl;

    if (bl.length() > 0 && last_part) { // if bl = bl_rem has data and this is the last part, write it to cache
      std::string oid = prefix + "_" + std::to_string(ofs) + "_" + std::to_string(bl_len);

      block.size = bl.length();
      block.blockId = ofs;
      block.version = version;
      block.dirty = dirty;
      auto lastAccessTime = time(NULL);

      uint64_t freeSpace = filter->get_cache_driver()->get_free_space(dpp);
      while(freeSpace < block.size) {
        freeSpace += filter->get_policy_driver()->get_cache_policy()->eviction(dpp, block.size, *y);
      }
      if (filter->get_cache_driver()->put_async(dpp, oid, bl, bl.length(), attrs) == 0) {
	std::string localWeight = filter->get_policy_driver()->get_cache_policy()->update(dpp, oid, ofs, bl.length(), version, dirty, lastAccessTime,  source->get_bucket()->get_owner()->get_id(), *y);
	filter->get_policy_driver()->get_cache_policy()->updateObj(dpp, prefix, version, dirty, source->get_obj_size(), lastAccessTime,  source->get_bucket()->get_owner()->get_id(), *y);
        if (source->driver->get_cache_driver()->set_attr(dpp, oid, "user.rgw.localWeight", localWeight, *y) < 0) 
          ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): CacheDriver set_attr method failed." << dendl;
        /* Store block in directory */
        if (!blockDir->exist_key(oid)) {
          int ret = blockDir->set_value(&block);
          if (ret < 0) {
            ldpp_dout(dpp, 0) << "D4N Filter: Block directory set operation failed." << dendl;
            return ret;
          } else {
            ldpp_dout(dpp, 20) << "D4N Filter: Block directory set operation succeeded." << dendl;
          }
        }
	else {
          existing_block.blockId = block.blockId;
          existing_block.size = block.size;
	  existing_block.dirty = block.dirty;
          if (blockDir->get_value(&existing_block) < 0) {
            ldpp_dout(dpp, 0) << "Failed to fetch existing block for: " << existing_block.cacheObj.objName << " blockID: " << existing_block.blockId << " block size: " << existing_block.size << dendl;
          } else {
            if (existing_block.version != block.version) {
              if (blockDir->del_value(&existing_block) < 0) //delete existing block
                ldpp_dout(dpp, 10) << "D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::" << __func__ << "(): BlockDirectory del method failed." << dendl;
              if (blockDir->set_value(&block) < 0) //new versioned block will have new version, hostsList etc, how about globalWeight?
                ldpp_dout(dpp, 10) << "D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::" << __func__ << "(): BlockDirectory set method failed." << dendl;
            } else {
              if (blockDir->update_field(&block, "blockHosts", blockDir->get_addr().host + ":" + std::to_string(blockDir->get_addr().port)) < 0)
                ldpp_dout(dpp, 10) << "D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::" << __func__ << "(): BlockDirectory update_field method failed for hostsList." << dendl;
            }
          }
	}
      }
    } else if (bl.length() == rgw_get_obj_max_req_size && bl_rem.length() == 0) { // if bl is the same size as rgw_get_obj_max_req_size, write it to cache
      std::string oid = prefix + "_" + std::to_string(ofs) + "_" + std::to_string(bl_len);
      //std::string oid_in_cache = "D_" + oid + "_" + std::to_string(bl_len);
      ofs += bl_len;
      block.blockId = ofs;
      block.size = bl.length();
      block.version = version;
      block.dirty = dirty;
      auto lastAccessTime = time(NULL);
      uint64_t freeSpace = filter->get_cache_driver()->get_free_space(dpp);
      while(freeSpace < block.size) {
        freeSpace += filter->get_policy_driver()->get_cache_policy()->eviction(dpp, block.size, *y);
      }
      if (filter->get_cache_driver()->put_async(dpp, oid, bl, bl.length(), attrs) == 0) {
	std::string localWeight = filter->get_policy_driver()->get_cache_policy()->update(dpp, oid, ofs, bl.length(), version, dirty, lastAccessTime,  source->get_bucket()->get_owner()->get_id(), *y);
        if (source->driver->get_cache_driver()->set_attr(dpp, oid, "user.rgw.localWeight", localWeight, *y) < 0) 
          ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): CacheDriver set_attr method failed." << dendl;
        /* Store block in directory */
        if (!blockDir->exist_key(oid)) {
          int ret = blockDir->set_value(&block);
          if (ret < 0) {
            ldpp_dout(dpp, 0) << "D4N Filter: Block directory set operation failed." << dendl;
            return ret;
          } else {
            ldpp_dout(dpp, 20) << "D4N Filter: Block directory set operation succeeded." << dendl;
          }
        } else {
              existing_block.blockId = block.blockId;
              existing_block.size = block.size;
	      existing_block.dirty = block.dirty;
              if (blockDir->get_value(&existing_block) < 0) {
                ldpp_dout(dpp, 0) << "Failed to fetch existing block for: " << existing_block.cacheObj.objName << " blockID: " << existing_block.blockId << " block size: " << existing_block.size << dendl;
              }
              if (existing_block.version != block.version) {
                if (blockDir->del_value(&existing_block) < 0)
                    ldpp_dout(dpp, 10) << "D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::" << __func__ << "(): BlockDirectory del method failed." << dendl;
                  if (blockDir->set_value(&block) < 0)
                    ldpp_dout(dpp, 10) << "D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::" << __func__ << "(): BlockDirectory set method failed." << dendl;
              } else {
                if (blockDir->update_field(&block, "blockHosts", blockDir->get_addr().host + ":" + std::to_string(blockDir->get_addr().port)) < 0)
                  ldpp_dout(dpp, 10) << "D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::" << __func__ << "(): BlockDirectory update_field method failed for blockHosts." << dendl;
              }
            }

      }
    } else { //copy data from incoming bl to bl_rem till it is rgw_get_obj_max_req_size, and then write it to cache
      uint64_t rem_space = rgw_get_obj_max_req_size - bl_rem.length();
      uint64_t len_to_copy = rem_space > bl.length() ? bl.length() : rem_space;
      bufferlist bl_copy;

      bl.splice(0, len_to_copy, &bl_copy);
      bl_rem.claim_append(bl_copy);

      if (bl_rem.length() == rgw_get_obj_max_req_size) {
        std::string oid = prefix + "_" + std::to_string(ofs) + "_" + std::to_string(bl_rem.length());
        //std::string oid_in_cache = "D_" + oid + "_" + std::to_string(bl_rem.length());
        ofs += bl_rem.length();
        block.blockId = ofs;
        block.size = bl_rem.length();
        block.version = version;
        block.dirty = dirty;
        auto lastAccessTime = time(NULL);
        uint64_t freeSpace = filter->get_cache_driver()->get_free_space(dpp);
        while(freeSpace < block.size) {
          freeSpace += filter->get_policy_driver()->get_cache_policy()->eviction(dpp, block.size, *y);
        }
        if (filter->get_cache_driver()->put_async(dpp, oid, bl_rem, bl_rem.length(), attrs) == 0) {
          std::string localWeight = filter->get_policy_driver()->get_cache_policy()->update(dpp, oid, ofs, bl_rem.length(),version, dirty, lastAccessTime, source->get_bucket()->get_owner()->get_id(), *y);
          if (source->driver->get_cache_driver()->set_attr(dpp, oid, "user.rgw.localWeight", localWeight, *y) < 0) 
            ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): CacheDriver set_attr method failed." << dendl;
          /* Store block in directory */
          if (!blockDir->exist_key(oid)) {
            int ret = blockDir->set_value(&block);
            if (ret < 0) {
              ldpp_dout(dpp, 0) << "D4N Filter: Block directory set operation failed." << dendl;
              return ret;
            } else {
              ldpp_dout(dpp, 20) << "D4N Filter: Block directory set operation succeeded." << dendl;
            }
          } else {
                  existing_block.blockId = block.blockId;
                  existing_block.size = block.size;
		  existing_block.dirty = block.dirty;
                  if (blockDir->get_value(&existing_block) < 0) {
                    ldpp_dout(dpp, 0) << "Failed to fetch existing block for: " << existing_block.cacheObj.objName << " blockID: " << existing_block.blockId << " block size: " << existing_block.size << dendl;
                  } else {
                    if (existing_block.version != block.version) {
                      if (blockDir->del_value(&existing_block) < 0)
                        ldpp_dout(dpp, 10) << "D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::" << __func__ << "(): BlockDirectory del method failed." << dendl;
                      if (blockDir->set_value(&block) < 0)
                        ldpp_dout(dpp, 10) << "D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::" << __func__ << "(): BlockDirectory set method failed." << dendl;
                    } else {
                    if (blockDir->update_field(&block, "blockHosts", blockDir->get_addr().host + ":" + std::to_string(blockDir->get_addr().port)) < 0)
                      ldpp_dout(dpp, 10) << "D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::" << __func__ << "(): BlockDirectory update_field method failed." << dendl;
                    }
                  }
                }

        }

        bl_rem.clear();
        bl_rem = std::move(bl);
      }
    }
  }

  return 0;
}

int D4NFilterObject::D4NFilterDeleteOp::delete_obj(const DoutPrefixProvider* dpp,
					   optional_yield y)
{
  //FIXME: shouldn't we take care of policy lists? : AMIN

  int delDirReturn = source->driver->get_block_dir()->del_value(source->driver->get_cache_block());

  if (delDirReturn < 0) {
    ldpp_dout(dpp, 20) << "D4N Filter: Block directory delete operation failed." << dendl;
  } else {
    ldpp_dout(dpp, 20) << "D4N Filter: Block directory delete operation succeeded." << dendl;
  }

  Attrs::iterator attrs;
  Attrs currentattrs = source->get_attrs();
  std::vector<std::string> currentFields;
  
  /* Extract fields from current attrs */
  for (attrs = currentattrs.begin(); attrs != currentattrs.end(); ++attrs) {
    currentFields.push_back(attrs->first);
  }

  int delObjReturn = source->driver->get_cache_driver()->delete_data(dpp, source->get_key().get_oid(), y);

  if (delObjReturn < 0) {
    ldpp_dout(dpp, 20) << "D4N Filter: Cache delete object operation failed." << dendl;
  } else {
    Attrs delattrs = source->get_attrs();
    delObjReturn = source->driver->get_cache_driver()->delete_attrs(dpp, source->get_key().get_oid(), delattrs, y);
    ldpp_dout(dpp, 20) << "D4N Filter: Cache delete operation succeeded." << dendl;
  }

  return next->delete_obj(dpp, y);
}

/* FIXME: for versioning, we need to have uniqu object names in the write cache
void D4NFilterWriter::gen_rand_obj_instance_name(Object *obj)
{
#define OBJ_INSTANCE_LEN 32
  char buf[OBJ_INSTANCE_LEN + 1];

  gen_rand_alphanumeric_no_underscore(filter->ctx(), buf, OBJ_INSTANCE_LEN);
  obj->get_key()->set_instance(buf);
}
*/

int D4NFilterWriter::prepare(optional_yield y) 
{
  d4n_writecache = g_conf()->d4n_writecache_enabled;

  s3_backend = g_conf()->s3_backend_enabled;
  if (s3_backend == true)
    return S3BackendPrepare(y);

  if (filter->get_write_to_backend() == true) 
  	return next->prepare(y);
  else
	return 0;
}



void D4NFilterWriter::getAccessSecretKeys(RGWAccessKey* accesskey, User* user)
{
  map<std::string, RGWAccessKey> accessKeys =  user->get_info().access_keys;

  accesskey->id = accessKeys.begin()->second.id;
  accesskey->key = accessKeys.begin()->second.key;
}



int D4NFilterWriter::S3BackendPrepare(optional_yield y) 
{
  this->user = (rgw::sal::D4NFilterUser*) obj->get_bucket()->get_owner();

  std::string url ="https://" + g_conf()->s3_backend_url;
  HostStyle host_style = PathStyle;
	 
  this->obj_wr = new RGWRESTStreamS3PutObj(filter->cct, "PUT", url, NULL, NULL, "", host_style);

  RGWAccessKey accesskey;
  getAccessSecretKeys(&accesskey, obj->get_bucket()->get_owner());

  map<string, bufferlist> obj_attrs; 
  this->obj_wr->put_obj_init(save_dpp, accesskey, this->obj->get_obj(), obj_attrs);
  return 0;
}

int D4NFilterWriter::process(bufferlist&& data, uint64_t offset)
{ 
    bufferlist bl = data;
    off_t bl_len = bl.length();
    off_t ofs = offset;
    bool dirty = 1;
    auto lastAccessTime = time(NULL);
    optional_yield y = null_yield;
    std::string version = "";
    rgw::d4n::BlockDirectory* blockDir = filter->get_block_dir();
    rgw::d4n::CacheBlock block;

    block.hostsList.push_back(blockDir->get_addr().host + ":" + std::to_string(blockDir->get_addr().port));


    block.cacheObj.bucketName = obj->get_bucket()->get_name();
    block.cacheObj.objName = obj->get_key().get_oid();
    block.cacheObj.dirty = 1;

    if (bl.length() > 0) { // if bl = bl_rem has data and this is the last part, write it to cache
      /* FIXME:  to add unique ID we have to generate a random number.
       * but we need to make sure it doesn't conflict with backend uniqueID.
       *
      std::string uniqueID = gen_rand_obj_instance_name(obj);
      std::string oid = uniqueID + obj->get_key().get_oid() + "_" + std::to_string(ofs) + "_" + std::to_string(bl_len);
      */
      std::string oid = obj->get_bucket()->get_name() + "_" + obj->get_key().get_oid() + "_" + std::to_string(ofs);
      std::string key = "D_" + oid + "_" + std::to_string(bl_len);
      std::string oid_in_cache = oid + "_" + std::to_string(bl_len);
      std::string oid_in_dir = oid + "_" + std::to_string(bl_len);
      block.size = bl.length();
      block.blockId = ofs;
      block.dirty = 1;

     uint64_t freeSpace = filter->get_cache_driver()->get_free_space(save_dpp);
      while(freeSpace < block.size) {
        freeSpace += filter->get_policy_driver()->get_cache_policy()->eviction(save_dpp, block.size, y);
      }


    if (filter->get_write_to_backend() == false){
     if (d4n_writecache == true){
      /* Do we need to delete olb object in the cache? : AMIN
      int del_dataReturn = filter->get_cache_driver()->delete_data(save_dpp, key, y);
  
      if (del_dataReturn < 0) {
	ldpp_dout(save_dpp, 20) << "D4N Filter: Cache delete data operation failed." << dendl;
      } else {
        ldpp_dout(save_dpp, 20) << "D4N Filter: Cache delete data operation succeeded." << dendl;
      }
      */

         if (filter->get_cache_driver()->put_async(save_dpp, key, bl, bl.length(), obj->get_attrs()) == 0) {
	// FIXME: uncomment this
        ldpp_dout(save_dpp, 20) << "D4NFilterWriter: " << __func__ << __LINE__ << " bucket name is: " << obj->get_bucket()->get_name() << dendl;
        std::string localWeight = filter->get_policy_driver()->get_cache_policy()->update(save_dpp, oid_in_cache, ofs, bl.length(), version, dirty, lastAccessTime,  obj->get_bucket()->get_owner()->get_id(), y);

        if (filter->get_cache_driver()->set_attr(save_dpp, key, "user.rgw.localWeight", localWeight, y) < 0) 
          ldpp_dout(save_dpp, 10) << "LFUDAPolicy::" << __func__ << "(): CacheDriver set_attr method failed." << dendl;
        // Store block in directory
        if (!blockDir->exist_key(oid_in_dir)) {
          int ret = blockDir->set_value(&block);
          if (ret < 0) {
            ldpp_dout(save_dpp, 0) << "D4N Filter: " << __func__ << ": Block directory set operation failed." << dendl;
            return ret;
          } else {
            ldpp_dout(save_dpp, 20) << "D4N Filter: " << __func__ << ": Block directory set operation succeeded." << dendl;
          }
        }
     }
    } 
    return 0;
    //if (s3_backend == true){
    //  return S3BackendProcess(std::move(data), offset);
    //}
   } //end d4n_writecache
  
  else{ //data cleaning
        ldpp_dout(save_dpp, 20) << "D4NFilter: " << __func__ << __LINE__ << ": data is: " << data.to_str()<< dendl;
        ldpp_dout(save_dpp, 20) << "D4NFilter: " << __func__ << __LINE__ << ": offset is: " << offset << dendl;
     int ret = next->process(std::move(data), offset);
     if (ret < 0)
     {
       ldpp_dout(save_dpp, 0) << "D4N Filter: " << __func__ << ": Writing to backend FAILED!" << dendl;
       return ret;
     }
/* 
      if (filter->get_cache_driver()->put_async(save_dpp, oid_in_cache, bl, bl.length(), obj->get_attrs()) == 0) { //this should be oid, without D_. we keep a clean copy in the cache
	dirty = 0;
        std::string localWeight = filter->get_policy_driver()->get_cache_policy()->update(save_dpp, oid_in_cache, ofs, bl.length(), version, dirty, lastAccessTime,  obj->get_bucket()->get_owner()->get_id(), y);

        if (filter->get_cache_driver()->set_attr(save_dpp, oid_in_cache, "user.rgw.localWeight", localWeight, y) < 0){ 
          ldpp_dout(save_dpp, 10) << "D4N Filter:" << __func__ << "(): CacheDriver set_attr method failed." << dendl;
	}
        
 
        ldpp_dout(save_dpp, 20) << "D4NFilter: " << __func__ << __LINE__ << dendl;
        ret = filter->get_cache_driver()->delete_data(save_dpp, key, y); //delete dirty data with D_
        if (ret < 0) { 
          ldpp_dout(save_dpp, 0) << "D4N Filter: " << __func__ << ": Deleting dirty data from cache FAILED!" << dendl;
          return ret;
        }

        ldpp_dout(save_dpp, 20) << "D4NFilter: " << __func__ << __LINE__ << dendl;
	// update block in directory
        block.dirty = 0;
    	int ret = blockDir->update_field(&block, "dirty", std::to_string(0));
        if (ret < 0) {
          ldpp_dout(save_dpp, 0) << "D4N Filter: " << __func__ << ": Block directory set operation failed." << dendl;
          return ret;
        } else {
          ldpp_dout(save_dpp, 20) << "D4N Filter: " << __func__ << ": Block directory set operation succeeded." << dendl;
        }
        ldpp_dout(save_dpp, 20) << "D4NFilter: " << __func__ << __LINE__ << dendl;
     }
*/    
     return 0;
   }
  }
  return 0;
}

int D4NFilterWriter::S3BackendProcess(bufferlist&& data, uint64_t offset)
{
  int ret = 0; 
  bufferlist objectData = data;
  

  if (objectData.length() != 0){
    send_data.claim_append(objectData);
    return 0;
  }
  else{
    this->obj_wr->set_send_length(send_data.length());

    ret = RGWHTTP::send(obj_wr);
    if (ret < 0) {
      delete obj_wr;
      return ret;
    }
  
    return this->obj_wr->get_out_cb()->handle_data(send_data, 0, send_data.length());
  }

}

int D4NFilterWriter::complete(size_t accounted_size, const std::string& etag,
                       ceph::real_time *mtime, ceph::real_time set_mtime,
                       std::map<std::string, bufferlist>& attrs,
                       ceph::real_time delete_at,
                       const char *if_match, const char *if_nomatch,
                       const std::string *user_data,
                       rgw_zone_set *zones_trace, bool *canceled,
                       const req_context& rctx)
{
  bool dirty = 1;
  auto lastAccessTime = time(NULL);
  optional_yield y = null_yield;
  std::string version = "";

  ldpp_dout(save_dpp, 20) << "D4N Filter: " << __func__ << " : " << __LINE__ << dendl;
  std::string prefix = obj->get_bucket()->get_name() + "_"+ obj->get_key().get_oid();

  if (filter->get_write_to_backend() == false){
  	filter->get_policy_driver()->get_cache_policy()->updateObj(save_dpp, prefix, version, dirty, accounted_size, lastAccessTime,  obj->get_bucket()->get_owner()->get_id(), y);
	return 0;
  }

  if (s3_backend == true)
    return S3BackendComplete(accounted_size);
   
  // Retrieve complete set of attrs
  int ret = 0;
  
  if (filter->get_write_to_backend() == true){
  	ldpp_dout(save_dpp, 20) << "D4N Filter: " << __func__ << " : " << __LINE__ << " object name is: " << prefix << dendl;
	dirty = 0;
  	filter->get_policy_driver()->get_cache_policy()->updateObj(save_dpp, prefix, version, dirty, accounted_size, lastAccessTime,  obj->get_bucket()->get_owner()->get_id(), y);
  	ret = next->complete(accounted_size, etag, mtime, set_mtime, attrs,
			delete_at, if_match, if_nomatch, user_data, zones_trace,
			canceled, rctx);
	/*
	obj->get_obj_attrs(rctx.y, save_dpp, NULL);

  	// Append additional metadata to attributes 
  	rgw::sal::Attrs baseAttrs = obj->get_attrs();
  	rgw::sal::Attrs attrs_temp = baseAttrs;
  	buffer::list bl;
  	RGWObjState* astate;
  	obj->get_obj_state(save_dpp, &astate, rctx.y);

  	bl.append(to_iso_8601(obj->get_mtime()));
  	baseAttrs.insert({"mtime", bl});
  	bl.clear();

  	bl.append(std::to_string(obj->get_obj_size()));
  	baseAttrs.insert({"object_size", bl});
  	bl.clear();

  	bl.append(std::to_string(accounted_size));
  	baseAttrs.insert({"accounted_size", bl});
  	bl.clear();
 
  	bl.append(std::to_string(astate->epoch));
  	baseAttrs.insert({"epoch", bl});
  	bl.clear();

  	if (obj->have_instance()) {
    	  bl.append(obj->get_instance());
     	  baseAttrs.insert({"version_id", bl});
    	  bl.clear();
  	} else {
    	  bl.append(""); // Empty value 
    	  baseAttrs.insert({"version_id", bl});
    	  bl.clear();
 	}

  	auto iter = attrs_temp.find(RGW_ATTR_SOURCE_ZONE);
  	if (iter != attrs_temp.end()) {
    	  bl.append(std::to_string(astate->zone_short_id));
    	  baseAttrs.insert({"source_zone_short_id", bl});
    	  bl.clear();
  	} else {
    	  bl.append("0"); // Initialized to zero
    	  baseAttrs.insert({"source_zone_short_id", bl});
    	  bl.clear();
  	}

  	RGWUserInfo info = obj->get_bucket()->get_owner()->get_info();
  	bl.append(std::to_string(info.quota.user_quota.max_size));
  	baseAttrs.insert({"user_quota.max_size", bl});
  	bl.clear();

  	bl.append(std::to_string(info.quota.user_quota.max_objects));
  	baseAttrs.insert({"user_quota.max_objects", bl});
  	bl.clear();

  	bl.append(std::to_string(obj->get_bucket()->get_owner()->get_max_buckets()));
  	baseAttrs.insert({"max_buckets", bl});
  	bl.clear();

  	baseAttrs.insert(attrs.begin(), attrs.end());
	*/
  }
  /*
  int set_attrsReturn = driver->get_cache_driver()->set_attrs(save_dpp, obj->get_key().get_oid(), baseAttrs);

  if (set_attrsReturn < 0) {
    ldpp_dout(save_dpp, 20) << "D4N Filter: Cache set attributes operation failed." << dendl;
  } else {
    ldpp_dout(save_dpp, 20) << "D4N Filter: Cache set attributes operation succeeded." << dendl;
  }
  */
  return ret;
  
}

int D4NFilterWriter::S3BackendComplete(size_t accounted_size)
{
  int ret = 0;
  this->obj_wr->set_send_length(accounted_size);
  ret = this->obj_wr->complete_request(null_yield);
  if (ret < 0){
	delete obj_wr;
	return ret;
  }

  return 0;
}



} } // namespace rgw::sal

extern "C" {

rgw::sal::Driver* newD4NFilter(rgw::sal::Driver* next)
{
  rgw::sal::D4NFilterDriver* driver = new rgw::sal::D4NFilterDriver(next);

  return driver;
}

}
