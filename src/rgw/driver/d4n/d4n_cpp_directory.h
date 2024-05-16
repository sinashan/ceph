#include <stdlib.h>
#include <sys/types.h>
#include <sstream>
#include "rgw_common.h"
#include <cpp_redis/cpp_redis>
#include <string>
#include <iostream>
#include <vector>
#include <list>
#include <cstdint>

namespace rgw { namespace d4n {

using namespace std;

struct CacheObjectCpp {
  std::string objName; /* S3 object name */
  std::string bucketName; /* S3 bucket name */
  std::string creationTime; /* Creation time of the S3 Object */
  bool dirty;
  std::vector<std::string> hostsList; /* List of hostnames <ip:port> of object locations for multiple backends */
  std::string version;
  uint64_t size; /* Object size in bytes */
  bool in_lsvd = false; /* is it in LSVD cache? */
  rgw::sal::Attrs attrs; /* List of object attributes */
};

struct CacheBlockCpp {
  CacheObjectCpp cacheObj;
  uint64_t blockID;
  std::string version;
  bool dirty;
  uint64_t size; /* Block size in bytes */
  int globalWeight = 0; /* LFUDA policy variable */
  std::vector<std::string> hostsList; /* List of hostnames <ip:port> of block locations */
};



class RGWDirectory{
public:
	RGWDirectory() {}
	virtual ~RGWDirectory(){ cout << "RGW Directory is destroyed!";}
	CephContext *cct;

private:

};

class RGWBlockDirectory: public RGWDirectory {
public:

	RGWBlockDirectory() {}
	void init(CephContext *_cct) {
		cct = _cct;
	}
	virtual ~RGWBlockDirectory() { cout << "RGWObject Directory is destroyed!";}

	void findClient(std::string key, cpp_redis::client *client);
	int existKey(std::string key, cpp_redis::client *client);
	int setValue(CacheBlockCpp *ptr);
private:
	std::string buildIndex(CacheBlockCpp *ptr);
};

} } 
