#ifndef CATALOG_SERVER_H
#define CATALOG_SERVER_H

#include <mutex>
#include <string>
#include <sys/types.h>
#include <unordered_set>

#include "config.h"
#include "malloc.h"

class CatalogServer {
public:
  CatalogServer();
  void Run();

private:
  void monitor();
  void listener();
};

#endif // STORE_H
