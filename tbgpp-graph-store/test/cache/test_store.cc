#include <assert.h>
#include <cstring>
#include <dirent.h>
#include <fcntl.h>
#include <iostream>
#include <signal.h>
#include <stdlib.h>
#include <string>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/poll.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <thread>
#include <unistd.h>
#include <unordered_set>
#include <vector>
#include <memory>

#include "log_disk.h"
#include "object_log.h"
#include "store.h"

#include <memory>

int main() {
  // if (signal(SIGINT, signal_handler) == SIG_ERR) {
  //   std::cerr << "cannot register signal handler!" << std::endl;
  //   exit(-1);
  // }

  std::shared_ptr<LightningStore> store = std::make_shared<LightningStore>(
      "/tmp/lightning", 320 * 1024 * 1024 * 1024L);
  store->Run();

  return 0;
}
