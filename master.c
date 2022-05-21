#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <netinet/in.h>

#include <sys/select.h>
#include <sys/socket.h>

#define DATA_BUF_SZ 256

#define MASTER_PORT 5000
#define MASTER_HEARTBEAT_PORT 5001
#define SLAVE_PORT 4000
#define SLAVE_HEARTBEAT_PORT 3000

#ifndef LOCAL
#define SLAVE_PORT_OFFS_MAX 1
#else
#define SLAVE_PORT_OFFS_MAX 16
#endif

typedef double real;

struct shard {
  struct sockaddr_in addr;
  real begin;
  real end;
  real int_sum;
};

struct heartbeat_state {
  int sk;
  long slaves_cnt;
};

static const real domain_sz = 5;

_Noreturn static void *
heartbeat(void *state);

int
main(int argc, const char *const *argv) {
  errno = 0;
  --argc;
  ++argv;

  if (argc != 1) {
    printf("USAGE: dist-int-comp-master <number of shards>\n");
    return EXIT_SUCCESS;
  }

  long n_shards = strtol(*argv, NULL, 10);
  if (errno != 0) {
    printf(
        "CLIENT ERROR: expected usage: dist-int-comp-master <number of "
        "shards>\n");
    return EXIT_FAILURE;
  }

  struct shard *shards = malloc(n_shards * sizeof(shards[0]));
  if (shards == NULL) {
    perror("malloc failed");
    return EXIT_FAILURE;
  }
  real part_sz = domain_sz / (real)n_shards;
  assert(part_sz > 0);
  for (size_t i = 0; i < n_shards; ++i) {
    shards[i].begin = part_sz * (real)i;
    shards[i].end = part_sz * (real)(i + 1);
    shards[i].int_sum = 0;
  }
  shards[n_shards - 1].end = domain_sz;

  int broadcast_sk = socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK, 0);
  struct sockaddr_in addr = {
      .sin_addr = INADDR_ANY,
      .sin_port = htons(MASTER_PORT),
      .sin_family = AF_INET,
  };
  bind(broadcast_sk, (struct sockaddr *)&addr, sizeof(addr));
  int opt = 1;
  setsockopt(broadcast_sk, SOL_SOCKET, SO_BROADCAST, &opt, sizeof(opt));

  struct heartbeat_state heartbeat_state = {
      .sk = socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK, 0),
      .slaves_cnt = n_shards,
  };
   struct sockaddr_in heartbeat_addr = {
      .sin_addr = INADDR_ANY,
      .sin_port = htons(MASTER_HEARTBEAT_PORT),
      .sin_family = AF_INET,
  };
  bind(heartbeat_state.sk, (struct sockaddr *)&heartbeat_addr,
        sizeof(heartbeat_addr));
  setsockopt(heartbeat_state.sk, SOL_SOCKET, SO_BROADCAST, &opt, sizeof(opt));

  char data_buf[DATA_BUF_SZ];
  uint64_t broadcast_msg = 0xDEADBEEF;
  struct sockaddr_in broadcast_addr = {
      .sin_addr = INADDR_BROADCAST,
      .sin_family = AF_INET,
  };

  for (size_t i = 0; i < SLAVE_PORT_OFFS_MAX; ++i) {
    broadcast_addr.sin_port = htons(SLAVE_PORT + i);
    sendto(broadcast_sk, &broadcast_msg, sizeof(broadcast_msg), 0,
           (struct sockaddr *)&broadcast_addr,  sizeof(broadcast_addr));
  }

  broadcast_msg = 0xDADBEEFDAD;
  for (size_t i = 0; i < SLAVE_PORT_OFFS_MAX; ++i) {
    broadcast_addr.sin_port = htons(SLAVE_HEARTBEAT_PORT + i);
    sendto(heartbeat_state.sk, &broadcast_msg, sizeof(broadcast_msg), 0,
           (struct sockaddr *)&broadcast_addr, sizeof(broadcast_addr));
  }

  struct timespec start;
  clock_gettime(CLOCK_MONOTONIC_RAW, &start);
  struct timeval accept_slaves_timeout = {
      .tv_sec = 30,
      .tv_usec = 0,
  };
  struct timespec curr = start;
  fd_set rfd;
  FD_ZERO(&rfd);
  FD_SET(broadcast_sk, &rfd);
  size_t shards_cnt = 0;
  while (curr.tv_sec - start.tv_sec < accept_slaves_timeout.tv_sec) {
    fd_set dirty_rfd = rfd;

    int ready = select(broadcast_sk + 1, &dirty_rfd, NULL, NULL,
                       &accept_slaves_timeout);
    if (ready == 0) {
      break;
    }

    uint64_t slave_msg = 0xBEEFDED;
    socklen_t addr_sz = sizeof(shards[0].addr);
    ssize_t bytes_read =
        recvfrom(broadcast_sk, data_buf, sizeof(slave_msg), 0,
                 (struct sockaddr *)&shards[shards_cnt].addr, &addr_sz);
    if (bytes_read != sizeof(slave_msg) ||
        memcmp(data_buf, &slave_msg, sizeof(slave_msg)) != 0) {
      goto update_curr;
    }

    ++shards_cnt;
    if (shards_cnt == n_shards) {
      break;
    }

  update_curr:
    clock_gettime(CLOCK_MONOTONIC_RAW, &curr);
  }
  if (shards_cnt != n_shards) {
    printf("MASTER ERROR: slaves did not connect\n");
    return EXIT_FAILURE;
  }

  for (size_t i = 0; i < shards_cnt; ++i) {
    memcpy(data_buf, &i, sizeof(i));
    memcpy(data_buf + sizeof(i), &shards[i].begin, sizeof(shards[i].begin));
    memcpy(data_buf + sizeof(i) + sizeof(shards[i].begin), &shards[i].end,
           sizeof(shards[i].end));
    size_t msg_sz = sizeof(i) + sizeof(shards[i].begin) + sizeof(shards[i].end);
    sendto(broadcast_sk, data_buf, msg_sz, 0,
           (struct sockaddr *)&shards[i].addr, sizeof(shards[i].addr));
  }

  pthread_t heartbeat_tid;
  pthread_create(&heartbeat_tid, NULL, heartbeat, &heartbeat_state);

  fcntl(broadcast_sk, F_SETFL, 0);
  real int_sum = 0;
  ssize_t shards_ready = 0;
  while (true) {
    size_t msg_sz = sizeof(size_t) + sizeof(real);
    ssize_t bytes_read = recvfrom(broadcast_sk, data_buf, msg_sz, 0, NULL, NULL);
    if (bytes_read != msg_sz) {
      continue;
    }

    size_t shard_idx;
    memcpy(&shard_idx, data_buf, sizeof(shard_idx));
    assert(shard_idx < shards_cnt || shards[shard_idx].int_sum != 0);
    memcpy(&shards[shard_idx].int_sum, data_buf + sizeof(shard_idx),
           sizeof(shards[shard_idx].int_sum));
    int_sum += shards[shard_idx].int_sum;
    ++shards_ready;
    if (shards_ready == shards_cnt) {
      break;
    }
  }
  assert(shards_ready == shards_cnt);
  printf("%lg\n", int_sum);
}

void *
heartbeat(void *state) {
  struct heartbeat_state *heartbeat_state = state;

  while (true) {
    struct timespec start;
    clock_gettime(CLOCK_MONOTONIC, &start);
    struct timeval timeout = {
        .tv_sec = 30,
        .tv_usec = 0,
    };
    struct timespec curr = start;
    fd_set rfd;
    FD_ZERO(&rfd);
    FD_SET(heartbeat_state->sk, &rfd);
    size_t slaves_cnt = 0;
    while (curr.tv_sec - start.tv_sec < timeout.tv_sec) {
      fd_set dirty_rfd = rfd;
      int ready = select(heartbeat_state->sk + 1, &dirty_rfd, NULL, NULL,
                         &timeout);
      if (ready == 0) {
        printf("HEARTBEAT ERROR: select timed out");
        break;
      }

      char data_buf[DATA_BUF_SZ];
      recvfrom(heartbeat_state->sk, data_buf, sizeof(uint64_t), 0,  NULL, NULL);

      ++slaves_cnt;
      if (slaves_cnt == heartbeat_state->slaves_cnt) {
        break;
      }

      clock_gettime(CLOCK_MONOTONIC, &curr);
    }
    if (slaves_cnt != heartbeat_state->slaves_cnt) {
      printf("HEARTBEAT ERROR: received %zu heartbeats from %ld slaves\n",
             slaves_cnt, heartbeat_state->slaves_cnt);
      exit(EXIT_FAILURE);
    }
    clock_gettime(CLOCK_MONOTONIC, &curr);
    if (curr.tv_sec - start.tv_sec < timeout.tv_sec) {
      sleep(timeout.tv_sec - curr.tv_sec + start.tv_sec);
    }
  }
}