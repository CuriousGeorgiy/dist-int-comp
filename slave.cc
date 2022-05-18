#include <assert.h>
#include <errno.h>
#include <math.h>
#include <netinet/in.h>
#include <pthread.h>
#include <atomic>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define DATA_BUF_SZ 256

#define SLAVE_PORT 4000
#define SLAVE_HEARTBEAT_PORT 3000

#ifdef LOCAL
#define SLAVE_PORT_OFFS_MAX 16
#endif

#define MAX(a, b) ((b) < (a) ? (a) : (b))

typedef double real;
static_assert(sizeof(real) <= sizeof(uintptr_t),
              "real must fit into function integer register");

struct worker_state {
  size_t core;
  real begin;
  real end;
  real sum;
};

#define DX 1e-7

static std::atomic_flag stop_heartbeat;

static void *
heartbeat(void *state);

static void *
worker(void *state);

static real
comp_int_sum_over_range(real begin, real end);

static real
integrand(real x);

int
main(int argc, const char *const *argv) {
  --argc;
  ++argv;

#ifndef LOCAL
  if (argc != 1) {
    printf("USAGE: dist-int-comp-slave <number of threads>\n");
    return EXIT_SUCCESS;
  }
#else
  if (argc != 2) {
    printf("USAGE: dist-int-comp-slave <port offset: 0...%d> "
           "<number of threads>\n", SLAVE_PORT_OFFS_MAX - 1);
    return EXIT_SUCCESS;
  }

  errno = 0;
  long port_offs = strtol(*argv, NULL, 10);
  if (errno != 0) {
    printf("USAGE: dist-int-comp-slave <port offset: 0...%d> "
           "<number of threads>\n", SLAVE_PORT_OFFS_MAX - 1);
    return EXIT_FAILURE;
  }
  if (port_offs >= SLAVE_PORT_OFFS_MAX) {
    printf("USAGE: dist-int-comp-slave <port offset: 0...%d> "
           "<number of threads>\n", SLAVE_PORT_OFFS_MAX - 1);
    return EXIT_FAILURE;
  }
  --argc;
  ++argv;
#endif

  errno = 0;
  long n_workers = strtol(*argv, NULL, 10);
  if (errno != 0) {
    printf(
        "CLIENT ERROR: expected usage: dist-int-comp-slave "
        "<number of threads>\n");
    return EXIT_FAILURE;
  }

  long cpus_cnt = sysconf(_SC_NPROCESSORS_ONLN);
  long cpu_cache_line_sz = sysconf(_SC_LEVEL1_DCACHE_LINESIZE);

  size_t n_threads = MAX(n_workers, cpus_cnt);
  size_t worker_state_sz =
      (sizeof(struct worker_state) / cpu_cache_line_sz + 1) * cpu_cache_line_sz;
  void *worker_states = malloc(n_threads * worker_state_sz);
  if (worker_states == NULL) {
    perror("malloc failed");
    return EXIT_FAILURE;
  }
  pthread_t *tids = (pthread_t *)malloc(n_threads * sizeof(tids[0]));
  if (tids == NULL) {
    perror("malloc failed");
    return EXIT_FAILURE;
  }

  int sk = socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK, 0);
  if (sk < 0) {
    perror("socket failed");
    return EXIT_FAILURE;
  }
  struct sockaddr_in addr;
#ifndef LOCAL
      addr.sin_port = htons(SLAVE_PORT);
#else
      addr.sin_port = htons(SLAVE_PORT + port_offs);
#endif
      addr.sin_family = AF_INET;
      addr.sin_addr = {INADDR_ANY};
  int rc = bind(sk, (struct sockaddr *)&addr, sizeof(addr));
  if (rc != 0) {
    perror("bind failed");
    return EXIT_FAILURE;
  }

  intptr_t heartbeat_sk = socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK, 0);
  if (heartbeat_sk < 0) {
    perror("socket failed");
    exit(EXIT_FAILURE);
  }
  struct sockaddr_in heartbeat_sk_addr;
#ifndef LOCAL
  heartbeat_sk_addr.sin_port = htons(SLAVE_HEARTBEAT_PORT);
#else
  heartbeat_sk_addr.sin_port = htons(SLAVE_HEARTBEAT_PORT + port_offs);
#endif
  heartbeat_sk_addr.sin_family = AF_INET;
  heartbeat_sk_addr.sin_addr = {INADDR_ANY};
  rc = bind((int)heartbeat_sk, (struct sockaddr *)&heartbeat_sk_addr,
            sizeof(heartbeat_sk_addr));
  if (rc != 0) {
    perror("bind failed");
    exit(EXIT_FAILURE);
  }

  while (true) {
  loop:
    std::atomic_flag_test_and_set(&stop_heartbeat);
    pthread_t heartbeat_tid;
#ifndef LOCAL
    rc = pthread_create(&heartbeat_tid, NULL, heartbeat, NULL);
#else
    rc = pthread_create(&heartbeat_tid, NULL, heartbeat, (void *)heartbeat_sk);
#endif
    if (rc != 0) {
      perror("pthread_create failed");
      return EXIT_FAILURE;
    }

    struct timespec start;
    rc = clock_gettime(CLOCK_MONOTONIC_RAW, &start);
    if (rc != 0) {
      perror("clock_gettime failed");
      return EXIT_FAILURE;
    }
    struct timespec curr = start;
    fd_set rfd;
    FD_ZERO(&rfd);
    FD_SET(sk, &rfd);
    char data_buf[DATA_BUF_SZ];
    struct sockaddr_in master_addr;
    socklen_t master_addr_sz = sizeof(master_addr);
    uint64_t broadcast_msg = 0xDEADBEEF;
    while (true) {
      fd_set dirty_rfd = rfd;

      int ready =
          select(sk + 1, &dirty_rfd, NULL, NULL, NULL);
      if (ready == -1) {
        perror("select failed");
        return EXIT_FAILURE;
      }
      if (ready == 0) {
        continue;
      }

      ssize_t bytes_read =
          recvfrom(sk, data_buf, sizeof(broadcast_msg), 0,
                   (struct sockaddr *)&master_addr, &master_addr_sz);
      if (bytes_read == 0) {
        continue;
      }
      if (bytes_read == -1) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
          continue;
        }
        perror("recvfrom failed");
        return EXIT_FAILURE;
      }
      if (bytes_read != sizeof(broadcast_msg) ||
          memcmp(data_buf, &broadcast_msg, sizeof(broadcast_msg)) != 0) {
        continue;
      }
      assert(master_addr_sz == sizeof(struct sockaddr_in));
      break;
    }

    uint64_t response = 0xBEEFDED;
    ssize_t bytes_sent =
        sendto(sk, &response, sizeof(response), 0,
               (struct sockaddr *)&master_addr, master_addr_sz);
    if (bytes_sent == -1) {
      perror("sendto failed");
      return EXIT_FAILURE;
    }
    assert(bytes_sent == sizeof(response));

    rc = clock_gettime(CLOCK_MONOTONIC_RAW, &start);
    if (rc != 0) {
      perror("clock_gettime failed");
      return EXIT_FAILURE;
    }
    curr = start;
    struct timeval master_shard_timeout = {
        .tv_sec = 30,
        .tv_usec = 0,
    };
    while (curr.tv_sec - start.tv_sec < master_shard_timeout.tv_sec) {
      fd_set dirty_rfd = rfd;

      int ready = select(sk + 1, &dirty_rfd, NULL, NULL, &master_shard_timeout);
      if (ready == -1) {
        perror("select failed");
        return EXIT_FAILURE;
      }
      if (ready == 0) {
        printf("MASTER ERROR: failed to receive task message from master\n");
        goto loop;
      }

      size_t task_msg_sz = sizeof(size_t) + 2 * sizeof(real);
      ssize_t bytes_read = recvfrom(sk, data_buf, task_msg_sz, 0, NULL, NULL);
      if (bytes_read == 0) {
        goto update_curr1;
      }
      if (bytes_read == -1) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
          goto update_curr1;
        }
        perror("recvfrom failed");
        return EXIT_FAILURE;
      }
      if (bytes_read != task_msg_sz) {
        goto update_curr1;
      }

      break;

    update_curr1:
      rc = clock_gettime(CLOCK_MONOTONIC_RAW, &curr);
      if (rc != 0) {
        perror("clock_gettime failed");
        return EXIT_FAILURE;
      }
    }
    if (curr.tv_sec - start.tv_sec > master_shard_timeout.tv_sec) {
      printf("MASTER ERROR: failed to receive shard information\n");
      goto loop;
    }

    real shard_begin;
    real shard_end;
    memcpy(&shard_begin, data_buf + sizeof(size_t), sizeof(shard_begin));
    memcpy(&shard_end, data_buf + sizeof(size_t) + sizeof(real),
           sizeof(shard_end));
    assert(shard_begin < shard_end);

    real part_sz = (shard_end - shard_begin) / (real)n_workers;
    assert(part_sz > 0);
    assert(part_sz > 0);
  for (size_t i = 0; i < n_threads; ++i) {
    *(struct worker_state *)((char *)worker_states + i * worker_state_sz) =
        (struct worker_state){
            .core = (i < n_workers) ? i % cpus_cnt : -1,
            .begin = shard_begin + ((i < n_workers) ? part_sz * (real)i : 0),
            .end = shard_begin + ((i < n_workers) ? part_sz * (real)(i + 1) : part_sz),
        };
  }

  for (size_t i = 0; i < n_threads; ++i) {
    rc = pthread_create(&tids[i], NULL, worker,
                        (char *)worker_states + i * worker_state_sz);
    if (rc != 0) {
      errno = rc;
      perror("pthread_create failed");
      return EXIT_FAILURE;
    }
  }

  real int_sum = 0;
    for (size_t i = 0; i < n_threads; ++i) {
      pthread_join(tids[i], NULL);
      if (i < n_workers) {
       int_sum += ((struct worker_state *)((char *)worker_states + i * worker_state_sz))->sum;
      }
    }

    memcpy(data_buf + sizeof(size_t), &int_sum, sizeof(int_sum));
    size_t msg_sz = sizeof(size_t) + sizeof(real);
    bytes_sent = sendto(sk, data_buf, msg_sz, 0,
                        (struct sockaddr *)&master_addr, master_addr_sz);
    if (bytes_sent == -1) {
      perror("sendto failed");
      return EXIT_FAILURE;
    }
    assert(bytes_sent == sizeof(size_t) + sizeof(real));

    std::atomic_flag_clear(&stop_heartbeat);
    pthread_join(heartbeat_tid, NULL);
  }
}

void *
heartbeat(void *state) {
  int sk = (int)(intptr_t)state;

  fd_set rfd;
  FD_ZERO(&rfd);
  FD_SET(sk, &rfd);
  char data_buf[DATA_BUF_SZ];
  struct sockaddr_in master_addr;
  socklen_t master_addr_sz = sizeof(master_addr);
  uint64_t master_msg = 0xDADBEEFDAD;
  while (true) {
    fd_set dirty_rfd = rfd;
    int ready = select(sk + 1, &dirty_rfd, NULL, NULL, NULL);
    if (ready == -1) {
      perror("select failed");
      exit(EXIT_FAILURE);
    }
    if (ready == 0) {
      continue;
    }

    ssize_t bytes_read =
        recvfrom(sk, data_buf, sizeof(master_msg), 0,
                 (struct sockaddr *)&master_addr, &master_addr_sz);
    if (bytes_read == 0) {
      continue;
    }
    if (bytes_read == -1) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        continue;
      }
      perror("recvfrom failed");
      exit(EXIT_FAILURE);
    }
    if (bytes_read != sizeof(master_msg) ||
        memcmp(data_buf, &master_msg, sizeof(master_msg)) != 0) {
      continue;
    }
    assert(master_addr_sz == sizeof(struct sockaddr_in));
    break;
  }

  while (true) {
    bool flag = std::atomic_flag_test_and_set(&stop_heartbeat);
    if (!flag) {
      return NULL;
    }

    size_t heartbeat_msg = 0xDEDEDEDED;
    ssize_t bytes_sent =
        sendto(sk, &heartbeat_msg, sizeof(heartbeat_msg), 0,
               (struct sockaddr *)&master_addr, sizeof(master_addr));
    if (bytes_sent == -1) {
      perror("sendto failed");
      exit(EXIT_FAILURE);
    }
    assert(bytes_sent == sizeof(heartbeat_msg));
    sleep(20);
  }
}

void *
worker(void *state) {
  struct worker_state *worker_state = (struct worker_state *)state;
  if (worker_state->core != -1) {
    cpu_set_t cpu_set;
    CPU_ZERO(&cpu_set);
    CPU_SET(worker_state->core, &cpu_set);
    pthread_setaffinity_np(pthread_self(), CPU_SETSIZE, &cpu_set);
  }
  worker_state->sum =
      comp_int_sum_over_range(worker_state->begin, worker_state->end);
  return NULL;
}

real
comp_int_sum_over_range(real begin, real end) {
  real int_sum = 0;
  for (real x = begin; x < end; x += DX) {
    int_sum += integrand(x) * DX;
  }
  return int_sum;
}

real
integrand(real x) {
  return cos(pow(x, 5) * sin(cos(x)));
}
