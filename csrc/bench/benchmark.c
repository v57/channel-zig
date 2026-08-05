#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

#include "channel-c/channel_c.h"

static atomic_long g_counter = 0;

static int bench_hello(const channel_c_post_request_t *request, char **out_body_json, char **out_error) {
  (void)request;
  (void)out_body_json;
  (void)out_error;
  atomic_fetch_add_explicit(&g_counter, 1, memory_order_relaxed);
  return 0;
}

typedef struct {
  channel_c_sender_t *sender;
  int64_t deadline_ns;
} worker_args_t;

static int64_t now_ns(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

static void *bench_worker(void *argument) {
  worker_args_t *args = (worker_args_t *)argument;
  while (now_ns() < args->deadline_ns) {
    char *body = NULL;
    char *error = NULL;
    if (channel_c_sender_send(args->sender, "hello", NULL, NULL, &body, &error) != 0) {
      channel_c_free(error);
      break;
    }
    channel_c_free(body);
  }
  return NULL;
}

static void run_process_benchmark(int threads, int duration_ms) {
  channel_c_channel_t *channel;
  channel_c_process_t *process;
  channel_c_sender_t *sender;
  pthread_t *workers;
  worker_args_t *args;
  int64_t start;
  int64_t end;
  int i;

  channel = channel_c_channel_new();
  channel_c_channel_post(channel, "hello", bench_hello, NULL);
  process = channel_c_process_new(channel, NULL, NULL);
  sender = channel_c_process_sender(process);

  workers = (pthread_t *)calloc((size_t)threads, sizeof(pthread_t));
  args = (worker_args_t *)calloc((size_t)threads, sizeof(worker_args_t));

  atomic_store_explicit(&g_counter, 0, memory_order_relaxed);
  start = now_ns();
  end = start + ((int64_t)duration_ms * 1000000LL);

  for (i = 0; i < threads; i += 1) {
    args[i].sender = sender;
    args[i].deadline_ns = end;
    pthread_create(&workers[i], NULL, bench_worker, &args[i]);
  }

  for (i = 0; i < threads; i += 1) {
    pthread_join(workers[i], NULL);
  }

  {
    double elapsed = (double)(now_ns() - start) / 1000000000.0;
    long count = atomic_load_explicit(&g_counter, memory_order_relaxed);
    double ops = elapsed > 0.0 ? count / elapsed : 0.0;
    printf("%10.0f ops process (%d threads)\n", ops, threads);
  }

  free(workers);
  free(args);
  channel_c_process_free(process);
  channel_c_channel_free(channel);
}

static void run_ws_benchmark(int threads, int duration_ms) {
  channel_c_channel_t *server_channel;
  channel_c_server_t *server;
  channel_c_channel_t *client_channel;
  channel_c_client_t *client;
  channel_c_sender_t *sender;
  pthread_t *workers;
  worker_args_t *args;
  int64_t start;
  int64_t end;
  int i;
  char *error = NULL;

  server_channel = channel_c_channel_new();
  channel_c_channel_post(server_channel, "hello", bench_hello, NULL);

  {
    channel_c_state_factory_t factory = {0};
    server = channel_c_server_listen(server_channel, "127.0.0.1", 2051, factory, &error);
  }
  if (!server) {
    fprintf(stderr, "failed to start benchmark server: %s\n", error ? error : "unknown");
    channel_c_free(error);
    channel_c_channel_free(server_channel);
    return;
  }

  client_channel = channel_c_channel_new();
  client = channel_c_client_connect(client_channel, "ws://127.0.0.1:2051", NULL, NULL, &error);
  if (!client) {
    fprintf(stderr, "failed to connect benchmark client: %s\n", error ? error : "unknown");
    channel_c_free(error);
    channel_c_server_free(server);
    channel_c_channel_free(client_channel);
    channel_c_channel_free(server_channel);
    return;
  }
  sender = channel_c_client_sender(client);

  workers = (pthread_t *)calloc((size_t)threads, sizeof(pthread_t));
  args = (worker_args_t *)calloc((size_t)threads, sizeof(worker_args_t));

  atomic_store_explicit(&g_counter, 0, memory_order_relaxed);
  start = now_ns();
  end = start + ((int64_t)duration_ms * 1000000LL);

  for (i = 0; i < threads; i += 1) {
    args[i].sender = sender;
    args[i].deadline_ns = end;
    pthread_create(&workers[i], NULL, bench_worker, &args[i]);
  }

  for (i = 0; i < threads; i += 1) {
    pthread_join(workers[i], NULL);
  }

  {
    double elapsed = (double)(now_ns() - start) / 1000000000.0;
    long count = atomic_load_explicit(&g_counter, memory_order_relaxed);
    double ops = elapsed > 0.0 ? count / elapsed : 0.0;
    printf("%10.0f ops websocket (%d threads)\n", ops, threads);
  }

  free(workers);
  free(args);

  channel_c_client_free(client);
  channel_c_channel_free(client_channel);

  channel_c_server_free(server);
  channel_c_channel_free(server_channel);
}

int main(void) {
  printf("channel-c benchmark (500ms windows)\n");
  run_process_benchmark(1, 500);
  run_process_benchmark(10, 500);
  run_ws_benchmark(1, 500);
  run_ws_benchmark(10, 500);
  run_ws_benchmark(100, 500);
  return 0;
}
