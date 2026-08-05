#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <pthread.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <cmocka.h>
#include <libwebsockets.h>

#include "channel-c/channel_c.h"
#include "../../src/core/internal.h"

typedef struct {
  pthread_mutex_t lock;
  char *last_message;
  int message_count;
} mock_transport_t;

typedef struct {
  pthread_mutex_t lock;
  int running;
  int connected;
} client_private_t;

typedef struct test_client_queue_message test_client_queue_message_t;

struct test_client_queue_message {
  char *text;
  test_client_queue_message_t *next;
};

typedef struct {
  pthread_mutex_t lock;
  int running;
  int connected;
  int close_notified;
  struct lws_context *context;
  struct lws *wsi;
  pthread_t thread;
  char *url;
  char *protocol;
  char *address;
  char *path;
  int port;
  int ssl;
  test_client_queue_message_t *queue_head;
  test_client_queue_message_t *queue_tail;
  char *rx_buffer;
  size_t rx_length;
  size_t rx_capacity;
  channel_c_connection_t *connection;
} client_runtime_private_t;

typedef struct {
  pthread_mutex_t lock;
  int running;
  struct lws_context *context;
} server_private_t;

typedef struct {
  pthread_mutex_t lock;
  int closed;
  void *server;
  struct lws *wsi;
  channel_c_connection_t *connection;
} server_client_private_t;

typedef struct test_stream_queue_item test_stream_queue_item_t;
typedef struct test_pending_request test_pending_request_t;
typedef struct test_listener_callback test_listener_callback_t;
typedef struct test_listener_entry test_listener_entry_t;
typedef struct test_topic_interest test_topic_interest_t;
typedef struct test_active_stream test_active_stream_t;
typedef struct test_channel_link test_channel_link_t;

struct test_stream_queue_item {
  char *body;
  char *error;
  int done;
  test_stream_queue_item_t *next;
};

struct test_pending_request {
  int id;
  int type;
  int completed;
  char *body;
  char *error;
  char *topic;
  int stream_done;
  test_stream_queue_item_t *stream_head;
  test_stream_queue_item_t *stream_tail;
  pthread_cond_t cond;
  test_pending_request_t *next;
};

struct test_listener_callback {
  int id;
  channel_c_event_callback_fn callback;
  void *user_data;
  test_listener_callback_t *next;
};

struct test_listener_entry {
  char *topic;
  test_listener_callback_t *callbacks;
  test_listener_entry_t *next;
};

struct test_topic_interest {
  char *topic;
  test_topic_interest_t *next;
};

struct test_active_stream {
  int id;
  int cancelled;
  test_active_stream_t *next;
};

struct test_channel_link {
  channel_c_connection_t *connection;
  test_channel_link_t *next;
};

typedef struct {
  channel_c_connection_t *connection;
} sender_private_t;

typedef struct {
  channel_c_connection_t *connection;
  int request_id;
  int closed;
} values_private_t;

typedef struct {
  channel_c_connection_t *connection;
  char *topic;
  int callback_id;
  int cancelled;
} subscription_handle_private_t;

typedef struct {
  pthread_mutex_t lock;
  pthread_cond_t connected_cond;
  int ref_count;
  int is_connected;
  int is_closed;
  int next_request_id;
  int next_callback_id;
  channel_c_channel_t *channel;
  test_channel_link_t *channel_link;
  test_pending_request_t *pending;
  test_listener_entry_t *listeners;
  test_topic_interest_t *remote_topics;
  test_active_stream_t *active_streams;
  sender_private_t sender;
  void *state;
  channel_c_state_destroy_fn state_destroy;
  void *state_destroy_user_data;
  void *transport_ctx;
  channel_c_transport_ops_t transport_ops;
} connection_private_t;

static int find_free_port(void) {
  int fd;
  int port = -1;
  struct sockaddr_in address;
  socklen_t length = sizeof(address);

  fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) return -1;

  memset(&address, 0, sizeof(address));
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = 0;

  if (bind(fd, (struct sockaddr *)&address, sizeof(address)) != 0) {
    close(fd);
    return -1;
  }

  if (getsockname(fd, (struct sockaddr *)&address, &length) == 0) {
    port = (int)ntohs(address.sin_port);
  }

  close(fd);
  return port;
}

static int wait_server_connections(channel_c_server_t *server, int expected, int timeout_ms) {
  int elapsed = 0;
  while (elapsed < timeout_ms) {
    if (channel_c_server_connection_count(server) == expected) return 1;
    usleep(1000);
    elapsed += 1;
  }
  return 0;
}

static int wait_client_connected(channel_c_client_t *client, int timeout_ms) {
  int elapsed = 0;
  while (elapsed < timeout_ms) {
    if (channel_c_client_is_connected(client)) return 1;
    usleep(1000);
    elapsed += 1;
  }
  return 0;
}

static int wait_client_disconnected(channel_c_client_t *client, int timeout_ms) {
  int elapsed = 0;
  while (elapsed < timeout_ms) {
    if (!channel_c_client_is_connected(client)) return 1;
    usleep(1000);
    elapsed += 1;
  }
  return 0;
}

static int wait_message_count(mock_transport_t *transport, int expected, int timeout_ms) {
  int elapsed = 0;
  while (elapsed < timeout_ms) {
    int count;
    pthread_mutex_lock(&transport->lock);
    count = transport->message_count;
    pthread_mutex_unlock(&transport->lock);
    if (count >= expected) return 1;
    usleep(1000);
    elapsed += 1;
  }
  return 0;
}

static int mock_send_text(void *ctx, const char *text) {
  mock_transport_t *transport = (mock_transport_t *)ctx;
  if (!transport || !text) return -1;

  pthread_mutex_lock(&transport->lock);
  free(transport->last_message);
  transport->last_message = channel_c_strdup(text);
  transport->message_count += 1;
  pthread_mutex_unlock(&transport->lock);
  return 0;
}

static void mock_stop(void *ctx) {
  (void)ctx;
}

static char *copy_last_message(mock_transport_t *transport) {
  char *copy = NULL;
  pthread_mutex_lock(&transport->lock);
  if (transport->last_message) copy = channel_c_strdup(transport->last_message);
  pthread_mutex_unlock(&transport->lock);
  return copy;
}

static int message_id_from_json_text(const char *json_text, int *out_id) {
  const char *marker;
  char *endptr;
  long value;
  if (!json_text || !out_id) return -1;
  marker = strstr(json_text, "\"id\":");
  if (!marker) return -1;
  marker += 5;
  value = strtol(marker, &endptr, 10);
  if (endptr == marker) return -1;
  *out_id = (int)value;
  return 0;
}

static int post_ok(const channel_c_post_request_t *request, char **out_body_json, char **out_error) {
  (void)request;
  (void)out_error;
  *out_body_json = channel_c_strdup("1");
  return *out_body_json ? 0 : -1;
}

static void stream_noop(const channel_c_stream_request_t *request) {
  (void)request;
}

static void event_noop(const char *body_json, void *user_data) {
  (void)body_json;
  (void)user_data;
}

static void stream_short(const channel_c_stream_request_t *request) {
  (void)request;
  usleep(10000);
}

static void stream_long(const channel_c_stream_request_t *request) {
  (void)request;
  usleep(120000);
}

static void test_process_failpoints(void **state) {
  channel_c_channel_t *channel;
  channel_c_process_t *process;

  (void)state;

  channel_c_internal_failpoint_reset();
  channel = channel_c_channel_new();
  assert_non_null(channel);

  channel_c_internal_failpoint_set("process.alloc_fail", 1);
  assert_null(channel_c_process_new(channel, NULL, NULL));

  channel_c_internal_failpoint_set("process.mutex_init_fail", 1);
  assert_null(channel_c_process_new(channel, NULL, NULL));

  channel_c_internal_failpoint_set("process.connection_create_fail", 1);
  assert_null(channel_c_process_new(channel, NULL, NULL));

  process = channel_c_process_new(channel, NULL, NULL);
  assert_non_null(process);
  assert_non_null(channel_c_internal_process_connection(process));
  assert_null(channel_c_internal_process_connection(NULL));
  channel_c_process_stop(process);
  assert_int_not_equal(channel_c_internal_process_send(process, "{}"), 0);

  channel_c_process_free(process);
  channel_c_channel_free(channel);
  channel_c_internal_failpoint_reset();
}

static void test_client_connect_failpoints(void **state) {
  channel_c_channel_t *channel;
  channel_c_channel_t *server_channel;
  channel_c_server_t *server;
  channel_c_state_factory_t factory = {0};
  channel_c_client_t *client;
  char *error = NULL;
  int port;
  char url[64];

  (void)state;

  channel_c_internal_failpoint_reset();
  channel = channel_c_channel_new();
  assert_non_null(channel);

  channel_c_internal_failpoint_set("client.client_alloc_fail", 1);
  client = channel_c_client_connect(channel, "ws://127.0.0.1:12345", NULL, NULL, &error);
  assert_null(client);
  assert_non_null(error);
  assert_string_equal(error, "internal error");
  channel_c_free(error);
  error = NULL;

  channel_c_internal_failpoint_set("client.mutex_init_fail", 1);
  client = channel_c_client_connect(channel, "ws://127.0.0.1:12345", NULL, NULL, &error);
  assert_null(client);
  assert_non_null(error);
  assert_string_equal(error, "internal error");
  channel_c_free(error);
  error = NULL;

  channel_c_internal_failpoint_set("client.parse_url_fail", 1);
  client = channel_c_client_connect(channel, "ws://127.0.0.1:12345", NULL, NULL, &error);
  assert_null(client);
  assert_non_null(error);
  assert_string_equal(error, "invalid websocket url");
  channel_c_free(error);
  error = NULL;

  channel_c_internal_failpoint_set("client.parse_uri_fail", 1);
  client = channel_c_client_connect(channel, "ws://127.0.0.1:12345", NULL, NULL, &error);
  assert_null(client);
  assert_non_null(error);
  assert_string_equal(error, "invalid websocket url");
  channel_c_free(error);
  error = NULL;

  channel_c_internal_failpoint_set("client.protocol_strdup_fail", 1);
  client = channel_c_client_connect(channel, "ws://127.0.0.1:12345", NULL, NULL, &error);
  assert_null(client);
  assert_non_null(error);
  assert_string_equal(error, "invalid websocket url");
  channel_c_free(error);
  error = NULL;

  channel_c_internal_failpoint_set("client.connection_create_fail", 1);
  client = channel_c_client_connect(channel, "ws://127.0.0.1:12345", NULL, NULL, &error);
  assert_null(client);
  assert_non_null(error);
  assert_string_equal(error, "internal error");
  channel_c_free(error);
  error = NULL;

  channel_c_internal_failpoint_set("client.context_create_fail", 1);
  client = channel_c_client_connect(channel, "ws://127.0.0.1:12345", NULL, NULL, &error);
  assert_null(client);
  assert_non_null(error);
  assert_string_equal(error, "failed to create websocket context");
  channel_c_free(error);
  error = NULL;

  channel_c_internal_failpoint_set("client.connect_via_info_fail", 1);
  client = channel_c_client_connect(channel, "ws://127.0.0.1:12345", NULL, NULL, &error);
  assert_null(client);
  assert_non_null(error);
  assert_string_equal(error, "failed to connect websocket");
  channel_c_free(error);
  error = NULL;

  server_channel = channel_c_channel_new();
  assert_non_null(server_channel);
  port = find_free_port();
  assert_true(port > 0);
  snprintf(url, sizeof(url), "ws://127.0.0.1:%d", port);
  server = channel_c_server_listen(server_channel, "127.0.0.1", port, factory, &error);
  assert_non_null(server);
  assert_null(error);

  client = channel_c_client_connect(channel, url, NULL, NULL, &error);
  assert_non_null(client);
  assert_null(error);
  assert_true(wait_client_connected(client, 2000));
  assert_non_null(channel_c_internal_client_connection(client));
  assert_null(channel_c_internal_client_connection(NULL));
  channel_c_client_free(client);

  channel_c_internal_failpoint_set("client.thread_create_fail", 1);
  client = channel_c_client_connect(channel, url, NULL, NULL, &error);
  assert_null(client);
  assert_non_null(error);
  assert_string_equal(error, "failed to start websocket thread");
  channel_c_free(error);

  channel_c_server_free(server);
  channel_c_channel_free(server_channel);
  channel_c_channel_free(channel);
  channel_c_internal_failpoint_reset();
}

static void test_server_failpoints(void **state) {
  channel_c_channel_t *channel;
  channel_c_channel_t *client_channel;
  channel_c_server_t *server;
  channel_c_client_t *client;
  channel_c_sender_t *sender;
  channel_c_state_factory_t factory = {0};
  char *body = NULL;
  char *error = NULL;
  int port;
  char url[64];

  (void)state;

  channel_c_internal_failpoint_reset();
  channel = channel_c_channel_new();
  client_channel = channel_c_channel_new();
  assert_non_null(channel);
  assert_non_null(client_channel);
  assert_int_equal(channel_c_channel_post(channel, "hello", post_ok, NULL), 0);

  channel_c_internal_failpoint_set("server.server_alloc_fail", 1);
  assert_null(channel_c_server_listen(channel, "127.0.0.1", 2201, factory, &error));
  assert_non_null(error);
  assert_string_equal(error, "internal error");
  channel_c_free(error);
  error = NULL;

  channel_c_internal_failpoint_set("server.mutex_init_fail", 1);
  assert_null(channel_c_server_listen(channel, "127.0.0.1", 2201, factory, &error));
  assert_non_null(error);
  assert_string_equal(error, "internal error");
  channel_c_free(error);
  error = NULL;

  channel_c_internal_failpoint_set("server.host_strdup_fail", 1);
  assert_null(channel_c_server_listen(channel, "127.0.0.1", 2201, factory, &error));
  assert_non_null(error);
  assert_string_equal(error, "internal error");
  channel_c_free(error);
  error = NULL;

  channel_c_internal_failpoint_set("server.context_create_fail", 1);
  assert_null(channel_c_server_listen(channel, "127.0.0.1", 2201, factory, &error));
  assert_non_null(error);
  assert_string_equal(error, "failed to create websocket server context");
  channel_c_free(error);
  error = NULL;

  channel_c_internal_failpoint_set("server.thread_create_fail", 1);
  assert_null(channel_c_server_listen(channel, "127.0.0.1", 2201, factory, &error));
  assert_non_null(error);
  assert_string_equal(error, "failed to start websocket server thread");
  channel_c_free(error);
  error = NULL;

  port = find_free_port();
  assert_true(port > 0);
  snprintf(url, sizeof(url), "ws://127.0.0.1:%d", port);

  server = channel_c_server_listen(channel, "127.0.0.1", port, factory, &error);
  assert_non_null(server);
  assert_null(error);

  client = channel_c_client_connect(client_channel, url, NULL, NULL, &error);
  assert_non_null(client);
  assert_null(error);
  assert_true(wait_client_connected(client, 2000));
  assert_true(wait_server_connections(server, 1, 2000));
  assert_non_null(channel_c_internal_server_first_connection(server));
  assert_null(channel_c_internal_server_first_connection(NULL));

  sender = channel_c_client_sender(client);
  assert_non_null(sender);

  channel_c_internal_failpoint_set("server.write_buffer_alloc_fail", 1);
  assert_int_not_equal(channel_c_sender_send(sender, "hello", NULL, NULL, &body, &error), 0);
  channel_c_free(body);
  channel_c_free(error);
  body = NULL;
  error = NULL;

  channel_c_internal_failpoint_set("server.write_fail", 1);
  assert_int_not_equal(channel_c_sender_send(sender, "hello", NULL, NULL, &body, &error), 0);
  channel_c_free(body);
  channel_c_free(error);
  body = NULL;
  error = NULL;

  channel_c_client_free(client);
  (void)wait_server_connections(server, 0, 2000);
  channel_c_server_free(server);
  channel_c_channel_free(client_channel);
  channel_c_channel_free(channel);
  channel_c_internal_failpoint_reset();
}

static void test_client_runtime_failpoints(void **state) {
  channel_c_channel_t *server_channel;
  channel_c_channel_t *client_channel;
  channel_c_server_t *server;
  channel_c_client_t *client;
  channel_c_sender_t *sender;
  channel_c_values_t *values;
  channel_c_state_factory_t factory = {0};
  client_private_t *client_private;
  char *body = NULL;
  char *error = NULL;
  int port;
  char url[64];

  (void)state;

  channel_c_internal_failpoint_reset();

  server_channel = channel_c_channel_new();
  client_channel = channel_c_channel_new();
  assert_non_null(server_channel);
  assert_non_null(client_channel);
  assert_int_equal(channel_c_channel_post(server_channel, "hello", post_ok, NULL), 0);
  assert_int_equal(channel_c_channel_stream(server_channel, "s", stream_noop, NULL), 0);

  port = find_free_port();
  assert_true(port > 0);
  snprintf(url, sizeof(url), "ws://127.0.0.1:%d", port);

  server = channel_c_server_listen(server_channel, "127.0.0.1", port, factory, &error);
  assert_non_null(server);
  assert_null(error);

  /* queue push/clear branches */
  client = channel_c_client_connect(client_channel, url, NULL, NULL, &error);
  assert_non_null(client);
  assert_null(error);
  assert_true(wait_client_connected(client, 2000));
  sender = channel_c_client_sender(client);
  assert_non_null(sender);

  channel_c_internal_failpoint_set("client.queue_pop_skip", 200);
  channel_c_internal_failpoint_set("client.queue_strdup_fail", 1);
  values = channel_c_sender_values(sender, "s", NULL, NULL, &error);
  assert_non_null(values);
  assert_null(error);
  channel_c_values_close(values);

  values = channel_c_sender_values(sender, "s", NULL, NULL, &error);
  assert_non_null(values);
  assert_null(error);
  channel_c_values_close(values);

  values = channel_c_sender_values(sender, "s", NULL, NULL, &error);
  assert_non_null(values);
  assert_null(error);
  channel_c_values_close(values);

  channel_c_client_free(client);

  /* write buffer allocation failure branch */
  client = channel_c_client_connect(client_channel, url, NULL, NULL, &error);
  assert_non_null(client);
  assert_null(error);
  assert_true(wait_client_connected(client, 2000));
  sender = channel_c_client_sender(client);
  assert_non_null(sender);
  channel_c_internal_failpoint_set("client.write_buffer_alloc_fail", 1);
  assert_int_not_equal(channel_c_sender_send(sender, "hello", NULL, NULL, &body, &error), 0);
  channel_c_free(body);
  channel_c_free(error);
  body = NULL;
  error = NULL;

  channel_c_internal_failpoint_set("client.write_fail", 1);
  assert_int_not_equal(channel_c_sender_send(sender, "hello", NULL, NULL, &body, &error), 0);
  channel_c_free(body);
  channel_c_free(error);
  body = NULL;
  error = NULL;
  channel_c_client_free(client);

  /* receive realloc failure branch */
  client = channel_c_client_connect(client_channel, url, NULL, NULL, &error);
  assert_non_null(client);
  assert_null(error);
  assert_true(wait_client_connected(client, 2000));
  sender = channel_c_client_sender(client);
  assert_non_null(sender);
  channel_c_internal_failpoint_set("client.rx_realloc_fail", 1);
  assert_int_not_equal(channel_c_sender_send(sender, "hello", NULL, NULL, &body, &error), 0);
  channel_c_free(body);
  channel_c_free(error);
  body = NULL;
  error = NULL;
  channel_c_client_free(client);

  /* send_text branch when running is already false */
  client = channel_c_client_connect(client_channel, url, NULL, NULL, &error);
  assert_non_null(client);
  assert_null(error);
  assert_true(wait_client_connected(client, 2000));
  sender = channel_c_client_sender(client);
  assert_non_null(sender);

  client_private = (client_private_t *)client;
  pthread_mutex_lock(&client_private->lock);
  client_private->running = 0;
  pthread_mutex_unlock(&client_private->lock);

  assert_int_not_equal(channel_c_sender_send(sender, "hello", NULL, NULL, &body, &error), 0);
  channel_c_free(body);
  channel_c_free(error);

  channel_c_client_free(client);
  channel_c_server_free(server);
  channel_c_channel_free(client_channel);
  channel_c_channel_free(server_channel);
  channel_c_internal_failpoint_reset();
}

static void test_server_runtime_failpoints(void **state) {
  channel_c_channel_t *server_channel;
  channel_c_channel_t *client_channel;
  channel_c_server_t *server;
  channel_c_client_t *client_temp = NULL;
  channel_c_client_t *client_a = NULL;
  channel_c_client_t *client_b = NULL;
  channel_c_client_t *fail_client = NULL;
  channel_c_sender_t *sender_temp;
  channel_c_sender_t *sender_b;
  channel_c_sender_t *server_sender;
  channel_c_values_t *values;
  channel_c_connection_t *server_connection;
  connection_private_t *connection_private;
  server_client_private_t *server_client_private;
  server_private_t *server_private;
  channel_c_state_factory_t factory = {0};
  char *body = NULL;
  char *error = NULL;
  int attempt = 0;
  int port;
  char url[64];

  (void)state;

  channel_c_internal_failpoint_reset();
  server_channel = channel_c_channel_new();
  client_channel = channel_c_channel_new();
  assert_non_null(server_channel);
  assert_non_null(client_channel);
  assert_int_equal(channel_c_channel_post(server_channel, "hello", post_ok, NULL), 0);
  assert_int_equal(channel_c_channel_stream(server_channel, "s", stream_noop, NULL), 0);

  port = find_free_port();
  assert_true(port > 0);
  snprintf(url, sizeof(url), "ws://127.0.0.1:%d", port);

  server = channel_c_server_listen(server_channel, "127.0.0.1", port, factory, &error);
  assert_non_null(server);
  assert_null(error);

  client_temp = channel_c_client_connect(client_channel, url, NULL, NULL, &error);
  assert_non_null(client_temp);
  assert_null(error);
  assert_true(wait_client_connected(client_temp, 2000));
  assert_true(wait_server_connections(server, 1, 2000));
  sender_temp = channel_c_client_sender(client_temp);
  assert_non_null(sender_temp);
  server_connection = channel_c_internal_server_first_connection(server);
  assert_non_null(server_connection);
  connection_private = (connection_private_t *)server_connection;
  assert_non_null(connection_private);
  assert_non_null(connection_private->transport_ops.send_text);

  channel_c_internal_failpoint_set("client.queue_pop_skip", 200);
  values = channel_c_sender_values(sender_temp, "s", NULL, NULL, &error);
  assert_non_null(values);
  assert_null(error);
  assert_int_equal(connection_private->transport_ops.send_text(connection_private->transport_ctx, "{\"id\":9000,\"body\":1}"), 0);
  usleep(50000);
  channel_c_values_close(values);

  channel_c_internal_failpoint_set("client.rx_realloc_fail", 50);
  for (attempt = 0; attempt < 4; attempt += 1) {
    assert_int_equal(connection_private->transport_ops.send_text(connection_private->transport_ctx, "{\"id\":9001,\"body\":1}"), 0);
    usleep(30000);
    if (wait_client_disconnected(client_temp, 200)) break;
  }
  (void)wait_client_disconnected(client_temp, 2000);
  channel_c_client_free(client_temp);
  client_temp = NULL;
  assert_true(wait_server_connections(server, 0, 2000));
  channel_c_internal_failpoint_reset();

  channel_c_internal_failpoint_set("server.client_alloc_fail", 8);
  for (attempt = 0; attempt < 4; attempt += 1) {
    fail_client = channel_c_client_connect(client_channel, url, NULL, NULL, &error);
    if (fail_client) {
      (void)wait_client_disconnected(fail_client, 2000);
      channel_c_client_free(fail_client);
    }
    fail_client = NULL;
    channel_c_free(error);
    error = NULL;
  }
  assert_true(wait_server_connections(server, 0, 2000));

  channel_c_internal_failpoint_set("server.client_mutex_init_fail", 8);
  for (attempt = 0; attempt < 4; attempt += 1) {
    fail_client = channel_c_client_connect(client_channel, url, NULL, NULL, &error);
    if (fail_client) {
      (void)wait_client_disconnected(fail_client, 2000);
      channel_c_client_free(fail_client);
    }
    fail_client = NULL;
    channel_c_free(error);
    error = NULL;
  }
  assert_true(wait_server_connections(server, 0, 2000));

  channel_c_internal_failpoint_set("server.connection_create_fail", 8);
  for (attempt = 0; attempt < 4; attempt += 1) {
    fail_client = channel_c_client_connect(client_channel, url, NULL, NULL, &error);
    if (fail_client) {
      (void)wait_client_disconnected(fail_client, 2000);
      channel_c_client_free(fail_client);
    }
    fail_client = NULL;
    channel_c_free(error);
    error = NULL;
  }
  assert_true(wait_server_connections(server, 0, 2000));

  channel_c_internal_failpoint_reset();

  client_a = channel_c_client_connect(client_channel, url, NULL, NULL, &error);
  assert_non_null(client_a);
  assert_null(error);
  assert_true(wait_client_connected(client_a, 2000));

  client_b = channel_c_client_connect(client_channel, url, NULL, NULL, &error);
  assert_non_null(client_b);
  assert_null(error);
  assert_true(wait_client_connected(client_b, 2000));
  assert_true(wait_server_connections(server, 2, 2000));

  channel_c_client_free(client_a);
  client_a = NULL;
  assert_true(wait_server_connections(server, 1, 2000));

  sender_b = channel_c_client_sender(client_b);
  assert_non_null(sender_b);
  server_connection = channel_c_internal_server_first_connection(server);
  assert_non_null(server_connection);

  channel_c_internal_failpoint_set("server.write_fail", 1);
  assert_int_not_equal(channel_c_sender_send(sender_b, "hello", NULL, NULL, &body, &error), 0);
  channel_c_free(body);
  channel_c_free(error);
  body = NULL;
  error = NULL;

  if (!wait_client_connected(client_b, 500)) {
    channel_c_client_free(client_b);
    client_b = channel_c_client_connect(client_channel, url, NULL, NULL, &error);
    assert_non_null(client_b);
    assert_null(error);
    assert_true(wait_client_connected(client_b, 2000));
    assert_true(wait_server_connections(server, 1, 2000));
    sender_b = channel_c_client_sender(client_b);
    assert_non_null(sender_b);
    server_connection = channel_c_internal_server_first_connection(server);
    assert_non_null(server_connection);
  }

  channel_c_internal_failpoint_set("server.queue_strdup_fail", 1);
  channel_c_connection_receive_text(server_connection, "{\"id\":9100,\"path\":\"hello\"}");

  connection_private = (connection_private_t *)server_connection;
  server_client_private = (server_client_private_t *)connection_private->transport_ctx;
  assert_non_null(server_client_private);
  server_sender = channel_c_connection_sender(server_connection);
  assert_non_null(server_sender);

  pthread_mutex_lock(&server_client_private->lock);
  server_client_private->closed = 1;
  pthread_mutex_unlock(&server_client_private->lock);
  assert_int_not_equal(channel_c_sender_send(server_sender, "hello", NULL, NULL, &body, &error), 0);
  channel_c_free(body);
  channel_c_free(error);
  body = NULL;
  error = NULL;
  pthread_mutex_lock(&server_client_private->lock);
  server_client_private->closed = 0;
  pthread_mutex_unlock(&server_client_private->lock);

  channel_c_internal_failpoint_set("server.queue_pop_skip", 200);
  channel_c_connection_receive_text(server_connection, "{\"id\":9101,\"path\":\"hello\"}");
  usleep(50000);

  server_private = (server_private_t *)server;
  pthread_mutex_lock(&server_private->lock);
  if (server_private->context) lws_cancel_service(server_private->context);
  pthread_mutex_unlock(&server_private->lock);
  usleep(50000);

  channel_c_client_free(client_b);
  client_b = NULL;
  assert_true(wait_server_connections(server, 0, 2000));

  channel_c_server_free(server);
  server = NULL;

  channel_c_channel_free(client_channel);
  channel_c_channel_free(server_channel);
  channel_c_internal_failpoint_reset();
}

static void test_core_failpoints(void **state) {
  channel_c_channel_t *channel;
  channel_c_channel_t *source;
  channel_c_channel_t *destination;
  channel_c_process_t *process;
  channel_c_sender_t *sender;
  channel_c_connection_t *connection;
  channel_c_transport_ops_t ops;
  channel_c_subscription_handle_t *handle = NULL;
  mock_transport_t transport = {0};
  char *error = NULL;
  char *body = NULL;

  (void)state;

  channel_c_internal_failpoint_reset();
  assert_int_equal(pthread_mutex_init(&transport.lock, NULL), 0);

  channel_c_internal_failpoint_set("core.channel_new_null", 1);
  assert_null(channel_c_channel_new());

  channel = channel_c_channel_new();
  assert_non_null(channel);
  assert_int_equal(channel_c_channel_post(channel, "p", post_ok, NULL), 0);
  assert_int_equal(channel_c_channel_stream(channel, "s", stream_noop, NULL), 0);
  assert_int_equal(channel_c_channel_subscription(channel, "sub", NULL, NULL), 0);

  ops.send_text = mock_send_text;
  ops.stop = mock_stop;
  connection = channel_c_connection_create(channel, NULL, NULL, NULL, &transport, ops);
  assert_non_null(connection);
  channel_c_internal_connection_mark_connected(connection, 1);
  sender = channel_c_connection_sender(connection);

  channel_c_internal_failpoint_set("core.pending_create_fail", 1);
  assert_int_not_equal(channel_c_sender_send(sender, "p", NULL, NULL, &body, &error), 0);
  assert_non_null(error);
  assert_string_equal(error, "internal error");
  channel_c_free(error);
  error = NULL;

  channel_c_internal_failpoint_set("core.request_json_fail", 1);
  assert_int_not_equal(channel_c_sender_send(sender, "p", NULL, NULL, &body, &error), 0);
  assert_non_null(error);
  assert_string_equal(error, "invalid json body or context");
  channel_c_free(error);
  error = NULL;

  channel_c_internal_failpoint_set("core.values_alloc_fail", 1);
  assert_null(channel_c_sender_values(sender, "s", NULL, NULL, &error));
  assert_non_null(error);
  assert_string_equal(error, "internal error");
  channel_c_free(error);
  error = NULL;

  channel_c_internal_failpoint_set("core.connection_unavailable", 1);
  assert_int_not_equal(channel_c_sender_send(sender, "p", NULL, NULL, &body, &error), 0);
  assert_non_null(error);
  assert_string_equal(error, "connection is not available");
  channel_c_free(error);
  error = NULL;

  channel_c_internal_failpoint_set("core.post_task_alloc_noid_fail", 1);
  channel_c_connection_receive_text(connection, "{\"path\":\"p\"}");

  channel_c_internal_failpoint_set("core.post_task_thread_noid_fail", 1);
  channel_c_connection_receive_text(connection, "{\"path\":\"p\"}");

  channel_c_internal_failpoint_set("core.post_task_alloc_withid_fail", 1);
  channel_c_connection_receive_text(connection, "{\"id\":41,\"path\":\"p\"}");
  assert_true(wait_message_count(&transport, 1, 1000));

  channel_c_internal_failpoint_set("core.post_task_thread_withid_fail", 1);
  channel_c_connection_receive_text(connection, "{\"id\":42,\"path\":\"p\"}");
  assert_true(wait_message_count(&transport, 2, 1000));

  channel_c_internal_failpoint_set("core.stream_task_alloc_fail", 1);
  channel_c_connection_receive_text(connection, "{\"id\":43,\"stream\":\"s\"}");
  assert_true(wait_message_count(&transport, 3, 1000));

  channel_c_internal_failpoint_set("core.stream_task_thread_fail", 1);
  channel_c_connection_receive_text(connection, "{\"id\":44,\"stream\":\"s\"}");
  assert_true(wait_message_count(&transport, 4, 1000));

  channel_c_internal_failpoint_set("core.sub_full_topic_alloc_fail", 1);
  channel_c_connection_receive_text(connection, "{\"id\":45,\"sub\":\"sub\",\"body\":\"x\"}");
  assert_true(wait_message_count(&transport, 5, 1000));

  channel_c_connection_receive_text(connection, "{\"x\":1}");
  channel_c_connection_receive_text(connection, "[{\"x\":1},{\"id\":46,\"path\":\"missing\"}]");
  assert_true(wait_message_count(&transport, 6, 1000));

  channel_c_internal_failpoint_set("core.post_handler_null", 1);
  channel_c_connection_receive_text(connection, "{\"id\":47,\"path\":\"p\"}");
  assert_true(wait_message_count(&transport, 7, 1000));

  channel_c_internal_failpoint_set("core.force_deadline_overflow", 1);
  channel_c_internal_failpoint_set("core.connection_unavailable", 1);
  assert_int_not_equal(channel_c_sender_send(sender, "p", NULL, NULL, &body, &error), 0);
  channel_c_free(error);
  error = NULL;

  channel_c_connection_on_close(connection);
  channel_c_connection_release(connection);

  process = channel_c_process_new(channel, NULL, NULL);
  assert_non_null(process);
  sender = channel_c_process_sender(process);

  channel_c_internal_failpoint_set("core.listener_alloc_fail", 1);
  assert_int_not_equal(channel_c_sender_subscribe(sender, "sub", "\"a\"", NULL, event_noop, NULL, &handle, &error), 0);
  assert_non_null(error);
  channel_c_free(error);
  error = NULL;

  channel_c_internal_failpoint_set("core.listener_topic_strdup_fail", 1);
  assert_int_not_equal(channel_c_sender_subscribe(sender, "sub", "\"a\"", NULL, event_noop, NULL, &handle, &error), 0);
  assert_non_null(error);
  channel_c_free(error);
  error = NULL;

  channel_c_internal_failpoint_set("core.callback_entry_alloc_fail", 1);
  assert_int_not_equal(channel_c_sender_subscribe(sender, "sub", "\"a\"", NULL, event_noop, NULL, &handle, &error), 0);
  assert_non_null(error);
  channel_c_free(error);
  error = NULL;

  channel_c_internal_failpoint_set("core.subscription_handle_alloc_fail", 1);
  assert_int_not_equal(channel_c_sender_subscribe(sender, "sub", "\"a\"", NULL, event_noop, NULL, &handle, &error), 0);
  assert_non_null(error);
  channel_c_free(error);
  error = NULL;

  channel_c_internal_failpoint_set("core.subscribe_missing_topic", 1);
  assert_int_not_equal(channel_c_sender_subscribe(sender, "sub", "\"a\"", NULL, event_noop, NULL, &handle, &error), 0);
  assert_non_null(error);
  channel_c_free(error);
  error = NULL;

  assert_int_equal(channel_c_sender_subscribe(sender, "sub", "\"a\"", NULL, event_noop, NULL, &handle, &error), 0);
  assert_non_null(handle);
  channel_c_internal_failpoint_set("core.event_full_topic_alloc_fail", 1);
  assert_int_not_equal(channel_c_channel_event_send(channel, "sub", "\"a\"", "\"e\"", &error), 0);
  assert_non_null(error);
  channel_c_free(error);
  error = NULL;
  channel_c_subscription_cancel(handle);

  channel_c_process_free(process);

  channel_c_internal_failpoint_set("core.channel_post_path_strdup_fail", 1);
  assert_int_not_equal(channel_c_channel_post(channel, "x", post_ok, NULL), 0);
  channel_c_internal_failpoint_set("core.channel_stream_path_strdup_fail", 1);
  assert_int_not_equal(channel_c_channel_stream(channel, "x", stream_noop, NULL), 0);
  channel_c_internal_failpoint_set("core.channel_subscription_name_strdup_fail", 1);
  assert_int_not_equal(channel_c_channel_subscription(channel, "x", NULL, NULL), 0);

  source = channel_c_channel_new();
  destination = channel_c_channel_new();
  assert_non_null(source);
  assert_non_null(destination);
  assert_int_equal(channel_c_channel_post(source, "m", post_ok, NULL), 0);
  channel_c_internal_failpoint_set("core.join_prefix_alloc_fail", 1);
  assert_int_not_equal(channel_c_channel_merge(destination, source, "a"), 0);

  channel_c_channel_free(source);
  channel_c_channel_free(destination);
  channel_c_channel_free(channel);
  channel_c_internal_failpoint_reset();

  free(transport.last_message);
  pthread_mutex_destroy(&transport.lock);
}

static void test_core_remaining_branches(void **state) {
  channel_c_channel_t *channel;
  channel_c_channel_t *source;
  channel_c_channel_t *destination;
  channel_c_connection_t *connection;
  channel_c_connection_t *conn_a;
  channel_c_connection_t *conn_b;
  channel_c_connection_t *timeout_connection;
  channel_c_sender_t *sender;
  channel_c_sender_t *timeout_sender;
  channel_c_transport_ops_t ops;
  connection_private_t *connection_private;
  sender_private_t detached_sender = {0};
  values_private_t fake_values = {0};
  subscription_handle_private_t *handle_a_old;
  subscription_handle_private_t *handle_a_new;
  subscription_handle_private_t *handle_b;
  test_listener_entry_t *listener_a;
  test_listener_entry_t *listener_b;
  test_listener_callback_t *callback_a_old;
  test_listener_callback_t *callback_a_new;
  test_listener_callback_t *callback_b;
  test_topic_interest_t *topic_head;
  test_topic_interest_t *topic_tail;
  test_pending_request_t *pending_close;
  test_pending_request_t *pending_manual_a;
  test_pending_request_t *pending_manual_b;
  test_active_stream_t *active_manual;
  channel_c_values_t *values_ctx;
  channel_c_values_t *values_a;
  channel_c_values_t *values_b;
  char *message = NULL;
  char *body = NULL;
  char *error = NULL;
  int id_a = -1;
  int id_b = -1;
  int done = 0;
  char response[128];

  (void)state;

  channel_c_internal_failpoint_reset();

  {
    mock_transport_t transport = {0};
    assert_int_equal(pthread_mutex_init(&transport.lock, NULL), 0);

    channel_c_internal_failpoint_set("core.channel_mutex_init_fail", 1);
    assert_null(channel_c_channel_new());

    channel = channel_c_channel_new();
    assert_non_null(channel);
    assert_int_equal(channel_c_channel_post(channel, "hello", post_ok, NULL), 0);
    assert_int_equal(channel_c_channel_stream(channel, "short", stream_short, NULL), 0);
    assert_int_equal(channel_c_channel_stream(channel, "long", stream_long, NULL), 0);
    assert_int_equal(channel_c_channel_subscription(channel, "plain", NULL, NULL), 0);
    assert_int_equal(channel_c_channel_subscription(channel, "subraw", NULL, NULL), 0);

    ops.send_text = mock_send_text;
    ops.stop = mock_stop;

    channel_c_internal_failpoint_set("core.connection_mutex_init_fail", 1);
    assert_null(channel_c_connection_create(channel, NULL, NULL, NULL, &transport, ops));

    channel_c_internal_failpoint_set("core.connection_cond_init_fail", 1);
    assert_null(channel_c_connection_create(channel, NULL, NULL, NULL, &transport, ops));

    connection = channel_c_connection_create(channel, NULL, NULL, NULL, &transport, ops);
    assert_non_null(connection);
    channel_c_internal_connection_mark_connected(connection, 1);
    sender = channel_c_connection_sender(connection);
    assert_non_null(sender);
    connection_private = (connection_private_t *)connection;

    channel_c_internal_failpoint_set("core.pending_cond_init_fail", 1);
    assert_int_not_equal(channel_c_sender_send(sender, "hello", NULL, NULL, &body, &error), 0);
    assert_non_null(error);
    channel_c_free(error);
    error = NULL;

    detached_sender.connection = NULL;
    assert_int_not_equal(channel_c_sender_send((channel_c_sender_t *)&detached_sender, "hello", NULL, NULL, &body, &error), 0);
    assert_non_null(error);
    channel_c_free(error);
    error = NULL;
    assert_null(channel_c_sender_values((channel_c_sender_t *)&detached_sender, "short", NULL, NULL, &error));
    assert_non_null(error);
    channel_c_free(error);
    error = NULL;

    assert_int_not_equal(channel_c_sender_subscribe(sender, "plain", "\"x\"", NULL, NULL, NULL, NULL, &error), 0);
    assert_non_null(error);
    channel_c_free(error);
    error = NULL;

    channel_c_internal_failpoint_set("core.connection_unavailable", 1);
    assert_int_not_equal(channel_c_sender_subscribe(sender, "plain", "\"x\"", NULL, event_noop, NULL, NULL, &error), 0);
    assert_non_null(error);
    channel_c_free(error);
    error = NULL;

    assert_int_not_equal(channel_c_channel_event_send(NULL, "plain", "\"x\"", "\"b\"", &error), 0);
    assert_non_null(error);
    channel_c_free(error);
    error = NULL;

    assert_int_not_equal(channel_c_channel_event_send(channel, "missing", "\"x\"", "\"b\"", &error), 0);
    assert_non_null(error);
    channel_c_free(error);
    error = NULL;

    assert_int_equal(channel_c_channel_event_send(channel, "plain", NULL, "\"b\"", &error), 0);
    assert_null(error);
    assert_int_equal(channel_c_channel_event_send(channel, "plain", "{\"k\":1}", "\"b\"", &error), 0);
    assert_null(error);

    channel_c_internal_failpoint_set("core.request_doc_alloc_fail", 1);
    assert_int_not_equal(channel_c_sender_send(sender, "hello", NULL, NULL, &body, &error), 0);
    channel_c_free(body);
    body = NULL;
    assert_non_null(error);
    channel_c_free(error);
    error = NULL;

    source = channel_c_channel_new();
    destination = channel_c_channel_new();
    assert_non_null(source);
    assert_non_null(destination);
    assert_int_equal(channel_c_channel_stream(source, "s", stream_noop, NULL), 0);
    channel_c_internal_failpoint_set("core.channel_stream_path_strdup_fail", 1);
    assert_int_not_equal(channel_c_channel_merge(destination, source, "p"), 0);
    channel_c_channel_free(source);
    channel_c_channel_free(destination);

    source = channel_c_channel_new();
    destination = channel_c_channel_new();
    assert_non_null(source);
    assert_non_null(destination);
    assert_int_equal(channel_c_channel_subscription(source, "n", NULL, NULL), 0);
    channel_c_internal_failpoint_set("core.channel_subscription_name_strdup_fail", 1);
    assert_int_not_equal(channel_c_channel_merge(destination, source, "p"), 0);
    channel_c_channel_free(source);
    channel_c_channel_free(destination);

    values_ctx = channel_c_sender_values(sender, "short", "1", "{\"ctx\":1}", &error);
    assert_non_null(values_ctx);
    assert_null(error);
    channel_c_values_close(values_ctx);

    values_a = channel_c_sender_values(sender, "short", NULL, NULL, &error);
    assert_non_null(values_a);
    assert_null(error);
    message = copy_last_message(&transport);
    assert_non_null(message);
    assert_int_equal(message_id_from_json_text(message, &id_a), 0);
    channel_c_free(message);
    message = NULL;

    values_b = channel_c_sender_values(sender, "short", NULL, NULL, &error);
    assert_non_null(values_b);
    assert_null(error);
    message = copy_last_message(&transport);
    assert_non_null(message);
    assert_int_equal(message_id_from_json_text(message, &id_b), 0);
    channel_c_free(message);
    message = NULL;

    snprintf(response, sizeof(response), "{\"id\":%d,\"body\":1}", id_a);
    channel_c_connection_receive_text(connection, response);
    snprintf(response, sizeof(response), "{\"id\":%d,\"body\":2}", id_a);
    channel_c_connection_receive_text(connection, response);
    snprintf(response, sizeof(response), "{\"id\":%d,\"done\":true}", id_a);
    channel_c_connection_receive_text(connection, response);

    channel_c_values_close(values_a);
    channel_c_values_close(values_b);

    fake_values.connection = connection;
    fake_values.request_id = 999999;
    fake_values.closed = 0;
    done = 0;
    assert_int_equal(channel_c_values_next((channel_c_values_t *)&fake_values, &body, &done, &error), 0);
    assert_int_equal(done, 1);
    assert_null(error);

    pending_close = (test_pending_request_t *)calloc(1, sizeof(test_pending_request_t));
    assert_non_null(pending_close);
    pending_close->id = 4040;
    pending_close->type = 1;
    assert_int_equal(pthread_cond_init(&pending_close->cond, NULL), 0);
    connection_private->pending = pending_close;
    connection_private->is_closed = 1;
    fake_values.request_id = 4040;
    fake_values.closed = 0;
    assert_int_not_equal(channel_c_values_next((channel_c_values_t *)&fake_values, &body, &done, &error), 0);
    assert_non_null(error);
    channel_c_free(error);
    error = NULL;
    connection_private->is_closed = 0;
    connection_private->pending = NULL;
    pthread_cond_destroy(&pending_close->cond);
    free(pending_close);

    pending_close = (test_pending_request_t *)calloc(1, sizeof(test_pending_request_t));
    assert_non_null(pending_close);
    pending_close->id = 4041;
    pending_close->type = 1;
    pending_close->stream_done = 1;
    assert_int_equal(pthread_cond_init(&pending_close->cond, NULL), 0);
    connection_private->pending = pending_close;
    fake_values.request_id = 4041;
    fake_values.closed = 0;
    done = 0;
    assert_int_equal(channel_c_values_next((channel_c_values_t *)&fake_values, &body, &done, &error), 0);
    assert_int_equal(done, 1);
    assert_null(error);

    channel_c_connection_receive_text(connection, "{\"id\":501,\"stream\":\"short\"}");
    usleep(5000);
    channel_c_connection_receive_text(connection, "{\"id\":502,\"stream\":\"long\"}");
    channel_c_connection_receive_text(connection, "{\"cancel\":999}");
    channel_c_connection_receive_text(connection, "{\"id\":601,\"sub\":\"subraw\"}");
    channel_c_connection_receive_text(connection, "{\"id\":602,\"sub\":\"subraw\",\"body\":{\"k\":1}}");
    channel_c_internal_failpoint_set("core.topic_set_strdup_fail", 1);
    channel_c_connection_receive_text(connection, "{\"id\":603,\"sub\":\"subraw\",\"body\":\"fresh\"}");
    usleep(200000);

    topic_head = (test_topic_interest_t *)calloc(1, sizeof(test_topic_interest_t));
    topic_tail = (test_topic_interest_t *)calloc(1, sizeof(test_topic_interest_t));
    assert_non_null(topic_head);
    assert_non_null(topic_tail);
    topic_head->topic = channel_c_strdup("keep");
    topic_tail->topic = channel_c_strdup("drop");
    assert_non_null(topic_head->topic);
    assert_non_null(topic_tail->topic);
    topic_head->next = topic_tail;
    connection_private->remote_topics = topic_head;
    assert_int_equal(channel_c_connection_is_remote_subscribed(connection, "drop"), 1);
    channel_c_internal_failpoint_set("core.event_doc_alloc_fail", 1);
    assert_int_equal(channel_c_channel_event_send(channel, "plain", "\"keep\"", "\"b\"", &error), 0);
    assert_null(error);
    channel_c_connection_receive_text(connection, "{\"unsub\":\"drop\"}");

    listener_a = (test_listener_entry_t *)calloc(1, sizeof(test_listener_entry_t));
    listener_b = (test_listener_entry_t *)calloc(1, sizeof(test_listener_entry_t));
    callback_a_old = (test_listener_callback_t *)calloc(1, sizeof(test_listener_callback_t));
    callback_a_new = (test_listener_callback_t *)calloc(1, sizeof(test_listener_callback_t));
    callback_b = (test_listener_callback_t *)calloc(1, sizeof(test_listener_callback_t));
    assert_non_null(listener_a);
    assert_non_null(listener_b);
    assert_non_null(callback_a_old);
    assert_non_null(callback_a_new);
    assert_non_null(callback_b);

    listener_a->topic = channel_c_strdup("topic-a");
    listener_b->topic = channel_c_strdup("topic-b");
    assert_non_null(listener_a->topic);
    assert_non_null(listener_b->topic);

    callback_a_old->id = 10;
    callback_a_old->callback = event_noop;
    callback_a_new->id = 20;
    callback_a_new->callback = event_noop;
    callback_b->id = 30;
    callback_b->callback = event_noop;
    callback_a_new->next = callback_a_old;
    listener_a->callbacks = callback_a_new;
    listener_b->callbacks = callback_b;
    listener_b->next = listener_a;
    connection_private->listeners = listener_b;

    channel_c_connection_receive_text(connection, "{\"topic\":\"topic-a\",\"body\":1}");

    pthread_mutex_lock(&connection_private->lock);
    connection_private->ref_count += 3;
    pthread_mutex_unlock(&connection_private->lock);

    handle_a_old = (subscription_handle_private_t *)calloc(1, sizeof(subscription_handle_private_t));
    handle_a_new = (subscription_handle_private_t *)calloc(1, sizeof(subscription_handle_private_t));
    handle_b = (subscription_handle_private_t *)calloc(1, sizeof(subscription_handle_private_t));
    assert_non_null(handle_a_old);
    assert_non_null(handle_a_new);
    assert_non_null(handle_b);

    handle_a_old->connection = connection;
    handle_a_old->topic = channel_c_strdup("topic-a");
    handle_a_old->callback_id = 10;
    handle_a_new->connection = connection;
    handle_a_new->topic = channel_c_strdup("topic-a");
    handle_a_new->callback_id = 20;
    handle_b->connection = connection;
    handle_b->topic = channel_c_strdup("topic-b");
    handle_b->callback_id = 30;
    assert_non_null(handle_a_old->topic);
    assert_non_null(handle_a_new->topic);
    assert_non_null(handle_b->topic);

    channel_c_subscription_cancel((channel_c_subscription_handle_t *)handle_a_old);
    channel_c_subscription_cancel((channel_c_subscription_handle_t *)handle_a_new);
    channel_c_internal_failpoint_set("core.unsubscribe_doc_alloc_fail", 1);
    channel_c_subscription_cancel((channel_c_subscription_handle_t *)handle_b);

    conn_a = channel_c_connection_create(channel, NULL, NULL, NULL, &transport, ops);
    conn_b = channel_c_connection_create(channel, NULL, NULL, NULL, &transport, ops);
    assert_non_null(conn_a);
    assert_non_null(conn_b);
    channel_c_connection_on_close(conn_a);
    channel_c_connection_release(conn_a);
    channel_c_connection_on_close(conn_b);
    channel_c_connection_release(conn_b);

    channel_c_internal_failpoint_set("core.force_deadline_overflow", 1);
    timeout_connection = channel_c_connection_create(channel, NULL, NULL, NULL, &transport, ops);
    assert_non_null(timeout_connection);
    timeout_sender = channel_c_connection_sender(timeout_connection);
    assert_non_null(timeout_sender);
    assert_int_not_equal(channel_c_sender_send(timeout_sender, "hello", NULL, NULL, &body, &error), 0);
    assert_non_null(error);
    channel_c_free(error);
    error = NULL;
    channel_c_connection_on_close(timeout_connection);
    channel_c_connection_release(timeout_connection);

    pending_manual_a = (test_pending_request_t *)calloc(1, sizeof(test_pending_request_t));
    pending_manual_b = (test_pending_request_t *)calloc(1, sizeof(test_pending_request_t));
    active_manual = (test_active_stream_t *)calloc(1, sizeof(test_active_stream_t));
    assert_non_null(pending_manual_a);
    assert_non_null(pending_manual_b);
    assert_non_null(active_manual);
    pending_manual_a->id = 9001;
    pending_manual_a->type = 0;
    pending_manual_b->id = 9002;
    pending_manual_b->type = 0;
    assert_int_equal(pthread_cond_init(&pending_manual_a->cond, NULL), 0);
    assert_int_equal(pthread_cond_init(&pending_manual_b->cond, NULL), 0);
    pending_manual_a->next = pending_manual_b;
    connection_private->pending = pending_manual_a;
    active_manual->id = 77;
    connection_private->active_streams = active_manual;

    assert_int_equal(channel_c_connection_is_remote_subscribed(connection, "keep"), 1);

    channel_c_connection_on_close(connection);
    channel_c_connection_release(connection);
    channel_c_channel_free(channel);

    free(transport.last_message);
    pthread_mutex_destroy(&transport.lock);
  }

  channel_c_internal_failpoint_reset();
}

int main(void) {
  const struct CMUnitTest tests[] = {
    cmocka_unit_test(test_process_failpoints),
    cmocka_unit_test(test_client_connect_failpoints),
    cmocka_unit_test(test_server_failpoints),
    cmocka_unit_test(test_client_runtime_failpoints),
    cmocka_unit_test(test_server_runtime_failpoints),
    cmocka_unit_test(test_core_failpoints),
    cmocka_unit_test(test_core_remaining_branches),
  };
  return cmocka_run_group_tests(tests, NULL, NULL);
}
