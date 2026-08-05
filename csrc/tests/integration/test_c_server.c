#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <cmocka.h>
#include <yyjson.h>

#include "channel-c/channel_c.h"

typedef struct {
  char *name;
} auth_state_t;

static int g_client_values_sent = 0;
static int g_server_stream_cancel_sent = 0;
static int g_server_mirror_events = 0;
static channel_c_subscription_handle_t *g_server_mirror_handle = NULL;

static void free_auth_state(void *state, void *user_data) {
  auth_state_t *auth = (auth_state_t *)state;
  (void)user_data;
  if (!auth) return;
  free(auth->name);
  free(auth);
}

static void *make_auth_state(void *user_data) {
  (void)user_data;
  return calloc(1, sizeof(auth_state_t));
}

static int server_hello(const channel_c_post_request_t *request, char **out_body_json, char **out_error) {
  (void)request;
  (void)out_error;
  *out_body_json = channel_c_strdup("\"world\"");
  return *out_body_json ? 0 : -1;
}

static int server_echo(const channel_c_post_request_t *request, char **out_body_json, char **out_error) {
  (void)out_error;
  if (request->body_json) {
    *out_body_json = channel_c_strdup(request->body_json);
  } else {
    *out_body_json = channel_c_strdup("null");
  }
  return *out_body_json ? 0 : -1;
}

static int server_empty(const channel_c_post_request_t *request, char **out_body_json, char **out_error) {
  (void)request;
  (void)out_body_json;
  (void)out_error;
  return 0;
}

static int server_disconnect(const channel_c_post_request_t *request, char **out_body_json, char **out_error) {
  (void)out_body_json;
  (void)out_error;
  channel_c_sender_stop(request->sender);
  return 0;
}

static int server_auth(const channel_c_post_request_t *request, char **out_body_json, char **out_error) {
  auth_state_t *state = (auth_state_t *)request->state;
  yyjson_doc *doc;
  yyjson_val *root;
  yyjson_val *name;

  if (!state || !request->body_json) {
    *out_error = channel_c_strdup("unauthorized");
    return -1;
  }

  doc = yyjson_read(request->body_json, strlen(request->body_json), 0);
  if (!doc) {
    *out_error = channel_c_strdup("invalid body");
    return -1;
  }

  root = yyjson_doc_get_root(doc);
  name = yyjson_obj_get(root, "name");
  if (!yyjson_is_str(name)) {
    yyjson_doc_free(doc);
    *out_error = channel_c_strdup("invalid body");
    return -1;
  }

  free(state->name);
  state->name = channel_c_strdup(yyjson_get_str(name));
  yyjson_doc_free(doc);

  if (!state->name) {
    *out_error = channel_c_strdup("internal error");
    return -1;
  }

  {
    size_t length = strlen(state->name) + 3;
    *out_body_json = (char *)malloc(length);
    if (!*out_body_json) {
      *out_error = channel_c_strdup("internal error");
      return -1;
    }
    snprintf(*out_body_json, length, "\"%s\"", state->name);
  }

  return 0;
}

static int server_auth_name(const channel_c_post_request_t *request, char **out_body_json, char **out_error) {
  auth_state_t *state = (auth_state_t *)request->state;
  if (!state || !state->name) {
    *out_error = channel_c_strdup("unauthorized");
    return -1;
  }

  {
    size_t length = strlen(state->name) + 3;
    *out_body_json = (char *)malloc(length);
    if (!*out_body_json) {
      *out_error = channel_c_strdup("internal error");
      return -1;
    }
    snprintf(*out_body_json, length, "\"%s\"", state->name);
  }

  return 0;
}

static void server_stream_values(const channel_c_stream_request_t *request) {
  int i;
  for (i = 0; i < 3; i += 1) {
    char value[16];
    if (channel_c_stream_is_cancelled(request->emitter)) return;
    snprintf(value, sizeof(value), "%d", i);
    if (channel_c_stream_emit_json(request->emitter, value) != 0) return;
    usleep(1000);
  }
}

static void server_stream_cancel(const channel_c_stream_request_t *request) {
  int i;
  for (i = 0; i < 10; i += 1) {
    char value[16];
    if (channel_c_stream_is_cancelled(request->emitter)) return;
    snprintf(value, sizeof(value), "%d", i);
    if (channel_c_stream_emit_json(request->emitter, value) != 0) return;
    g_server_stream_cancel_sent += 1;
    usleep(1000);
  }
}

static int server_mirror(const channel_c_post_request_t *request, char **out_body_json, char **out_error) {
  return channel_c_sender_send(request->sender, "hello", NULL, NULL, out_body_json, out_error);
}

static void on_server_mirror_event(const char *body_json, void *user_data) {
  (void)body_json;
  (void)user_data;
  g_server_mirror_events += 1;
  if (g_server_mirror_handle) {
    channel_c_subscription_cancel(g_server_mirror_handle);
    g_server_mirror_handle = NULL;
  }
}

static int server_mirror_events(const channel_c_post_request_t *request, char **out_body_json, char **out_error) {
  (void)out_body_json;
  if (g_server_mirror_handle) {
    channel_c_subscription_cancel(g_server_mirror_handle);
    g_server_mirror_handle = NULL;
  }
  return channel_c_sender_subscribe(
    request->sender,
    "names",
    "\"1\"",
    NULL,
    on_server_mirror_event,
    NULL,
    &g_server_mirror_handle,
    out_error
  );
}

static void server_mirror_stream(const channel_c_stream_request_t *request) {
  channel_c_values_t *values;
  char *error = NULL;
  char *body = NULL;
  int done = 0;

  values = channel_c_sender_values(request->sender, "stream/values", NULL, NULL, &error);
  if (!values) {
    channel_c_free(error);
    return;
  }

  while (!done) {
    if (channel_c_values_next(values, &body, &done, &error) != 0) {
      channel_c_free(error);
      break;
    }
    if (!done && body) {
      (void)channel_c_stream_emit_json(request->emitter, body);
      channel_c_free(body);
      body = NULL;
    }
  }

  channel_c_values_close(values);
}

static void server_mirror_stream_cancel(const channel_c_stream_request_t *request) {
  channel_c_values_t *values;
  char *error = NULL;
  char *body = NULL;
  int done = 0;

  values = channel_c_sender_values(request->sender, "stream/cancel", NULL, NULL, &error);
  if (!values) {
    channel_c_free(error);
    return;
  }

  while (!done) {
    if (channel_c_values_next(values, &body, &done, &error) != 0) {
      channel_c_free(error);
      break;
    }
    if (!done && body) {
      if (channel_c_stream_emit_json(request->emitter, body) != 0) {
        channel_c_free(body);
        body = NULL;
        break;
      }
      channel_c_free(body);
      body = NULL;
    }
  }

  channel_c_values_close(values);
}

static int client_hello(const channel_c_post_request_t *request, char **out_body_json, char **out_error) {
  (void)request;
  (void)out_error;
  *out_body_json = channel_c_strdup("\"client world\"");
  return *out_body_json ? 0 : -1;
}

static void client_stream_values(const channel_c_stream_request_t *request) {
  int i;
  for (i = 0; i < 3; i += 1) {
    char value[16];
    if (channel_c_stream_is_cancelled(request->emitter)) return;
    snprintf(value, sizeof(value), "%d", i);
    if (channel_c_stream_emit_json(request->emitter, value) != 0) return;
    usleep(1000);
  }
}

static void client_stream_cancel(const channel_c_stream_request_t *request) {
  int i;
  for (i = 0; i < 10; i += 1) {
    char value[16];
    if (channel_c_stream_is_cancelled(request->emitter)) return;
    snprintf(value, sizeof(value), "%d", i);
    if (channel_c_stream_emit_json(request->emitter, value) != 0) return;
    g_client_values_sent += 1;
    usleep(1000);
  }
}

static void on_client_hello_event(const char *body_json, void *user_data) {
  int *count = (int *)user_data;
  if (body_json && strstr(body_json, "event")) {
    *count += 1;
  }
}

static int wait_remote_topic(channel_c_client_t *client, const char *topic, int expected, int timeout_ms) {
  int elapsed = 0;
  while (elapsed < timeout_ms) {
    if (channel_c_client_remote_is_subscribed(client, topic) == expected) return 1;
    usleep(1000);
    elapsed += 1;
  }
  return 0;
}

static int wait_client_disconnected(channel_c_client_t *client, int timeout_ms) {
  int elapsed = 0;
  while (elapsed < timeout_ms) {
    if (channel_c_client_is_connected(client) == 0) return 1;
    usleep(1000);
    elapsed += 1;
  }
  return 0;
}

static int wait_server_connection_count(channel_c_server_t *server, int expected, int timeout_ms) {
  int elapsed = 0;
  while (elapsed < timeout_ms) {
    if (channel_c_server_connection_count(server) == expected) return 1;
    usleep(1000);
    elapsed += 1;
  }
  return 0;
}

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

static void test_client_against_c_server(void **state) {
  channel_c_channel_t *server_channel;
  channel_c_channel_t *client_channel;
  channel_c_server_t *server;
  channel_c_client_t *client;
  channel_c_client_t *client2;
  channel_c_sender_t *sender;
  channel_c_sender_t *sender2;
  channel_c_values_t *values;
  channel_c_subscription_handle_t *sub = NULL;

  char *body = NULL;
  char *error = NULL;
  int done = 0;
  int index = 0;
  int event_count = 0;
  int port;
  char url[64];

  (void)state;

  port = find_free_port();
  assert_true(port > 0);
  snprintf(url, sizeof(url), "ws://127.0.0.1:%d", port);

  g_client_values_sent = 0;
  g_server_stream_cancel_sent = 0;
  g_server_mirror_events = 0;
  if (g_server_mirror_handle) {
    channel_c_subscription_cancel(g_server_mirror_handle);
    g_server_mirror_handle = NULL;
  }

  server_channel = channel_c_channel_new();
  assert_non_null(server_channel);
  assert_int_equal(channel_c_channel_post(server_channel, "hello", server_hello, NULL), 0);
  assert_int_equal(channel_c_channel_post(server_channel, "mirror", server_mirror, NULL), 0);
  assert_int_equal(channel_c_channel_post(server_channel, "echo", server_echo, NULL), 0);
  assert_int_equal(channel_c_channel_post(server_channel, "empty", server_empty, NULL), 0);
  assert_int_equal(channel_c_channel_post(server_channel, "disconnect", server_disconnect, NULL), 0);
  assert_int_equal(channel_c_channel_post(server_channel, "auth", server_auth, NULL), 0);
  assert_int_equal(channel_c_channel_post(server_channel, "auth/name", server_auth_name, NULL), 0);
  assert_int_equal(channel_c_channel_post(server_channel, "mirror/events", server_mirror_events, NULL), 0);

  assert_int_equal(channel_c_channel_stream(server_channel, "stream/values", server_stream_values, NULL), 0);
  assert_int_equal(channel_c_channel_stream(server_channel, "stream/cancel", server_stream_cancel, NULL), 0);
  assert_int_equal(channel_c_channel_stream(server_channel, "mirror/stream", server_mirror_stream, NULL), 0);
  assert_int_equal(channel_c_channel_stream(server_channel, "mirror/stream/cancel", server_mirror_stream_cancel, NULL), 0);

  assert_int_equal(channel_c_channel_subscription(server_channel, "hello", NULL, NULL), 0);

  {
    channel_c_state_factory_t factory;
    factory.create = make_auth_state;
    factory.destroy = free_auth_state;
    factory.user_data = NULL;
    server = channel_c_server_listen(server_channel, "127.0.0.1", port, factory, &error);
  }

  assert_non_null(server);
  assert_null(error);
  assert_int_equal(channel_c_server_connection_count(NULL), 0);
  assert_int_equal(channel_c_server_connection_count(server), 0);

  client_channel = channel_c_channel_new();
  assert_non_null(client_channel);
  assert_int_equal(channel_c_channel_post(client_channel, "hello", client_hello, NULL), 0);
  assert_int_equal(channel_c_channel_stream(client_channel, "stream/values", client_stream_values, NULL), 0);
  assert_int_equal(channel_c_channel_stream(client_channel, "stream/cancel", client_stream_cancel, NULL), 0);
  assert_int_equal(channel_c_channel_subscription(client_channel, "names", NULL, NULL), 0);

  client = channel_c_client_connect(client_channel, url, NULL, NULL, &error);
  assert_non_null(client);
  assert_null(error);
  assert_true(wait_server_connection_count(server, 1, 2000));
  sender = channel_c_client_sender(client);
  assert_non_null(sender);

  assert_int_equal(channel_c_sender_send(sender, "hello", NULL, NULL, &body, &error), 0);
  assert_null(error);
  assert_string_equal(body, "\"world\"");
  channel_c_free(body);
  body = NULL;

  assert_int_equal(channel_c_sender_send(sender, "mirror", NULL, NULL, &body, &error), 0);
  assert_null(error);
  assert_string_equal(body, "\"client world\"");
  channel_c_free(body);
  body = NULL;

  assert_int_equal(channel_c_sender_send(sender, "echo", "123", NULL, &body, &error), 0);
  assert_null(error);
  assert_string_equal(body, "123");
  channel_c_free(body);
  body = NULL;

  values = channel_c_sender_values(sender, "stream/values", NULL, NULL, &error);
  assert_non_null(values);
  assert_null(error);

  done = 0;
  index = 0;
  while (!done) {
    assert_int_equal(channel_c_values_next(values, &body, &done, &error), 0);
    assert_null(error);
    if (!done) {
      char expected[16];
      snprintf(expected, sizeof(expected), "%d", index++);
      assert_string_equal(body, expected);
      channel_c_free(body);
      body = NULL;
    }
  }
  assert_int_equal(index, 3);
  channel_c_values_close(values);

  values = channel_c_sender_values(sender, "stream/cancel", NULL, NULL, &error);
  assert_non_null(values);
  assert_null(error);

  done = 0;
  while (!done) {
    assert_int_equal(channel_c_values_next(values, &body, &done, &error), 0);
    assert_null(error);
    if (done) break;
    if (body && strcmp(body, "2") == 0) {
      channel_c_free(body);
      body = NULL;
      break;
    }
    channel_c_free(body);
    body = NULL;
  }
  channel_c_values_close(values);
  usleep(10000);
  assert_true(g_server_stream_cancel_sent <= 32);

  assert_int_equal(
    channel_c_sender_subscribe(sender, "hello", "\"test\"", NULL, on_client_hello_event, &event_count, &sub, &error),
    0
  );
  assert_null(error);
  assert_non_null(sub);

  assert_int_equal(channel_c_channel_event_send(server_channel, "hello", "\"test\"", "\"event 1\"", &error), 0);
  assert_null(error);
  usleep(5000);
  assert_int_equal(event_count, 1);

  channel_c_subscription_cancel(sub);
  sub = NULL;

  assert_int_equal(channel_c_channel_event_send(server_channel, "hello", "\"test\"", "\"event 2\"", &error), 0);
  assert_null(error);
  usleep(5000);
  assert_int_equal(event_count, 1);

  assert_int_equal(channel_c_sender_send(sender, "mirror/events", NULL, NULL, &body, &error), 0);
  assert_null(error);
  assert_null(body);
  assert_true(wait_remote_topic(client, "names/1", 1, 2000));

  assert_int_equal(channel_c_channel_event_send(client_channel, "names", "\"1\"", "\"h\"", &error), 0);
  assert_null(error);
  assert_true(wait_remote_topic(client, "names/1", 0, 2000));
  assert_int_equal(g_server_mirror_events, 1);

  values = channel_c_sender_values(sender, "mirror/stream/cancel", NULL, NULL, &error);
  assert_non_null(values);
  assert_null(error);

  done = 0;
  while (!done) {
    assert_int_equal(channel_c_values_next(values, &body, &done, &error), 0);
    assert_null(error);
    if (done) break;
    if (body && strcmp(body, "2") == 0) {
      channel_c_free(body);
      body = NULL;
      break;
    }
    channel_c_free(body);
    body = NULL;
  }
  channel_c_values_close(values);
  usleep(10000);
  assert_true(g_client_values_sent <= 4);

  assert_int_equal(channel_c_sender_send(sender, "auth", "{\"name\":\"tester\"}", NULL, &body, &error), 0);
  assert_null(error);
  assert_string_equal(body, "\"tester\"");
  channel_c_free(body);
  body = NULL;

  assert_int_equal(channel_c_sender_send(sender, "auth/name", NULL, NULL, &body, &error), 0);
  assert_null(error);
  assert_string_equal(body, "\"tester\"");
  channel_c_free(body);
  body = NULL;

  client2 = channel_c_client_connect(client_channel, url, NULL, NULL, &error);
  assert_non_null(client2);
  assert_null(error);
  assert_true(wait_server_connection_count(server, 2, 2000));
  sender2 = channel_c_client_sender(client2);

  assert_int_not_equal(channel_c_sender_send(sender2, "auth/name", NULL, NULL, &body, &error), 0);
  assert_non_null(error);
  assert_string_equal(error, "unauthorized");
  channel_c_free(error);
  error = NULL;

  channel_c_client_free(client2);
  assert_true(wait_server_connection_count(server, 1, 2000));

  assert_int_equal(channel_c_sender_send(sender, "disconnect", NULL, NULL, &body, &error), 0);
  assert_null(error);
  assert_null(body);
  assert_true(wait_client_disconnected(client, 2000));

  channel_c_client_free(client);
  assert_true(wait_server_connection_count(server, 0, 2000));
  channel_c_channel_free(client_channel);

  channel_c_server_free(server);
  channel_c_channel_free(server_channel);
}

int main(void) {
  const struct CMUnitTest tests[] = {
    cmocka_unit_test(test_client_against_c_server),
  };
  return cmocka_run_group_tests(tests, NULL, NULL);
}
