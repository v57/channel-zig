#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <pthread.h>

#include <cmocka.h>
#include <yyjson.h>

#include "channel-c/channel_c.h"
#include "../../src/core/internal.h"

typedef struct {
  pthread_mutex_t lock;
  char *last_message;
  int message_count;
  int stop_count;
  int fail_send;
} mock_transport_t;

typedef struct {
  channel_c_sender_t *sender;
  int rc;
  char *body;
  char *error;
} send_thread_args_t;

typedef struct {
  channel_c_sender_t *sender;
  int rc;
  channel_c_subscription_handle_t *handle;
  char *error;
} subscribe_thread_args_t;

typedef struct {
  channel_c_connection_t *connection;
} sender_private_t;

static void noop_event(const char *body_json, void *user_data);

static int mock_send_text(void *ctx, const char *text) {
  mock_transport_t *mock = (mock_transport_t *)ctx;
  if (!mock || !text) return -1;

  pthread_mutex_lock(&mock->lock);
  if (mock->fail_send) {
    pthread_mutex_unlock(&mock->lock);
    return -1;
  }
  free(mock->last_message);
  mock->last_message = channel_c_strdup(text);
  mock->message_count += 1;
  pthread_mutex_unlock(&mock->lock);
  return 0;
}

static void mock_stop(void *ctx) {
  mock_transport_t *mock = (mock_transport_t *)ctx;
  if (!mock) return;
  pthread_mutex_lock(&mock->lock);
  mock->stop_count += 1;
  pthread_mutex_unlock(&mock->lock);
}

static void *sender_thread_main(void *argument) {
  send_thread_args_t *args = (send_thread_args_t *)argument;
  args->rc = channel_c_sender_send(args->sender, "wait", NULL, NULL, &args->body, &args->error);
  return NULL;
}

static void *subscribe_thread_main(void *argument) {
  subscribe_thread_args_t *args = (subscribe_thread_args_t *)argument;
  args->rc = channel_c_sender_subscribe(
    args->sender,
    "hello",
    "\"x\"",
    NULL,
    noop_event,
    NULL,
    &args->handle,
    &args->error
  );
  return NULL;
}

static int post_hello(const channel_c_post_request_t *request, char **out_body_json, char **out_error) {
  (void)request;
  (void)out_error;
  *out_body_json = channel_c_strdup("\"world\"");
  return *out_body_json ? 0 : -1;
}

static int post_other(const channel_c_post_request_t *request, char **out_body_json, char **out_error) {
  (void)request;
  (void)out_error;
  *out_body_json = channel_c_strdup("\"ok\"");
  return *out_body_json ? 0 : -1;
}

static void stream_noop(const channel_c_stream_request_t *request) {
  (void)request;
}

static void noop_event(const char *body_json, void *user_data) {
  (void)body_json;
  (void)user_data;
}

static int post_mark_call(const channel_c_post_request_t *request, char **out_body_json, char **out_error) {
  int *counter = (int *)request->user_data;
  (void)out_error;
  if (counter) {
    __sync_fetch_and_add(counter, 1);
  }
  *out_body_json = channel_c_strdup("\"ok\"");
  return *out_body_json ? 0 : -1;
}

static void stream_mark_call(const channel_c_stream_request_t *request) {
  int *counter = (int *)request->user_data;
  if (counter) {
    __sync_fetch_and_add(counter, 1);
  }
}

static int sub_fail_no_error(
  const channel_c_subscription_request_t *request,
  char **out_topic,
  char **out_body_json,
  char **out_error
) {
  (void)request;
  (void)out_topic;
  (void)out_body_json;
  (void)out_error;
  return -1;
}

static int sub_resolve_topic_and_body(
  const channel_c_subscription_request_t *request,
  char **out_topic,
  char **out_body_json,
  char **out_error
) {
  (void)out_error;
  if (request && request->request_body_json && strcmp(request->request_body_json, "\"fail\"") == 0) {
    return -1;
  }
  *out_topic = channel_c_strdup("topic-1");
  *out_body_json = channel_c_strdup("\"event\"");
  if (!*out_topic || !*out_body_json) {
    channel_c_free(*out_topic);
    channel_c_free(*out_body_json);
    *out_topic = NULL;
    *out_body_json = NULL;
    return -1;
  }
  return 0;
}

static void destroy_state_counter(void *state, void *user_data) {
  int *count = (int *)user_data;
  channel_c_free(state);
  if (count) __sync_fetch_and_add(count, 1);
}

static int wait_for_message_count(mock_transport_t *transport, int expected_count, int timeout_ms) {
  int waited = 0;
  while (waited < timeout_ms) {
    int count;
    pthread_mutex_lock(&transport->lock);
    count = transport->message_count;
    pthread_mutex_unlock(&transport->lock);
    if (count >= expected_count) return 1;
    usleep(1000);
    waited += 1;
  }
  return 0;
}

static char *copy_last_message(mock_transport_t *transport) {
  char *copy = NULL;
  pthread_mutex_lock(&transport->lock);
  if (transport->last_message) {
    copy = channel_c_strdup(transport->last_message);
  }
  pthread_mutex_unlock(&transport->lock);
  return copy;
}

static int message_id_from_json(const char *json_text, int *out_id) {
  yyjson_doc *doc;
  yyjson_val *root;
  yyjson_val *id;

  if (!json_text || !out_id) return -1;

  doc = yyjson_read(json_text, strlen(json_text), 0);
  if (!doc) return -1;

  root = yyjson_doc_get_root(doc);
  if (!yyjson_is_obj(root)) {
    yyjson_doc_free(doc);
    return -1;
  }

  id = yyjson_obj_get(root, "id");
  if (!id || (!yyjson_is_int(id) && !yyjson_is_uint(id))) {
    yyjson_doc_free(doc);
    return -1;
  }

  *out_id = (int)yyjson_get_sint(id);
  yyjson_doc_free(doc);
  return 0;
}

static void test_internal_connection_failure_wakes_pending(void **state) {
  channel_c_channel_t *channel;
  channel_c_connection_t *connection;
  channel_c_transport_ops_t ops;
  mock_transport_t transport = {0};
  pthread_t thread;
  send_thread_args_t args = {0};
  int waited = 0;

  (void)state;

  assert_int_equal(pthread_mutex_init(&transport.lock, NULL), 0);

  channel = channel_c_channel_new();
  assert_non_null(channel);

  ops.send_text = mock_send_text;
  ops.stop = mock_stop;
  connection = channel_c_connection_create(channel, NULL, NULL, NULL, &transport, ops);
  assert_non_null(connection);

  assert_int_equal(channel_c_connection_is_connected(connection), 0);
  channel_c_internal_connection_mark_connected(connection, 1);
  assert_int_equal(channel_c_connection_is_connected(connection), 1);

  channel_c_connection_set_transport(connection, &transport, ops);

  args.sender = channel_c_connection_sender(connection);
  assert_non_null(args.sender);
  assert_int_equal(pthread_create(&thread, NULL, sender_thread_main, &args), 0);

  while (waited < 2000 && !wait_for_message_count(&transport, 1, 1)) {
    waited += 1;
  }
  assert_true(waited < 2000);

  channel_c_internal_connection_mark_failed(connection, "boom");
  pthread_join(thread, NULL);

  assert_int_not_equal(args.rc, 0);
  assert_non_null(args.error);
  assert_string_equal(args.error, "boom");

  channel_c_free(args.body);
  channel_c_free(args.error);

  channel_c_connection_on_close(connection);
  channel_c_connection_release(connection);
  channel_c_channel_free(channel);

  free(transport.last_message);
  pthread_mutex_destroy(&transport.lock);
}

static void test_channel_merge_prefix_and_empty_prefix(void **state) {
  channel_c_channel_t *source;
  channel_c_channel_t *destination;
  channel_c_process_t *process;
  channel_c_sender_t *sender;
  char *body = NULL;
  char *error = NULL;

  (void)state;

  source = channel_c_channel_new();
  destination = channel_c_channel_new();
  assert_non_null(source);
  assert_non_null(destination);

  assert_int_equal(channel_c_channel_post(source, "hello", post_hello, NULL), 0);
  assert_int_equal(channel_c_channel_stream(source, "stream", stream_noop, NULL), 0);
  assert_int_equal(channel_c_channel_subscription(source, "events", NULL, NULL), 0);

  assert_int_equal(channel_c_channel_merge(destination, source, "merged"), 0);
  assert_int_equal(channel_c_channel_merge(destination, source, ""), 0);

  assert_int_equal(channel_c_channel_post(destination, "wait", post_other, NULL), 0);

  process = channel_c_process_new(destination, NULL, NULL);
  assert_non_null(process);
  sender = channel_c_process_sender(process);
  assert_non_null(sender);

  assert_int_equal(channel_c_sender_send(sender, "merged/hello", NULL, NULL, &body, &error), 0);
  assert_null(error);
  assert_string_equal(body, "\"world\"");
  channel_c_free(body);
  body = NULL;

  assert_int_equal(channel_c_sender_send(sender, "hello", NULL, NULL, &body, &error), 0);
  assert_null(error);
  assert_string_equal(body, "\"world\"");
  channel_c_free(body);

  channel_c_process_free(process);
  channel_c_channel_free(destination);
  channel_c_channel_free(source);
}

static void test_cleanup_with_unreleased_subscriptions(void **state) {
  channel_c_channel_t *channel;
  channel_c_process_t *process;
  channel_c_sender_t *sender;
  channel_c_connection_t *connection;
  channel_c_subscription_handle_t *a = NULL;
  channel_c_subscription_handle_t *b = NULL;
  char *error = NULL;

  (void)state;

  channel = channel_c_channel_new();
  assert_non_null(channel);
  assert_int_equal(channel_c_channel_subscription(channel, "hello", NULL, NULL), 0);

  process = channel_c_process_new(channel, NULL, NULL);
  assert_non_null(process);
  sender = channel_c_process_sender(process);
  assert_non_null(sender);

  assert_int_equal(
    channel_c_sender_subscribe(sender, "hello", "\"x\"", NULL, noop_event, NULL, &a, &error),
    0
  );
  assert_null(error);
  assert_non_null(a);

  assert_int_equal(
    channel_c_sender_subscribe(sender, "hello", "\"x\"", NULL, noop_event, NULL, &b, &error),
    0
  );
  assert_null(error);
  assert_non_null(b);

  connection = ((sender_private_t *)sender)->connection;
  assert_non_null(connection);

  /* balance handle ref-count increments to force connection teardown with live callbacks */
  channel_c_connection_release(connection);
  channel_c_connection_release(connection);

  channel_c_process_free(process);
  channel_c_channel_free(channel);

  (void)a;
  (void)b;
}

static void test_receive_unknown_requests_reply_with_errors(void **state) {
  channel_c_channel_t *channel;
  channel_c_connection_t *connection;
  channel_c_transport_ops_t ops;
  mock_transport_t transport = {0};
  char *message;

  (void)state;

  assert_int_equal(pthread_mutex_init(&transport.lock, NULL), 0);
  channel = channel_c_channel_new();
  assert_non_null(channel);

  ops.send_text = mock_send_text;
  ops.stop = mock_stop;
  connection = channel_c_connection_create(channel, NULL, NULL, NULL, &transport, ops);
  assert_non_null(connection);

  channel_c_connection_receive_text(connection, "{\"id\":11,\"path\":\"missing\"}");
  assert_true(wait_for_message_count(&transport, 1, 1000));
  message = copy_last_message(&transport);
  assert_non_null(message);
  assert_non_null(strstr(message, "\"id\":11"));
  assert_non_null(strstr(message, "api not found"));
  channel_c_free(message);

  channel_c_connection_receive_text(connection, "{\"id\":12,\"stream\":\"missing\"}");
  assert_true(wait_for_message_count(&transport, 2, 1000));
  message = copy_last_message(&transport);
  assert_non_null(message);
  assert_non_null(strstr(message, "\"id\":12"));
  assert_non_null(strstr(message, "api not found"));
  channel_c_free(message);

  channel_c_connection_receive_text(connection, "{\"id\":13,\"sub\":\"missing\"}");
  assert_true(wait_for_message_count(&transport, 3, 1000));
  message = copy_last_message(&transport);
  assert_non_null(message);
  assert_non_null(strstr(message, "\"id\":13"));
  assert_non_null(strstr(message, "subscription not found"));
  channel_c_free(message);

  channel_c_connection_on_close(connection);
  channel_c_connection_release(connection);
  channel_c_channel_free(channel);
  free(transport.last_message);
  pthread_mutex_destroy(&transport.lock);
}

static void test_receive_fire_and_forget_post_and_stream_without_id(void **state) {
  channel_c_channel_t *channel;
  channel_c_connection_t *connection;
  channel_c_transport_ops_t ops;
  mock_transport_t transport = {0};
  int post_calls = 0;
  int stream_calls = 0;
  int waited = 0;

  (void)state;

  assert_int_equal(pthread_mutex_init(&transport.lock, NULL), 0);
  channel = channel_c_channel_new();
  assert_non_null(channel);
  assert_int_equal(channel_c_channel_post(channel, "fire", post_mark_call, &post_calls), 0);
  assert_int_equal(channel_c_channel_stream(channel, "live", stream_mark_call, &stream_calls), 0);

  ops.send_text = mock_send_text;
  ops.stop = mock_stop;
  connection = channel_c_connection_create(channel, NULL, NULL, NULL, &transport, ops);
  assert_non_null(connection);

  channel_c_connection_receive_text(connection, "{\"path\":\"fire\",\"body\":1,\"context\":{\"k\":2}}");
  while (waited < 2000 && post_calls == 0) {
    usleep(1000);
    waited += 1;
  }
  assert_true(post_calls > 0);

  channel_c_connection_receive_text(connection, "{\"stream\":\"live\",\"body\":1}");
  usleep(5000);
  assert_int_equal(stream_calls, 0);

  pthread_mutex_lock(&transport.lock);
  assert_int_equal(transport.message_count, 0);
  pthread_mutex_unlock(&transport.lock);

  channel_c_connection_on_close(connection);
  channel_c_connection_release(connection);
  channel_c_channel_free(channel);
  free(transport.last_message);
  pthread_mutex_destroy(&transport.lock);
}

static void test_sender_send_common_error_paths(void **state) {
  channel_c_channel_t *channel;
  channel_c_connection_t *connection;
  channel_c_sender_t *sender;
  channel_c_transport_ops_t ops;
  mock_transport_t transport = {0};
  char *body = NULL;
  char *error = NULL;

  (void)state;

  assert_int_equal(pthread_mutex_init(&transport.lock, NULL), 0);
  channel = channel_c_channel_new();
  assert_non_null(channel);

  ops.send_text = mock_send_text;
  ops.stop = mock_stop;
  connection = channel_c_connection_create(channel, NULL, NULL, NULL, &transport, ops);
  assert_non_null(connection);
  channel_c_internal_connection_mark_connected(connection, 1);

  sender = channel_c_connection_sender(connection);
  assert_non_null(sender);

  assert_int_not_equal(channel_c_sender_send(sender, NULL, NULL, NULL, &body, &error), 0);
  assert_non_null(error);
  assert_string_equal(error, "invalid arguments");
  channel_c_free(error);
  error = NULL;

  transport.fail_send = 1;
  assert_int_not_equal(channel_c_sender_send(sender, "hello", NULL, NULL, &body, &error), 0);
  assert_non_null(error);
  assert_string_equal(error, "failed to send request");
  channel_c_free(error);
  error = NULL;

  channel_c_connection_on_close(connection);
  channel_c_connection_release(connection);
  channel_c_channel_free(channel);
  free(transport.last_message);
  pthread_mutex_destroy(&transport.lock);
}

static void test_values_next_error_and_done_messages(void **state) {
  channel_c_channel_t *channel;
  channel_c_connection_t *connection;
  channel_c_sender_t *sender;
  channel_c_values_t *values;
  channel_c_transport_ops_t ops;
  mock_transport_t transport = {0};
  char *body = NULL;
  char *error = NULL;
  char *message = NULL;
  int request_id = -1;
  int done = 0;
  char response[128];

  (void)state;

  assert_int_equal(pthread_mutex_init(&transport.lock, NULL), 0);
  channel = channel_c_channel_new();
  assert_non_null(channel);

  ops.send_text = mock_send_text;
  ops.stop = mock_stop;
  connection = channel_c_connection_create(channel, NULL, NULL, NULL, &transport, ops);
  assert_non_null(connection);
  channel_c_internal_connection_mark_connected(connection, 1);

  sender = channel_c_connection_sender(connection);
  assert_non_null(sender);

  values = channel_c_sender_values(sender, "stream/one", NULL, NULL, &error);
  assert_non_null(values);
  assert_null(error);
  assert_true(wait_for_message_count(&transport, 1, 1000));
  message = copy_last_message(&transport);
  assert_non_null(message);
  assert_int_equal(message_id_from_json(message, &request_id), 0);
  channel_c_free(message);

  snprintf(response, sizeof(response), "{\"id\":%d,\"error\":\"boom\"}", request_id);
  channel_c_connection_receive_text(connection, response);

  assert_int_not_equal(channel_c_values_next(values, &body, &done, &error), 0);
  assert_non_null(error);
  assert_string_equal(error, "boom");
  channel_c_free(error);
  error = NULL;
  channel_c_values_close(values);

  values = channel_c_sender_values(sender, "stream/two", NULL, NULL, &error);
  assert_non_null(values);
  assert_null(error);
  assert_true(wait_for_message_count(&transport, 2, 1000));
  message = copy_last_message(&transport);
  assert_non_null(message);
  assert_int_equal(message_id_from_json(message, &request_id), 0);
  channel_c_free(message);

  snprintf(response, sizeof(response), "{\"id\":%d,\"done\":true}", request_id);
  channel_c_connection_receive_text(connection, response);

  assert_int_equal(channel_c_values_next(values, &body, &done, &error), 0);
  assert_null(error);
  assert_int_equal(done, 1);
  assert_null(body);
  assert_int_equal(channel_c_values_next(values, &body, &done, &error), 0);
  assert_int_equal(done, 1);
  channel_c_values_close(values);

  channel_c_connection_on_close(connection);
  channel_c_connection_release(connection);
  channel_c_channel_free(channel);
  free(transport.last_message);
  pthread_mutex_destroy(&transport.lock);
}

static void test_subscription_resolver_default_error_message(void **state) {
  channel_c_channel_t *channel;
  channel_c_connection_t *connection;
  channel_c_transport_ops_t ops;
  mock_transport_t transport = {0};
  char *message = NULL;

  (void)state;

  assert_int_equal(pthread_mutex_init(&transport.lock, NULL), 0);
  channel = channel_c_channel_new();
  assert_non_null(channel);
  assert_int_equal(channel_c_channel_subscription(channel, "names", sub_fail_no_error, NULL), 0);

  ops.send_text = mock_send_text;
  ops.stop = mock_stop;
  connection = channel_c_connection_create(channel, NULL, NULL, NULL, &transport, ops);
  assert_non_null(connection);

  channel_c_connection_receive_text(connection, "{\"id\":22,\"sub\":\"names\",\"body\":\"1\"}");
  assert_true(wait_for_message_count(&transport, 1, 1000));
  message = copy_last_message(&transport);
  assert_non_null(message);
  assert_non_null(strstr(message, "\"id\":22"));
  assert_non_null(strstr(message, "subscription failed"));
  channel_c_free(message);

  channel_c_connection_on_close(connection);
  channel_c_connection_release(connection);
  channel_c_channel_free(channel);
  free(transport.last_message);
  pthread_mutex_destroy(&transport.lock);
}

static void test_receive_invalid_json_closes_connection(void **state) {
  channel_c_channel_t *channel;
  channel_c_connection_t *connection;
  channel_c_transport_ops_t ops;
  mock_transport_t transport = {0};

  (void)state;

  assert_int_equal(pthread_mutex_init(&transport.lock, NULL), 0);
  channel = channel_c_channel_new();
  assert_non_null(channel);

  ops.send_text = mock_send_text;
  ops.stop = mock_stop;
  connection = channel_c_connection_create(channel, NULL, NULL, NULL, &transport, ops);
  assert_non_null(connection);
  channel_c_internal_connection_mark_connected(connection, 1);
  assert_int_equal(channel_c_connection_is_connected(connection), 1);

  channel_c_connection_receive_text(connection, "{invalid-json");
  assert_int_equal(channel_c_connection_is_connected(connection), 0);

  channel_c_connection_release(connection);
  channel_c_channel_free(channel);
  free(transport.last_message);
  pthread_mutex_destroy(&transport.lock);
}

static void test_channel_event_send_resolver_and_body_errors(void **state) {
  channel_c_channel_t *channel;
  char *error = NULL;

  (void)state;

  channel = channel_c_channel_new();
  assert_non_null(channel);
  assert_int_equal(channel_c_channel_subscription(channel, "plain", NULL, NULL), 0);
  assert_int_equal(channel_c_channel_subscription(channel, "resolved", sub_resolve_topic_and_body, NULL), 0);

  assert_int_not_equal(channel_c_channel_event_send(channel, "plain", "\"a\"", NULL, &error), 0);
  assert_non_null(error);
  assert_string_equal(error, "body is required");
  channel_c_free(error);
  error = NULL;

  assert_int_equal(channel_c_channel_event_send(channel, "resolved", "\"ok\"", NULL, &error), 0);
  assert_null(error);

  assert_int_not_equal(channel_c_channel_event_send(channel, "resolved", "\"fail\"", NULL, &error), 0);
  assert_null(error);

  channel_c_channel_free(channel);
}

static void test_channel_free_releases_attached_connection(void **state) {
  channel_c_channel_t *channel;
  channel_c_connection_t *connection;
  channel_c_transport_ops_t ops;
  mock_transport_t transport = {0};
  int destroyed = 0;
  char *state_value;

  (void)state;

  assert_int_equal(pthread_mutex_init(&transport.lock, NULL), 0);
  channel = channel_c_channel_new();
  assert_non_null(channel);

  state_value = channel_c_strdup("state");
  assert_non_null(state_value);

  ops.send_text = mock_send_text;
  ops.stop = mock_stop;
  connection = channel_c_connection_create(
    channel,
    state_value,
    destroy_state_counter,
    &destroyed,
    &transport,
    ops
  );
  assert_non_null(connection);

  channel_c_channel_free(channel);
  assert_int_equal(destroyed, 1);

  free(transport.last_message);
  pthread_mutex_destroy(&transport.lock);
}

static void test_sender_subscribe_wait_aborts_on_close(void **state) {
  channel_c_channel_t *channel;
  channel_c_connection_t *connection;
  channel_c_transport_ops_t ops;
  mock_transport_t transport = {0};
  subscribe_thread_args_t args = {0};
  pthread_t thread;

  (void)state;

  assert_int_equal(pthread_mutex_init(&transport.lock, NULL), 0);
  channel = channel_c_channel_new();
  assert_non_null(channel);
  assert_int_equal(channel_c_channel_subscription(channel, "hello", NULL, NULL), 0);

  ops.send_text = mock_send_text;
  ops.stop = mock_stop;
  connection = channel_c_connection_create(channel, NULL, NULL, NULL, &transport, ops);
  assert_non_null(connection);
  channel_c_internal_connection_mark_connected(connection, 1);

  args.sender = channel_c_connection_sender(connection);
  assert_non_null(args.sender);
  assert_int_equal(pthread_create(&thread, NULL, subscribe_thread_main, &args), 0);
  assert_true(wait_for_message_count(&transport, 1, 1000));

  channel_c_connection_on_close(connection);
  pthread_join(thread, NULL);

  assert_int_not_equal(args.rc, 0);
  assert_non_null(args.error);
  assert_string_equal(args.error, "connection closed");
  channel_c_free(args.error);

  channel_c_connection_release(connection);
  channel_c_channel_free(channel);
  free(transport.last_message);
  pthread_mutex_destroy(&transport.lock);
}

int main(void) {
  const struct CMUnitTest tests[] = {
    cmocka_unit_test(test_internal_connection_failure_wakes_pending),
    cmocka_unit_test(test_channel_merge_prefix_and_empty_prefix),
    cmocka_unit_test(test_cleanup_with_unreleased_subscriptions),
    cmocka_unit_test(test_receive_unknown_requests_reply_with_errors),
    cmocka_unit_test(test_receive_fire_and_forget_post_and_stream_without_id),
    cmocka_unit_test(test_sender_send_common_error_paths),
    cmocka_unit_test(test_values_next_error_and_done_messages),
    cmocka_unit_test(test_subscription_resolver_default_error_message),
    cmocka_unit_test(test_receive_invalid_json_closes_connection),
    cmocka_unit_test(test_channel_event_send_resolver_and_body_errors),
    cmocka_unit_test(test_channel_free_releases_attached_connection),
    cmocka_unit_test(test_sender_subscribe_wait_aborts_on_close),
  };

  return cmocka_run_group_tests(tests, NULL, NULL);
}
