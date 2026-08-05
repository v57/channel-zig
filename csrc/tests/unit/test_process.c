#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>

#include <cmocka.h>

#include "channel-c/channel_c.h"

static int g_values_sent = 0;

static int post_hello(const channel_c_post_request_t *request, char **out_body_json, char **out_error) {
  (void)request;
  (void)out_error;
  *out_body_json = channel_c_strdup("\"world\"");
  return *out_body_json ? 0 : -1;
}

static int post_echo(const channel_c_post_request_t *request, char **out_body_json, char **out_error) {
  (void)out_error;
  if (!request->body_json) {
    *out_body_json = channel_c_strdup("null");
  } else {
    *out_body_json = channel_c_strdup(request->body_json);
  }
  return *out_body_json ? 0 : -1;
}

static int post_error(const channel_c_post_request_t *request, char **out_body_json, char **out_error) {
  (void)request;
  (void)out_body_json;
  *out_error = channel_c_strdup("failed");
  return -1;
}

static void stream_values(const channel_c_stream_request_t *request) {
  int i;
  (void)request;
  for (i = 0; i < 3; i += 1) {
    char body[16];
    snprintf(body, sizeof(body), "%d", i);
    if (channel_c_stream_emit_json(request->emitter, body) != 0) return;
    usleep(1000);
  }
}

static void stream_cancel(const channel_c_stream_request_t *request) {
  int i;
  for (i = 0; i < 10; i += 1) {
    char body[16];
    if (channel_c_stream_is_cancelled(request->emitter)) return;
    snprintf(body, sizeof(body), "%d", i);
    if (channel_c_stream_emit_json(request->emitter, body) != 0) return;
    g_values_sent += 1;
    usleep(1000);
  }
}

static void on_event(const char *body_json, void *user_data) {
  int *count = (int *)user_data;
  if (body_json && strstr(body_json, "event")) {
    *count += 1;
  }
}

static void test_process_send_and_stream(void **state) {
  channel_c_channel_t *channel;
  channel_c_process_t *process;
  channel_c_sender_t *sender;
  char *body = NULL;
  char *error = NULL;
  channel_c_values_t *values;
  int done = 0;
  int expected = 0;

  (void)state;

  channel = channel_c_channel_new();
  assert_non_null(channel);
  assert_int_equal(channel_c_channel_post(channel, "hello", post_hello, NULL), 0);
  assert_int_equal(channel_c_channel_post(channel, "echo", post_echo, NULL), 0);
  assert_int_equal(channel_c_channel_post(channel, "fail", post_error, NULL), 0);
  assert_int_equal(channel_c_channel_stream(channel, "stream/values", stream_values, NULL), 0);

  process = channel_c_process_new(channel, NULL, NULL);
  assert_non_null(process);

  sender = channel_c_process_sender(process);
  assert_non_null(sender);

  assert_int_equal(channel_c_sender_send(sender, "hello", NULL, NULL, &body, &error), 0);
  assert_null(error);
  assert_string_equal(body, "\"world\"");
  channel_c_free(body);
  body = NULL;

  assert_int_equal(channel_c_sender_send(sender, "echo", "42", NULL, &body, &error), 0);
  assert_null(error);
  assert_string_equal(body, "42");
  channel_c_free(body);
  body = NULL;

  assert_int_not_equal(channel_c_sender_send(sender, "fail", NULL, NULL, &body, &error), 0);
  assert_non_null(error);
  assert_string_equal(error, "failed");
  channel_c_free(error);
  error = NULL;

  values = channel_c_sender_values(sender, "stream/values", NULL, NULL, &error);
  assert_non_null(values);
  assert_null(error);

  while (!done) {
    assert_int_equal(channel_c_values_next(values, &body, &done, &error), 0);
    assert_null(error);
    if (!done) {
      char expected_text[16];
      snprintf(expected_text, sizeof(expected_text), "%d", expected);
      assert_non_null(body);
      assert_string_equal(body, expected_text);
      channel_c_free(body);
      body = NULL;
      expected += 1;
    }
  }
  assert_int_equal(expected, 3);
  channel_c_values_close(values);

  channel_c_process_free(process);
  channel_c_channel_free(channel);
}

static void test_process_subscribe_and_cancel(void **state) {
  channel_c_channel_t *channel;
  channel_c_process_t *process;
  channel_c_sender_t *sender;
  channel_c_subscription_handle_t *handle = NULL;
  char *error = NULL;
  int count = 0;

  (void)state;

  channel = channel_c_channel_new();
  assert_non_null(channel);
  assert_int_equal(channel_c_channel_subscription(channel, "hello", NULL, NULL), 0);

  process = channel_c_process_new(channel, NULL, NULL);
  assert_non_null(process);
  sender = channel_c_process_sender(process);

  assert_int_equal(
    channel_c_sender_subscribe(sender, "hello", "\"test\"", NULL, on_event, &count, &handle, &error),
    0
  );
  assert_null(error);
  assert_non_null(handle);

  assert_int_equal(channel_c_channel_event_send(channel, "hello", "\"test\"", "\"event 1\"", &error), 0);
  assert_null(error);
  usleep(2000);
  assert_int_equal(count, 1);

  channel_c_subscription_cancel(handle);
  handle = NULL;

  assert_int_equal(channel_c_channel_event_send(channel, "hello", "\"test\"", "\"event 2\"", &error), 0);
  assert_null(error);
  usleep(2000);
  assert_int_equal(count, 1);

  channel_c_process_free(process);
  channel_c_channel_free(channel);
}

static void test_process_stream_cancel(void **state) {
  channel_c_channel_t *channel;
  channel_c_process_t *process;
  channel_c_sender_t *sender;
  channel_c_values_t *values;
  char *error = NULL;
  char *body = NULL;
  int done = 0;

  (void)state;

  g_values_sent = 0;

  channel = channel_c_channel_new();
  assert_non_null(channel);
  assert_int_equal(channel_c_channel_stream(channel, "stream/cancel", stream_cancel, NULL), 0);

  process = channel_c_process_new(channel, NULL, NULL);
  assert_non_null(process);
  sender = channel_c_process_sender(process);

  values = channel_c_sender_values(sender, "stream/cancel", NULL, NULL, &error);
  assert_non_null(values);
  assert_null(error);

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
  assert_true(g_values_sent <= 6);

  channel_c_process_free(process);
  channel_c_channel_free(channel);
}

int main(void) {
  const struct CMUnitTest tests[] = {
    cmocka_unit_test(test_process_send_and_stream),
    cmocka_unit_test(test_process_subscribe_and_cancel),
    cmocka_unit_test(test_process_stream_cancel),
  };

  return cmocka_run_group_tests(tests, NULL, NULL);
}
