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

#include "channel-c/channel_c.h"

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

static int wait_server_connection_count(channel_c_server_t *server, int expected, int timeout_ms) {
  int elapsed = 0;
  while (elapsed < timeout_ms) {
    if (channel_c_server_connection_count(server) == expected) return 1;
    usleep(1000);
    elapsed += 1;
  }
  return 0;
}

static void test_client_connect_argument_validation_and_failure(void **state) {
  channel_c_channel_t *channel;
  channel_c_client_t *client;
  char *error = NULL;
  int port;
  char url[64];

  (void)state;

  channel = channel_c_channel_new();
  assert_non_null(channel);

  client = channel_c_client_connect(NULL, "ws://127.0.0.1:12345", NULL, NULL, &error);
  assert_null(client);
  assert_non_null(error);
  assert_string_equal(error, "invalid arguments");
  channel_c_free(error);
  error = NULL;

  client = channel_c_client_connect(channel, NULL, NULL, NULL, &error);
  assert_null(client);
  assert_non_null(error);
  assert_string_equal(error, "invalid arguments");
  channel_c_free(error);
  error = NULL;

  client = channel_c_client_connect(channel, "not-a-websocket-url", NULL, NULL, &error);
  assert_null(client);
  assert_non_null(error);
  assert_true(
    strcmp(error, "invalid websocket url") == 0 ||
    strcmp(error, "failed to connect websocket") == 0
  );
  channel_c_free(error);
  error = NULL;

  port = find_free_port();
  assert_true(port > 0);
  snprintf(url, sizeof(url), "ws://127.0.0.1:%d", port);

  client = channel_c_client_connect(channel, url, NULL, NULL, &error);
  if (client) {
    assert_null(error);
    assert_true(wait_client_disconnected(client, 5000));
    channel_c_client_stop(client);
    channel_c_client_stop(client);
    channel_c_client_free(client);
  } else {
    assert_non_null(error);
    assert_string_equal(error, "failed to connect websocket");
    channel_c_free(error);
    error = NULL;
  }

  channel_c_channel_free(channel);
}

static void test_server_lifecycle_with_connected_client(void **state) {
  channel_c_channel_t *server_channel;
  channel_c_channel_t *client_channel;
  channel_c_server_t *server;
  channel_c_client_t *client;
  channel_c_state_factory_t factory = {0};
  char *error = NULL;
  int port;
  char url[64];

  (void)state;

  server_channel = channel_c_channel_new();
  client_channel = channel_c_channel_new();
  assert_non_null(server_channel);
  assert_non_null(client_channel);

  assert_null(channel_c_server_listen(NULL, "127.0.0.1", 2000, factory, &error));
  assert_non_null(error);
  assert_string_equal(error, "invalid arguments");
  channel_c_free(error);
  error = NULL;

  assert_null(channel_c_server_listen(server_channel, "127.0.0.1", 0, factory, &error));
  assert_non_null(error);
  assert_string_equal(error, "invalid arguments");
  channel_c_free(error);
  error = NULL;

  port = find_free_port();
  assert_true(port > 0);
  snprintf(url, sizeof(url), "ws://127.0.0.1:%d", port);

  server = channel_c_server_listen(server_channel, "127.0.0.1", port, factory, &error);
  assert_non_null(server);
  assert_null(error);
  assert_int_equal(channel_c_server_connection_count(server), 0);

  client = channel_c_client_connect(client_channel, url, NULL, NULL, &error);
  assert_non_null(client);
  assert_null(error);
  assert_true(wait_client_connected(client, 2000));
  assert_true(wait_server_connection_count(server, 1, 2000));

  channel_c_server_stop(server);
  channel_c_server_stop(server);
  channel_c_server_free(server);

  assert_true(wait_client_disconnected(client, 2000));
  channel_c_client_free(client);

  channel_c_channel_free(client_channel);
  channel_c_channel_free(server_channel);
}

int main(void) {
  const struct CMUnitTest tests[] = {
    cmocka_unit_test(test_client_connect_argument_validation_and_failure),
    cmocka_unit_test(test_server_lifecycle_with_connected_client),
  };

  return cmocka_run_group_tests(tests, NULL, NULL);
}
