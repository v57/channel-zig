#include <pthread.h>
#include <stdlib.h>
#include <string.h>

#include <libwebsockets.h>

#include "internal.h"

typedef struct queued_message queued_message_t;

struct queued_message {
  size_t length;
  queued_message_t *next;
  unsigned char payload[];
};

struct channel_c_client {
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

  queued_message_t *queue_head;
  queued_message_t *queue_tail;

  char *rx_buffer;
  size_t rx_length;
  size_t rx_capacity;

  channel_c_connection_t *connection;
};

static int client_ws_callback(struct lws *wsi, enum lws_callback_reasons reason, void *user, void *in, size_t length);

static struct lws_protocols client_protocols[] = {
  {
    .name = "channel-c",
    .callback = client_ws_callback,
    .per_session_data_size = 0,
    .rx_buffer_size = 0,
    .id = 0,
    .user = NULL,
    .tx_packet_size = 0,
  },
  { NULL, NULL, 0, 0, 0, NULL, 0 }
};

static void client_queue_push(struct channel_c_client *client, const char *text) {
  size_t length;
  queued_message_t *message;

  if (!text) return;
  length = strlen(text);
  message = (queued_message_t *)calloc(1, sizeof(queued_message_t) + LWS_PRE + length);
  if (!message) return;

  if (channel_c_internal_failpoint_hit("client.queue_strdup_fail")) {
    free(message);
    return;
  }
  message->length = length;
  memcpy(message->payload + LWS_PRE, text, length);

  if (client->queue_tail) {
    client->queue_tail->next = message;
  } else {
    client->queue_head = message;
  }
  client->queue_tail = message;
}

static queued_message_t *client_queue_pop_batch(
  struct channel_c_client *client,
  size_t max_messages,
  size_t max_payload_bytes,
  size_t *out_count,
  size_t *out_payload_bytes
) {
  queued_message_t *head = NULL;
  queued_message_t *tail = NULL;
  size_t count = 0;
  size_t payload_bytes = 0;

  if (out_count) *out_count = 0;
  if (out_payload_bytes) *out_payload_bytes = 0;
  if (!client) return NULL;

  while (count < max_messages) {
    queued_message_t *message;
    size_t candidate_bytes;

    if (channel_c_internal_failpoint_hit("client.queue_pop_skip")) break;
    message = client->queue_head;
    if (!message) break;

    candidate_bytes = payload_bytes + message->length + (count > 0 ? 1 : 0);
    if (count > 0 && candidate_bytes + 2 > max_payload_bytes) break;

    client->queue_head = message->next;
    if (!client->queue_head) client->queue_tail = NULL;
    message->next = NULL;

    if (tail) {
      tail->next = message;
    } else {
      head = message;
    }
    tail = message;

    payload_bytes = candidate_bytes;
    count += 1;
  }

  if (count == 0) return NULL;
  if (count > 1) payload_bytes += 2;

  if (out_count) *out_count = count;
  if (out_payload_bytes) *out_payload_bytes = payload_bytes;
  return head;
}

static void client_queue_clear(struct channel_c_client *client) {
  queued_message_t *message = client->queue_head;
  while (message) {
    queued_message_t *next = message->next;
    free(message);
    message = next;
  }
  client->queue_head = NULL;
  client->queue_tail = NULL;
}

static int client_transport_send_text(void *ctx, const char *text) {
  struct channel_c_client *client = (struct channel_c_client *)ctx;
  int was_empty;
  if (!client || !text) return -1;

  pthread_mutex_lock(&client->lock);
  if (!client->running) {
    pthread_mutex_unlock(&client->lock);
    return -1;
  }

  was_empty = client->queue_head == NULL;
  client_queue_push(client, text);

  if (was_empty && client->context) {
    lws_cancel_service(client->context);
  }
  if (was_empty && client->wsi && client->connected) {
    lws_callback_on_writable(client->wsi);
  }

  pthread_mutex_unlock(&client->lock);
  return 0;
}

static void client_notify_closed(struct channel_c_client *client) {
  if (client->close_notified) return;
  client->close_notified = 1;
  channel_c_internal_connection_mark_connected(client->connection, 0);
  channel_c_connection_on_close(client->connection);
}

static void client_transport_stop(void *ctx) {
  struct channel_c_client *client = (struct channel_c_client *)ctx;
  if (!client) return;

  pthread_mutex_lock(&client->lock);
  if (!client->running) {
    pthread_mutex_unlock(&client->lock);
    return;
  }
  client->running = 0;
  if (client->context) lws_cancel_service(client->context);
  pthread_mutex_unlock(&client->lock);
}

static int client_rx_append(struct channel_c_client *client, const char *data, size_t length) {
  size_t required;
  char *buffer;

  if (length == 0) return 0;
  required = client->rx_length + length + 1;
  if (required > client->rx_capacity) {
    size_t capacity = client->rx_capacity ? client->rx_capacity : 1024;
    while (capacity < required) capacity *= 2;
    if (channel_c_internal_failpoint_hit("client.rx_realloc_fail")) return -1;
    buffer = (char *)realloc(client->rx_buffer, capacity);
    if (!buffer) return -1;
    client->rx_buffer = buffer;
    client->rx_capacity = capacity;
  }

  memcpy(client->rx_buffer + client->rx_length, data, length);
  client->rx_length += length;
  client->rx_buffer[client->rx_length] = '\0';
  return 0;
}

static int client_ws_callback(struct lws *wsi, enum lws_callback_reasons reason, void *user, void *in, size_t length) {
  struct lws_context *context = lws_get_context(wsi);
  struct channel_c_client *client = (struct channel_c_client *)lws_context_user(context);
  (void)user;

  if (!client) return 0;

  switch (reason) {
    case LWS_CALLBACK_CLIENT_ESTABLISHED:
      pthread_mutex_lock(&client->lock);
      client->wsi = wsi;
      client->connected = 1;
      channel_c_internal_connection_mark_connected(client->connection, 1);
      if (client->queue_head) lws_callback_on_writable(wsi);
      pthread_mutex_unlock(&client->lock);
      break;

    case LWS_CALLBACK_CLIENT_RECEIVE:
      {
        const char *message = NULL;
        pthread_mutex_lock(&client->lock);
        if (client_rx_append(client, (const char *)in, length) != 0) {
          pthread_mutex_unlock(&client->lock); /* LCOV_EXCL_LINE */
          return -1;                           /* LCOV_EXCL_LINE */
        }
        if (lws_is_final_fragment(wsi) && lws_remaining_packet_payload(wsi) == 0) {
          message = client->rx_buffer;
          client->rx_length = 0;
        }
        pthread_mutex_unlock(&client->lock);
        if (message) {
          channel_c_connection_receive_text(client->connection, message);
        }
      }
      break;

    case LWS_CALLBACK_CLIENT_WRITEABLE: {
      queued_message_t *message_batch;
      size_t batch_count = 0;
      size_t payload_bytes = 0;
      int has_more = 0;
      const size_t max_batch_messages = 256;
      const size_t max_batch_bytes = 256 * 1024;
      pthread_mutex_lock(&client->lock);
      message_batch = client_queue_pop_batch(client, max_batch_messages, max_batch_bytes, &batch_count, &payload_bytes);
      has_more = client->queue_head != NULL;
      pthread_mutex_unlock(&client->lock);

      if (message_batch) {
        int write_failed = 0;

        if (batch_count == 1) {
          if (channel_c_internal_failpoint_hit("client.write_buffer_alloc_fail") ||
              channel_c_internal_failpoint_hit("client.write_fail") ||
              lws_write(wsi, message_batch->payload + LWS_PRE, message_batch->length, LWS_WRITE_TEXT) < 0) {
            write_failed = 1;
          }
        } else {
          unsigned char *buffer = channel_c_internal_failpoint_hit("client.write_buffer_alloc_fail")
                                    ? NULL
                                    : (unsigned char *)malloc(LWS_PRE + payload_bytes);
          if (!buffer) {
            write_failed = 1;
          } else {
            queued_message_t *current = message_batch;
            size_t offset = 0;
            buffer[LWS_PRE + offset++] = '[';
            while (current) {
              memcpy(buffer + LWS_PRE + offset, current->payload + LWS_PRE, current->length);
              offset += current->length;
              if (current->next) buffer[LWS_PRE + offset++] = ',';
              current = current->next;
            }
            buffer[LWS_PRE + offset++] = ']';

            if (channel_c_internal_failpoint_hit("client.write_fail") ||
                lws_write(wsi, buffer + LWS_PRE, offset, LWS_WRITE_TEXT) < 0) {
              write_failed = 1;
            }
            free(buffer);
          }
        }

        while (message_batch) {
          queued_message_t *next = message_batch->next;
          free(message_batch);
          message_batch = next;
        }

        if (write_failed) return -1;
      }

      if (has_more) {
        pthread_mutex_lock(&client->lock);
        if (client->queue_head) lws_callback_on_writable(wsi);
        pthread_mutex_unlock(&client->lock);
      }
      break;
    }

    case LWS_CALLBACK_CLIENT_CONNECTION_ERROR:
      pthread_mutex_lock(&client->lock);
      client->connected = 0;
      client->wsi = NULL;
      channel_c_internal_connection_mark_failed(client->connection, "connection error");
      client_notify_closed(client);
      client->running = 0;
      pthread_mutex_unlock(&client->lock);
      break;

    case LWS_CALLBACK_CLIENT_CLOSED:
      pthread_mutex_lock(&client->lock);
      client->connected = 0;
      client->wsi = NULL;
      client_notify_closed(client);
      pthread_mutex_unlock(&client->lock);
      break;

    case LWS_CALLBACK_EVENT_WAIT_CANCELLED:
      pthread_mutex_lock(&client->lock);
      if (client->wsi && client->queue_head) lws_callback_on_writable(client->wsi);
      pthread_mutex_unlock(&client->lock);
      break;

    default:
      break;
  }

  return 0;
}

static void *client_thread_main(void *argument) {
  struct channel_c_client *client = (struct channel_c_client *)argument;

  while (1) {
    struct lws *wsi = NULL;
    int timeout_ms = 20;
    pthread_mutex_lock(&client->lock);
    if (!client->running) {
      pthread_mutex_unlock(&client->lock);
      break;
    }
    if (client->connected) timeout_ms = 2;
    if (client->connected && client->wsi && client->queue_head) {
      wsi = client->wsi;
      timeout_ms = 0;
    }
    pthread_mutex_unlock(&client->lock);
    if (wsi) lws_callback_on_writable(wsi);
    lws_service(client->context, timeout_ms);
  }

  pthread_mutex_lock(&client->lock);
  client_notify_closed(client);
  pthread_mutex_unlock(&client->lock);

  return NULL;
}

static int parse_url(
  const char *url,
  char **out_protocol,
  char **out_address,
  int *out_port,
  char **out_path,
  int *out_ssl
) {
  char *copy;
  const char *protocol = NULL;
  const char *address = NULL;
  const char *path = NULL;
  int port = 0;

  if (!url || !out_protocol || !out_address || !out_port || !out_path || !out_ssl) return -1;

  copy = channel_c_strdup(url);
  if (!copy) return -1;

  if (channel_c_internal_failpoint_hit("client.parse_uri_fail") || lws_parse_uri(copy, &protocol, &address, &port, &path) != 0) {
    free(copy);
    return -1;
  }

  if (!channel_c_internal_failpoint_hit("client.protocol_strdup_fail")) {
    *out_protocol = channel_c_strdup(protocol ? protocol : "ws");
  }
  if (!channel_c_internal_failpoint_hit("client.address_strdup_fail")) {
    *out_address = channel_c_strdup(address ? address : "127.0.0.1");
  }
  if (!channel_c_internal_failpoint_hit("client.path_strdup_fail")) {
    *out_path = channel_c_strdup(path ? path : "/");
  }
  *out_port = port > 0 ? port : 80;
  *out_ssl = (protocol && strcmp(protocol, "wss") == 0) ? 1 : 0;

  free(copy);
  if (!*out_protocol || !*out_address || !*out_path) {
    free(*out_protocol);
    free(*out_address);
    free(*out_path);
    *out_protocol = NULL;
    *out_address = NULL;
    *out_path = NULL;
    return -1;
  }

  return 0;
}

channel_c_client_t *channel_c_client_connect(
  channel_c_channel_t *channel,
  const char *url,
  void *state,
  channel_c_state_destroy_fn state_destroy,
  char **out_error
) {
  struct channel_c_client *client;
  struct lws_context_creation_info context_info;
  struct lws_client_connect_info connect_info;
  channel_c_transport_ops_t transport_ops;

  if (out_error) *out_error = NULL;

  if (!channel || !url) {
    if (out_error) *out_error = channel_c_strdup("invalid arguments");
    return NULL;
  }

  client = channel_c_internal_failpoint_hit("client.client_alloc_fail")
             ? NULL
             : (struct channel_c_client *)calloc(1, sizeof(struct channel_c_client));
  if (!client) {
    if (out_error) *out_error = channel_c_strdup("internal error");
    return NULL;
  }

  if (channel_c_internal_failpoint_hit("client.mutex_init_fail") || pthread_mutex_init(&client->lock, NULL) != 0) {
    free(client);
    if (out_error) *out_error = channel_c_strdup("internal error");
    return NULL;
  }

  client->running = 1;
  client->connected = 0;
  client->close_notified = 0;
  client->url = channel_c_strdup(url);

  if (channel_c_internal_failpoint_hit("client.parse_url_fail") ||
      parse_url(url, &client->protocol, &client->address, &client->port, &client->path, &client->ssl) != 0) {
    pthread_mutex_destroy(&client->lock);
    free(client->url);
    free(client);
    if (out_error) *out_error = channel_c_strdup("invalid websocket url");
    return NULL;
  }

  transport_ops.send_text = client_transport_send_text;
  transport_ops.stop = client_transport_stop;

  client->connection = channel_c_internal_failpoint_hit("client.connection_create_fail")
                         ? NULL
                         : channel_c_connection_create(channel, state, state_destroy, NULL, client, transport_ops);
  if (!client->connection) {
    pthread_mutex_destroy(&client->lock);
    free(client->url);
    free(client->protocol);
    free(client->address);
    free(client->path);
    free(client);
    if (out_error) *out_error = channel_c_strdup("internal error");
    return NULL;
  }

  memset(&context_info, 0, sizeof(context_info));
  context_info.port = CONTEXT_PORT_NO_LISTEN;
  context_info.protocols = client_protocols;
  context_info.gid = -1;
  context_info.uid = -1;
  context_info.user = client;

  client->context = channel_c_internal_failpoint_hit("client.context_create_fail")
                      ? NULL
                      : lws_create_context(&context_info);
  if (!client->context) {
    channel_c_connection_on_close(client->connection);
    channel_c_connection_release(client->connection);
    pthread_mutex_destroy(&client->lock);
    free(client->url);
    free(client->protocol);
    free(client->address);
    free(client->path);
    free(client);
    if (out_error) *out_error = channel_c_strdup("failed to create websocket context");
    return NULL;
  }

  memset(&connect_info, 0, sizeof(connect_info));
  connect_info.context = client->context;
  connect_info.address = client->address;
  connect_info.port = client->port;
  connect_info.path = client->path;
  connect_info.host = client->address;
  connect_info.origin = client->address;
  connect_info.ssl_connection = client->ssl ? LCCSCF_USE_SSL : 0;
  connect_info.protocol = client_protocols[0].name;

  client->wsi = channel_c_internal_failpoint_hit("client.connect_via_info_fail")
                  ? NULL
                  : lws_client_connect_via_info(&connect_info);
  if (!client->wsi) {
    lws_context_destroy(client->context);
    client->context = NULL;
    channel_c_internal_connection_mark_failed(client->connection, "failed to connect websocket");
    channel_c_connection_on_close(client->connection);
    channel_c_connection_release(client->connection);
    pthread_mutex_destroy(&client->lock);
    free(client->url);
    free(client->protocol);
    free(client->address);
    free(client->path);
    free(client);
    if (out_error) *out_error = channel_c_strdup("failed to connect websocket");
    return NULL;
  }

  if (channel_c_internal_failpoint_hit("client.thread_create_fail") ||
      pthread_create(&client->thread, NULL, client_thread_main, client) != 0) {
    lws_context_destroy(client->context);
    client->context = NULL;
    channel_c_connection_on_close(client->connection);
    channel_c_connection_release(client->connection);
    pthread_mutex_destroy(&client->lock);
    free(client->url);
    free(client->protocol);
    free(client->address);
    free(client->path);
    free(client);
    if (out_error) *out_error = channel_c_strdup("failed to start websocket thread");
    return NULL;
  }

  return client;
}

channel_c_sender_t *channel_c_client_sender(channel_c_client_t *client) {
  struct channel_c_client *c = (struct channel_c_client *)client;
  if (!c || !c->connection) return NULL;
  return channel_c_connection_sender(c->connection);
}

int channel_c_client_is_connected(channel_c_client_t *client) {
  struct channel_c_client *c = (struct channel_c_client *)client;
  int connected;
  if (!c) return 0;
  pthread_mutex_lock(&c->lock);
  connected = c->connected;
  pthread_mutex_unlock(&c->lock);
  return connected;
}

int channel_c_client_remote_is_subscribed(channel_c_client_t *client, const char *topic) {
  struct channel_c_client *c = (struct channel_c_client *)client;
  if (!c || !c->connection) return 0;
  return channel_c_connection_is_remote_subscribed(c->connection, topic);
}

void channel_c_client_stop(channel_c_client_t *client) {
  struct channel_c_client *c = (struct channel_c_client *)client;
  struct lws_context *context = NULL;
  if (!c) return;

  client_transport_stop(c);
  if (c->context) lws_cancel_service(c->context);

  if (c->thread) {
    pthread_join(c->thread, NULL);
    c->thread = 0;
  }

  pthread_mutex_lock(&c->lock);
  context = c->context;
  c->context = NULL;
  c->wsi = NULL;
  c->connected = 0;
  pthread_mutex_unlock(&c->lock);

  if (context) {
    lws_context_destroy(context);
  }

  pthread_mutex_lock(&c->lock);
  client_notify_closed(c);
  pthread_mutex_unlock(&c->lock);
}

void channel_c_client_free(channel_c_client_t *client) {
  struct channel_c_client *c = (struct channel_c_client *)client;
  if (!c) return;

  channel_c_client_stop(client);

  client_queue_clear(c);
  free(c->rx_buffer);

  if (c->connection) {
    channel_c_connection_release(c->connection);
    c->connection = NULL;
  }

  pthread_mutex_destroy(&c->lock);

  free(c->url);
  free(c->protocol);
  free(c->address);
  free(c->path);
  free(c);
}

channel_c_connection_t *channel_c_internal_client_connection(channel_c_client_t *client) {
  struct channel_c_client *c = (struct channel_c_client *)client;
  if (!c) return NULL;
  return c->connection;
}
