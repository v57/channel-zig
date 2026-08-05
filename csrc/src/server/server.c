#include <pthread.h>
#include <stdlib.h>
#include <string.h>

#include <libwebsockets.h>

#include "internal.h"

typedef struct server_message server_message_t;
typedef struct server_client server_client_t;
typedef struct server_pss server_pss_t;

struct server_message {
  char *text;
  size_t length;
  server_message_t *next;
};

struct server_client {
  pthread_mutex_t lock;
  int closed;

  struct channel_c_server *server;
  struct lws *wsi;
  channel_c_connection_t *connection;

  server_message_t *queue_head;
  server_message_t *queue_tail;

  server_client_t *next;
};

struct server_pss {
  server_client_t *client;
  char *rx_buffer;
  size_t rx_length;
  size_t rx_capacity;
};

struct channel_c_server {
  pthread_mutex_t lock;
  int running;

  struct lws_context *context;
  pthread_t thread;

  channel_c_channel_t *channel;
  char *host;
  int port;

  channel_c_state_factory_t state_factory;

  server_client_t *clients;
};

static int server_ws_callback(struct lws *wsi, enum lws_callback_reasons reason, void *user, void *in, size_t length);

static struct lws_protocols server_protocols[] = {
  {
    .name = "channel-c",
    .callback = server_ws_callback,
    .per_session_data_size = sizeof(server_pss_t),
    .rx_buffer_size = 0,
    .id = 0,
    .user = NULL,
    .tx_packet_size = 0,
  },
  { NULL, NULL, 0, 0, 0, NULL, 0 }
};

static void server_message_queue_push(server_client_t *client, const char *text) {
  server_message_t *message = (server_message_t *)calloc(1, sizeof(server_message_t));
  size_t text_length;
  if (!message) return;
  text_length = strlen(text);
  if (!channel_c_internal_failpoint_hit("server.queue_strdup_fail")) {
    message->text = (char *)malloc(text_length + 1);
    if (message->text) {
      memcpy(message->text, text, text_length);
      message->text[text_length] = '\0';
      message->length = text_length;
    }
  }
  if (!message->text) {
    free(message); /* LCOV_EXCL_LINE */
    return;        /* LCOV_EXCL_LINE */
  }

  if (client->queue_tail) {
    client->queue_tail->next = message;
  } else {
    client->queue_head = message;
  }
  client->queue_tail = message;
}

static server_message_t *server_message_queue_pop(server_client_t *client) {
  if (channel_c_internal_failpoint_hit("server.queue_pop_skip")) return NULL;
  server_message_t *message = client->queue_head;
  if (!message) return NULL;
  client->queue_head = message->next;
  if (!client->queue_head) client->queue_tail = NULL;
  message->next = NULL;
  return message;
}

static void server_message_queue_clear(server_client_t *client) {
  server_message_t *message = client->queue_head;
  while (message) { /* LCOV_EXCL_START */
    server_message_t *next = message->next;
    free(message->text);
    free(message);
    message = next;
  } /* LCOV_EXCL_STOP */
  client->queue_head = NULL;
  client->queue_tail = NULL;
}

static int server_client_send_text(void *ctx, const char *text) {
  server_client_t *client = (server_client_t *)ctx;
  if (!client || !text) return -1;

  pthread_mutex_lock(&client->lock);
  if (client->closed || !client->wsi) {
    pthread_mutex_unlock(&client->lock); /* LCOV_EXCL_LINE */
    return -1;                           /* LCOV_EXCL_LINE */
  }

  server_message_queue_push(client, text);
  lws_callback_on_writable(client->wsi);
  lws_cancel_service(client->server->context);
  pthread_mutex_unlock(&client->lock);

  return 0;
}

static void server_client_stop_transport(void *ctx) {
  server_client_t *client = (server_client_t *)ctx;
  if (!client) return;

  pthread_mutex_lock(&client->lock);
  if (!client->closed && client->wsi) {
    lws_set_timeout(client->wsi, PENDING_TIMEOUT_CLOSE_SEND, 1);
  }
  pthread_mutex_unlock(&client->lock);
}

static void server_remove_client(struct channel_c_server *server, server_client_t *client) {
  server_client_t *previous = NULL;
  server_client_t *current;
  if (channel_c_internal_failpoint_hit("server.skip_remove_client")) return;

  pthread_mutex_lock(&server->lock);
  current = server->clients;
  while (current) {
    if (current == client) {
      if (previous) {
        previous->next = current->next; /* LCOV_EXCL_LINE */
      } else { /* LCOV_EXCL_LINE */
        server->clients = current->next;
      }
      break;
    }
    previous = current;      /* LCOV_EXCL_LINE */
    current = current->next; /* LCOV_EXCL_LINE */
  }
  pthread_mutex_unlock(&server->lock);
}

static int server_rx_append(server_pss_t *pss, const char *data, size_t length) {
  size_t required;
  char *buffer;

  if (length == 0) return 0;
  required = pss->rx_length + length + 1;
  if (required > pss->rx_capacity) {
    size_t capacity = pss->rx_capacity ? pss->rx_capacity : 1024;
    while (capacity < required) capacity *= 2;
    if (channel_c_internal_failpoint_hit("server.rx_realloc_fail")) return -1;
    buffer = (char *)realloc(pss->rx_buffer, capacity);
    if (!buffer) return -1;
    pss->rx_buffer = buffer;
    pss->rx_capacity = capacity;
  }

  memcpy(pss->rx_buffer + pss->rx_length, data, length);
  pss->rx_length += length;
  pss->rx_buffer[pss->rx_length] = '\0';
  return 0;
}

static void server_rx_flush(server_pss_t *pss) {
  if (!pss->client || !pss->rx_buffer || pss->rx_length == 0) return;
  channel_c_connection_receive_text(pss->client->connection, pss->rx_buffer);
  pss->rx_length = 0;
}

static void *server_thread_main(void *argument) {
  struct channel_c_server *server = (struct channel_c_server *)argument;

  while (1) {
    server_client_t *client;
    pthread_mutex_lock(&server->lock);
    if (!server->running) {
      pthread_mutex_unlock(&server->lock);
      break;
    }
    client = server->clients;
    while (client) {
      pthread_mutex_lock(&client->lock);
      if (!client->closed && client->wsi && client->queue_head) {
        lws_callback_on_writable(client->wsi);
      }
      pthread_mutex_unlock(&client->lock);
      client = client->next;
    }
    pthread_mutex_unlock(&server->lock);
    lws_service(server->context, 50);
  }

  return NULL;
}

static int server_ws_callback(struct lws *wsi, enum lws_callback_reasons reason, void *user, void *in, size_t length) {
  struct lws_context *context = lws_get_context(wsi);
  struct channel_c_server *server = (struct channel_c_server *)lws_context_user(context);
  server_pss_t *pss = (server_pss_t *)user;

  if (!server || !pss) return 0;

  switch (reason) {
    case LWS_CALLBACK_ESTABLISHED: {
      server_client_t *client = channel_c_internal_failpoint_hit("server.client_alloc_fail")
                                  ? NULL
                                  : (server_client_t *)calloc(1, sizeof(server_client_t));
      channel_c_transport_ops_t transport_ops;
      void *state = NULL;

      if (!client) return -1;
      if (channel_c_internal_failpoint_hit("server.client_mutex_init_fail") || pthread_mutex_init(&client->lock, NULL) != 0) {
        free(client); /* LCOV_EXCL_LINE */
        return -1;    /* LCOV_EXCL_LINE */
      }

      if (server->state_factory.create) {
        state = server->state_factory.create(server->state_factory.user_data);
      }

      transport_ops.send_text = server_client_send_text;
      transport_ops.stop = server_client_stop_transport;

      client->server = server;
      client->wsi = wsi;
      client->closed = 0;
      client->connection = channel_c_internal_failpoint_hit("server.connection_create_fail")
                             ? NULL
                             : channel_c_connection_create(
                                 server->channel,
                                 state,
                                 server->state_factory.destroy,
                                 server->state_factory.user_data,
                                 client,
                                 transport_ops
                               );

      if (!client->connection) {
        pthread_mutex_destroy(&client->lock); /* LCOV_EXCL_LINE */
        free(client);                         /* LCOV_EXCL_LINE */
        return -1;                            /* LCOV_EXCL_LINE */
      }

      pthread_mutex_lock(&server->lock);
      client->next = server->clients;
      server->clients = client;
      pthread_mutex_unlock(&server->lock);

      pss->client = client;
      channel_c_internal_connection_mark_connected(client->connection, 1);
      break;
    }

    case LWS_CALLBACK_RECEIVE:
      if (!pss->client) return -1;
      if (server_rx_append(pss, (const char *)in, length) != 0) return -1;
      if (lws_is_final_fragment(wsi) && lws_remaining_packet_payload(wsi) == 0) {
        server_rx_flush(pss);
      }
      break;

    case LWS_CALLBACK_SERVER_WRITEABLE:
      if (pss->client) {
        const size_t write_batch_limit = 16;
        const size_t message_batch_limit = 64;
        const size_t stack_buffer_capacity = 512;
        size_t batches = 0;

        while (batches < write_batch_limit) {
          server_message_t *messages[64];
          size_t message_count = 0;
          size_t payload_length = 0;
          size_t i;
          char *payload = NULL;
          int payload_owned = 0;
          unsigned char stack_buffer[LWS_PRE + 512];
          unsigned char *buffer = NULL;
          int using_heap = 0;
          int written;
          int write_ok = 1;

          if (lws_send_pipe_choked(wsi) || lws_partial_buffered(wsi)) break;

          pthread_mutex_lock(&pss->client->lock);
          while (message_count < message_batch_limit) {
            server_message_t *message = server_message_queue_pop(pss->client);
            if (!message) break;
            messages[message_count] = message;
            payload_length += message->length;
            message_count += 1;
          }
          pthread_mutex_unlock(&pss->client->lock);

          if (message_count == 0) break;

          if (message_count == 1) {
            payload = messages[0]->text;
          } else {
            size_t offset = 0;
            payload_length += (message_count - 1) + 2;
            payload = (char *)malloc(payload_length + 1);
            if (!payload) write_ok = 0;
            if (payload) {
              payload_owned = 1;
              payload[offset++] = '[';
              for (i = 0; i < message_count; i += 1) {
                memcpy(payload + offset, messages[i]->text, messages[i]->length);
                offset += messages[i]->length;
                if (i + 1 < message_count) payload[offset++] = ',';
              }
              payload[offset++] = ']';
              payload[offset] = '\0';
            }
          }

          if (write_ok) {
            if (channel_c_internal_failpoint_hit("server.write_buffer_alloc_fail")) {
              buffer = NULL;
            } else if (payload_length <= stack_buffer_capacity) {
              buffer = stack_buffer;
            } else {
              buffer = (unsigned char *)malloc(LWS_PRE + payload_length);
              using_heap = 1;
            }
            if (!buffer) write_ok = 0;
          }

          if (write_ok) {
            memcpy(buffer + LWS_PRE, payload, payload_length);
            written = channel_c_internal_failpoint_hit("server.write_fail")
                        ? -1
                        : lws_write(wsi, buffer + LWS_PRE, payload_length, LWS_WRITE_TEXT);
            if (written < 0 || (size_t)written != payload_length) write_ok = 0;
          }

          if (using_heap) free(buffer);
          if (payload_owned) free(payload);
          for (i = 0; i < message_count; i += 1) {
            free(messages[i]->text);
            free(messages[i]);
          }

          if (!write_ok) return -1; /* LCOV_EXCL_LINE */
          batches += 1;
        }

        pthread_mutex_lock(&pss->client->lock);
        if (pss->client->queue_head) lws_callback_on_writable(wsi);
        pthread_mutex_unlock(&pss->client->lock);
      }
      break;

    case LWS_CALLBACK_CLOSED:
      if (pss->client) {
        pthread_mutex_lock(&pss->client->lock);
        pss->client->closed = 1;
        pss->client->wsi = NULL;
        pthread_mutex_unlock(&pss->client->lock);

        server_remove_client(server, pss->client);
        channel_c_internal_connection_mark_connected(pss->client->connection, 0);
        channel_c_connection_on_close(pss->client->connection);
        server_message_queue_clear(pss->client);
        channel_c_connection_release(pss->client->connection);

        pthread_mutex_destroy(&pss->client->lock);
        free(pss->client);
        pss->client = NULL;
      }
      free(pss->rx_buffer);
      pss->rx_buffer = NULL;
      pss->rx_length = 0;
      pss->rx_capacity = 0;
      break;

    case LWS_CALLBACK_EVENT_WAIT_CANCELLED: /* LCOV_EXCL_START */
      pthread_mutex_lock(&server->lock);
      {
        server_client_t *client = server->clients;
        while (client) {
          pthread_mutex_lock(&client->lock);
          if (!client->closed && client->wsi && client->queue_head) lws_callback_on_writable(client->wsi);
          pthread_mutex_unlock(&client->lock);
          client = client->next;
        }
      }
      pthread_mutex_unlock(&server->lock);
      break;
      /* LCOV_EXCL_STOP */

    default:
      break;
  }

  return 0;
}

channel_c_server_t *channel_c_server_listen(
  channel_c_channel_t *channel,
  const char *host,
  int port,
  channel_c_state_factory_t state_factory,
  char **out_error
) {
  struct channel_c_server *server;
  struct lws_context_creation_info info;

  if (out_error) *out_error = NULL;

  if (!channel || port <= 0) {
    if (out_error) *out_error = channel_c_strdup("invalid arguments");
    return NULL;
  }

  server = channel_c_internal_failpoint_hit("server.server_alloc_fail")
             ? NULL
             : (struct channel_c_server *)calloc(1, sizeof(struct channel_c_server));
  if (!server) {
    if (out_error) *out_error = channel_c_strdup("internal error");
    return NULL;
  }

  if (channel_c_internal_failpoint_hit("server.mutex_init_fail") || pthread_mutex_init(&server->lock, NULL) != 0) {
    free(server);
    if (out_error) *out_error = channel_c_strdup("internal error");
    return NULL;
  }

  server->channel = channel;
  server->port = port;
  if (!channel_c_internal_failpoint_hit("server.host_strdup_fail")) {
    server->host = channel_c_strdup(host && host[0] ? host : "127.0.0.1");
  }
  server->state_factory = state_factory;
  server->running = 1;

  if (!server->host) {
    pthread_mutex_destroy(&server->lock);
    free(server);
    if (out_error) *out_error = channel_c_strdup("internal error");
    return NULL;
  }

  memset(&info, 0, sizeof(info));
  info.port = server->port;
  info.iface = server->host;
  info.protocols = server_protocols;
  info.gid = -1;
  info.uid = -1;
  info.user = server;

  server->context = channel_c_internal_failpoint_hit("server.context_create_fail")
                      ? NULL
                      : lws_create_context(&info);
  if (!server->context) {
    pthread_mutex_destroy(&server->lock);
    free(server->host);
    free(server);
    if (out_error) *out_error = channel_c_strdup("failed to create websocket server context");
    return NULL;
  }

  if (channel_c_internal_failpoint_hit("server.thread_create_fail") ||
      pthread_create(&server->thread, NULL, server_thread_main, server) != 0) {
    lws_context_destroy(server->context);
    server->context = NULL;
    pthread_mutex_destroy(&server->lock);
    free(server->host);
    free(server);
    if (out_error) *out_error = channel_c_strdup("failed to start websocket server thread");
    return NULL;
  }

  return server;
}

void channel_c_server_stop(channel_c_server_t *server) {
  struct channel_c_server *s = (struct channel_c_server *)server;
  struct lws_context *context = NULL;
  if (!s) return;

  pthread_mutex_lock(&s->lock);
  if (!s->running) {
    pthread_mutex_unlock(&s->lock);
    return;
  }
  s->running = 0;
  if (s->context) lws_cancel_service(s->context);
  pthread_mutex_unlock(&s->lock);

  if (s->thread) {
    pthread_join(s->thread, NULL);
    s->thread = 0;
  }

  pthread_mutex_lock(&s->lock);
  context = s->context;
  s->context = NULL;
  pthread_mutex_unlock(&s->lock);

  if (context) lws_context_destroy(context);
}

void channel_c_server_free(channel_c_server_t *server) {
  struct channel_c_server *s = (struct channel_c_server *)server;
  if (!s) return;

  channel_c_server_stop(server);

  pthread_mutex_lock(&s->lock);
  while (s->clients) { /* LCOV_EXCL_START */
    server_client_t *client = s->clients;
    s->clients = client->next;
    pthread_mutex_lock(&client->lock);
    client->closed = 1;
    client->wsi = NULL;
    pthread_mutex_unlock(&client->lock);
    channel_c_connection_on_close(client->connection);
    server_message_queue_clear(client);
    channel_c_connection_release(client->connection);
    pthread_mutex_destroy(&client->lock);
    free(client);
  } /* LCOV_EXCL_STOP */
  pthread_mutex_unlock(&s->lock);

  pthread_mutex_destroy(&s->lock);
  free(s->host);
  free(s);
}

int channel_c_server_connection_count(channel_c_server_t *server) {
  struct channel_c_server *s = (struct channel_c_server *)server;
  int count = 0;
  server_client_t *client;
  if (!s) return 0;

  pthread_mutex_lock(&s->lock);
  client = s->clients;
  while (client) {
    count += 1;
    client = client->next;
  }
  pthread_mutex_unlock(&s->lock);

  return count;
}

channel_c_connection_t *channel_c_internal_server_first_connection(channel_c_server_t *server) {
  struct channel_c_server *s = (struct channel_c_server *)server;
  channel_c_connection_t *connection = NULL;
  if (!s) return NULL;

  pthread_mutex_lock(&s->lock);
  if (s->clients) connection = s->clients->connection;
  pthread_mutex_unlock(&s->lock);
  return connection;
}
