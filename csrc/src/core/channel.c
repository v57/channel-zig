#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <yyjson.h>

#include "internal.h"

typedef struct post_entry post_entry_t;
typedef struct stream_entry stream_entry_t;
typedef struct subscription_entry subscription_entry_t;
typedef struct channel_link channel_link_t;
typedef struct pending_request pending_request_t;
typedef struct stream_queue_item stream_queue_item_t;
typedef struct listener_entry listener_entry_t;
typedef struct listener_callback listener_callback_t;
typedef struct topic_interest topic_interest_t;
typedef struct active_stream active_stream_t;

typedef enum {
  PENDING_SEND = 0,
  PENDING_STREAM = 1,
  PENDING_SUB = 2,
  PENDING_SEND_ASYNC = 3,
} pending_type_t;

#define PENDING_SEND_FLAG_KEEP_BODY 0x2
#define PENDING_SEND_FLAG_KEEP_ERROR 0x4
#define PENDING_SEND_FLAG_HAS_ERROR 0x8
#define PENDING_SEND_FLAG_ASYNC_CLOSED 0x10
#define PENDING_INDEX_BUCKETS 4096

typedef struct {
  char *name;
  int remaining;
} failpoint_entry_t;

static pthread_mutex_t g_failpoint_lock = PTHREAD_MUTEX_INITIALIZER;
static failpoint_entry_t g_failpoints[64];
static atomic_int g_failpoint_active = 0;

void channel_c_internal_failpoint_reset(void) {
  size_t i;
  pthread_mutex_lock(&g_failpoint_lock);
  for (i = 0; i < sizeof(g_failpoints) / sizeof(g_failpoints[0]); i += 1) {
    free(g_failpoints[i].name);
    g_failpoints[i].name = NULL;
    g_failpoints[i].remaining = 0;
  }
  atomic_store(&g_failpoint_active, 0);
  pthread_mutex_unlock(&g_failpoint_lock);
}

void channel_c_internal_failpoint_set(const char *name, int count) {
  size_t i;
  size_t slot = sizeof(g_failpoints) / sizeof(g_failpoints[0]);
  char *copy;

  if (!name || count <= 0) return;

  copy = (char *)malloc(strlen(name) + 1);
  if (!copy) return;
  strcpy(copy, name);

  pthread_mutex_lock(&g_failpoint_lock);
  for (i = 0; i < sizeof(g_failpoints) / sizeof(g_failpoints[0]); i += 1) {
    if (g_failpoints[i].name && strcmp(g_failpoints[i].name, name) == 0) {
      free(g_failpoints[i].name);
      g_failpoints[i].name = copy;
      g_failpoints[i].remaining = count;
      atomic_store(&g_failpoint_active, 1);
      pthread_mutex_unlock(&g_failpoint_lock);
      return;
    }
    if (!g_failpoints[i].name && slot == sizeof(g_failpoints) / sizeof(g_failpoints[0])) slot = i;
  }

  if (slot < sizeof(g_failpoints) / sizeof(g_failpoints[0])) {
    g_failpoints[slot].name = copy;
    g_failpoints[slot].remaining = count;
    atomic_store(&g_failpoint_active, 1);
    copy = NULL;
  }

  pthread_mutex_unlock(&g_failpoint_lock);
  free(copy);
}

int channel_c_internal_failpoint_hit(const char *name) {
  size_t i;
  int hit = 0;
  if (!name) return 0;
  if (!atomic_load(&g_failpoint_active)) return 0;

  pthread_mutex_lock(&g_failpoint_lock);
  for (i = 0; i < sizeof(g_failpoints) / sizeof(g_failpoints[0]); i += 1) {
    if (!g_failpoints[i].name) continue;
    if (strcmp(g_failpoints[i].name, name) != 0) continue;
    if (g_failpoints[i].remaining > 0) {
      g_failpoints[i].remaining -= 1;
      hit = 1;
    }
    break;
  }
  pthread_mutex_unlock(&g_failpoint_lock);
  return hit;
}

struct post_entry {
  char *path;
  channel_c_post_handler_fn handler;
  void *user_data;
  post_entry_t *next;
};

struct stream_entry {
  char *path;
  channel_c_stream_handler_fn handler;
  void *user_data;
  stream_entry_t *next;
};

struct subscription_entry {
  char *name;
  char *prefix;
  channel_c_subscription_resolver_fn resolver;
  void *user_data;
  subscription_entry_t *next;
};

struct channel_link {
  channel_c_connection_t *connection;
  channel_link_t *next;
};

struct channel_c_channel {
  pthread_mutex_t lock;
  int skip_post_payload_json;
  post_entry_t *posts;
  stream_entry_t *streams;
  subscription_entry_t *subscriptions;
  channel_link_t *connections;
};

struct stream_queue_item {
  char *body;
  char *error;
  int done;
  stream_queue_item_t *next;
};

struct pending_request {
  int id;
  pending_type_t type;
  int completed;
  char *body;
  char *error;
  char *topic;
  int stream_done;
  stream_queue_item_t *stream_head;
  stream_queue_item_t *stream_tail;
  channel_c_async_result_fn async_callback;
  void *async_user_data;
  int attached;
  pthread_cond_t cond;
  pending_request_t *prev;
  pending_request_t *hash_next;
  pending_request_t *next;
};

struct listener_callback {
  int id;
  channel_c_event_callback_fn callback;
  void *user_data;
  listener_callback_t *next;
};

struct listener_entry {
  char *topic;
  listener_callback_t *callbacks;
  listener_entry_t *next;
};

struct topic_interest {
  char *topic;
  topic_interest_t *next;
};

struct active_stream {
  int id;
  int cancelled;
  active_stream_t *next;
};

struct channel_c_sender {
  channel_c_connection_t *connection;
};

struct channel_c_values {
  channel_c_connection_t *connection;
  int request_id;
  int closed;
};

struct channel_c_subscription_handle {
  channel_c_connection_t *connection;
  char *topic;
  int callback_id;
  int cancelled;
};

struct channel_c_stream_emitter {
  channel_c_connection_t *connection;
  int request_id;
  active_stream_t *active;
};

struct channel_c_connection {
  pthread_mutex_t lock;
  pthread_cond_t connected_cond;
  int ref_count;
  int is_connected;
  int is_closed;
  int process_fast_path_enabled;
  int next_request_id;
  int next_callback_id;

  channel_c_channel_t *channel;
  channel_link_t *channel_link;

  pending_request_t *pending;
  listener_entry_t *listeners;
  topic_interest_t *remote_topics;
  active_stream_t *active_streams;

  channel_c_sender_t sender;

  void *state;
  channel_c_state_destroy_fn state_destroy;
  void *state_destroy_user_data;

  void *transport_ctx;
  channel_c_transport_ops_t transport_ops;

  pending_request_t *pending_tail;
  pending_request_t *pending_pool;
  pending_request_t *pending_index[PENDING_INDEX_BUCKETS];
};

typedef struct {
  channel_c_connection_t *connection;
  int request_id;
  char *path;
  char *body_json;
  char *context_json;
  channel_c_post_handler_fn handler;
  void *handler_user_data;
} post_task_t;

typedef struct {
  channel_c_connection_t *connection;
  int request_id;
  char *path;
  char *body_json;
  char *context_json;
  channel_c_stream_handler_fn handler;
  void *handler_user_data;
  active_stream_t *active;
} stream_task_t;

static int has_prefix(const char *prefix) {
  return prefix && prefix[0] != '\0';
}

char *channel_c_strdup(const char *source) {
  size_t length;
  char *copy;
  if (!source) return NULL;
  length = strlen(source);
  copy = (char *)malloc(length + 1);
  if (!copy) return NULL;
  memcpy(copy, source, length + 1);
  return copy;
}

void channel_c_free(void *pointer) {
  if (pointer) free(pointer);
}

static void connection_retain(channel_c_connection_t *connection) {
  pthread_mutex_lock(&connection->lock);
  connection->ref_count += 1;
  pthread_mutex_unlock(&connection->lock);
}

static void free_stream_queue(stream_queue_item_t *item) {
  while (item) {
    stream_queue_item_t *next = item->next;
    free(item->body);
    free(item->error);
    free(item);
    item = next;
  }
}

static void pending_clear_data(pending_request_t *pending) {
  if (!pending) return;
  free(pending->body);
  pending->body = NULL;
  free(pending->error);
  pending->error = NULL;
  free(pending->topic);
  pending->topic = NULL;
  free_stream_queue(pending->stream_head);
  pending->stream_head = NULL;
  pending->stream_tail = NULL;
  pending->stream_done = 0;
  pending->completed = 0;
  pending->async_callback = NULL;
  pending->async_user_data = NULL;
  pending->attached = 0;
  pending->prev = NULL;
  pending->hash_next = NULL;
  pending->next = NULL;
}

static void pending_dispose(pending_request_t *pending) {
  if (!pending) return;
  pending_clear_data(pending);
  pthread_cond_destroy(&pending->cond);
  free(pending);
}

static void free_listener_callbacks(listener_callback_t *callback) {
  while (callback) {
    listener_callback_t *next = callback->next;
    free(callback);
    callback = next;
  }
}

static void free_listeners(listener_entry_t *listener) {
  while (listener) {
    listener_entry_t *next = listener->next;
    free(listener->topic);
    free_listener_callbacks(listener->callbacks);
    free(listener);
    listener = next;
  }
}

static void free_topics(topic_interest_t *topic) {
  while (topic) {
    topic_interest_t *next = topic->next;
    free(topic->topic);
    free(topic);
    topic = next;
  }
}

static void free_active_streams(active_stream_t *stream) {
  while (stream) {
    active_stream_t *next = stream->next;
    free(stream);
    stream = next;
  }
}

void channel_c_connection_release(channel_c_connection_t *connection) {
  int should_free = 0;
  if (!connection) return;

  pthread_mutex_lock(&connection->lock);
  connection->ref_count -= 1;
  if (connection->ref_count == 0) should_free = 1;
  pthread_mutex_unlock(&connection->lock);

  if (!should_free) return;

  if (connection->state_destroy) {
    connection->state_destroy(connection->state, connection->state_destroy_user_data);
  }

  free_listeners(connection->listeners);
  free_topics(connection->remote_topics);
  free_active_streams(connection->active_streams);

  while (connection->pending) {
    pending_request_t *next = connection->pending->next;
    pending_dispose(connection->pending);
    connection->pending = next;
  }
  connection->pending_tail = NULL;
  while (connection->pending_pool) {
    pending_request_t *next = connection->pending_pool->next;
    pending_dispose(connection->pending_pool);
    connection->pending_pool = next;
  }

  pthread_cond_destroy(&connection->connected_cond);
  pthread_mutex_destroy(&connection->lock);
  free(connection);
}

static post_entry_t *find_post_handler(channel_c_channel_t *channel, const char *path) {
  post_entry_t *entry = channel->posts;
  while (entry) {
    if (strcmp(entry->path, path) == 0) return entry;
    entry = entry->next;
  }
  return NULL;
}

static stream_entry_t *find_stream_handler(channel_c_channel_t *channel, const char *path) {
  stream_entry_t *entry = channel->streams;
  while (entry) {
    if (strcmp(entry->path, path) == 0) return entry;
    entry = entry->next;
  }
  return NULL;
}

static subscription_entry_t *find_subscription(channel_c_channel_t *channel, const char *name) {
  subscription_entry_t *entry = channel->subscriptions;
  while (entry) {
    if (strcmp(entry->name, name) == 0) return entry;
    entry = entry->next;
  }
  return NULL;
}

static int topic_set_contains(topic_interest_t *topics, const char *topic) {
  while (topics) {
    if (strcmp(topics->topic, topic) == 0) return 1;
    topics = topics->next;
  }
  return 0;
}

static void topic_set_add(topic_interest_t **topics, const char *topic) {
  topic_interest_t *entry;
  if (topic_set_contains(*topics, topic)) return;
  entry = (topic_interest_t *)calloc(1, sizeof(topic_interest_t));
  if (!entry) return;
  entry->topic = channel_c_strdup(topic);
  if (!entry->topic) {
    free(entry); /* LCOV_EXCL_LINE */
    return;      /* LCOV_EXCL_LINE */
  }
  entry->next = *topics;
  *topics = entry;
}

static void topic_set_remove(topic_interest_t **topics, const char *topic) {
  topic_interest_t *previous = NULL;
  topic_interest_t *current = *topics;
  while (current) {
    if (strcmp(current->topic, topic) == 0) {
      if (previous) {
        previous->next = current->next;
      } else {
        *topics = current->next;
      }
      free(current->topic);
      free(current);
      return;
    }
    previous = current;
    current = current->next;
  }
}

static listener_entry_t *find_listener(listener_entry_t *listeners, const char *topic) {
  listener_entry_t *listener = listeners;
  while (listener) {
    if (strcmp(listener->topic, topic) == 0) return listener;
    listener = listener->next;
  }
  return NULL;
}

static char *json_write_value(yyjson_val *value) {
  if (!value) return NULL;
  return yyjson_val_write(value, YYJSON_WRITE_ALLOW_INF_AND_NAN, NULL);
}

static yyjson_mut_val *json_copy_value_from_text(yyjson_mut_doc *doc, const char *json_text) {
  yyjson_doc *source_doc;
  yyjson_val *source_root;
  yyjson_mut_val *copied;

  if (!doc || !json_text) return NULL;
  source_doc = yyjson_read(json_text, strlen(json_text), YYJSON_READ_ALLOW_INF_AND_NAN);
  if (!source_doc) return NULL;
  source_root = yyjson_doc_get_root(source_doc);
  copied = yyjson_val_mut_copy(doc, source_root);
  yyjson_doc_free(source_doc);
  return copied;
}

static char *json_message_response(
  int id,
  const char *body_json,
  const char *error_text,
  const char *topic,
  int include_done,
  int done_value
) {
  int id_length;
  size_t body_length;
  size_t text_length;
  char *text;

  if (!error_text && !topic && !include_done) {
    id_length = snprintf(NULL, 0, "%d", id);
    if (id_length < 0) return NULL;

    if (!body_json) {
      text_length = (size_t)id_length + strlen("{\"id\":}");
      text = (char *)malloc(text_length + 1);
      if (!text) return NULL;
      if (snprintf(text, text_length + 1, "{\"id\":%d}", id) < 0) {
        free(text);
        return NULL;
      }
      return text;
    }

    body_length = strlen(body_json);
    text_length = (size_t)id_length + body_length + strlen("{\"id\":,\"body\":}");
    text = (char *)malloc(text_length + 1);
    if (!text) return NULL;
    if (snprintf(text, text_length + 1, "{\"id\":%d,\"body\":%s}", id, body_json) < 0) {
      free(text);
      return NULL;
    }
    return text;
  }

  yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
  yyjson_mut_val *root;
  text = NULL;

  if (!doc) return NULL;
  root = yyjson_mut_obj(doc);
  yyjson_mut_doc_set_root(doc, root);
  yyjson_mut_obj_add_int(doc, root, "id", id);

  if (topic) yyjson_mut_obj_add_strcpy(doc, root, "topic", topic);
  if (error_text) {
    yyjson_mut_obj_add_strcpy(doc, root, "error", error_text);
  } else if (body_json) {
    yyjson_mut_val *body = json_copy_value_from_text(doc, body_json);
    if (body) yyjson_mut_obj_add_val(doc, root, "body", body);
  }

  if (include_done) {
    yyjson_mut_obj_add_bool(doc, root, "done", done_value ? 1 : 0);
  }

  text = yyjson_mut_write(doc, YYJSON_WRITE_ALLOW_INF_AND_NAN, NULL);
  yyjson_mut_doc_free(doc);
  return text;
}

static char *json_message_event(const char *topic, const char *body_json) {
  yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
  yyjson_mut_val *root;
  yyjson_mut_val *body;
  char *text;

  if (!doc || !topic || !body_json) {
    if (doc) yyjson_mut_doc_free(doc); /* LCOV_EXCL_LINE */
    return NULL;                       /* LCOV_EXCL_LINE */
  }

  root = yyjson_mut_obj(doc);
  yyjson_mut_doc_set_root(doc, root);
  yyjson_mut_obj_add_strcpy(doc, root, "topic", topic);

  body = json_copy_value_from_text(doc, body_json);
  if (body) yyjson_mut_obj_add_val(doc, root, "body", body);

  text = yyjson_mut_write(doc, YYJSON_WRITE_ALLOW_INF_AND_NAN, NULL);
  yyjson_mut_doc_free(doc);
  return text;
}

static char *json_message_request(
  int id,
  const char *field,
  const char *name,
  const char *body_json,
  const char *context_json
) {
  yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
  yyjson_mut_val *root;
  yyjson_mut_val *body;
  yyjson_mut_val *context;
  char *text;

  if (!doc || !field || !name) {
    if (doc) yyjson_mut_doc_free(doc); /* LCOV_EXCL_LINE */
    return NULL;                       /* LCOV_EXCL_LINE */
  }

  root = yyjson_mut_obj(doc);
  yyjson_mut_doc_set_root(doc, root);
  yyjson_mut_obj_add_int(doc, root, "id", id);
  yyjson_mut_obj_add_strcpy(doc, root, field, name);

  if (body_json) {
    body = json_copy_value_from_text(doc, body_json);
    if (body) yyjson_mut_obj_add_val(doc, root, "body", body);
  }
  if (context_json) {
    context = json_copy_value_from_text(doc, context_json);
    if (context) yyjson_mut_obj_add_val(doc, root, "context", context);
  }

  text = yyjson_mut_write(doc, YYJSON_WRITE_ALLOW_INF_AND_NAN, NULL);
  yyjson_mut_doc_free(doc);
  return text;
}

static int json_text_needs_escape(const char *text) {
  const unsigned char *scan = (const unsigned char *)text;
  if (!scan) return 1;
  while (*scan) {
    if (*scan < 0x20 || *scan == '"' || *scan == '\\') return 1;
    scan += 1;
  }
  return 0;
}

static char *json_message_request_fast(int id, const char *field, const char *name) {
  char *text;
  size_t length;
  int id_length;

  if (!field || !name) return NULL;
  if (json_text_needs_escape(field) || json_text_needs_escape(name)) return NULL;

  id_length = snprintf(NULL, 0, "%d", id);
  if (id_length <= 0) return NULL;

  length = (size_t)id_length + strlen(field) + strlen(name) + strlen("{\"id\":,\"\":\"\"}");
  text = (char *)malloc(length + 1);
  if (!text) return NULL;

  if (snprintf(text, length + 1, "{\"id\":%d,\"%s\":\"%s\"}", id, field, name) < 0) {
    free(text);
    return NULL;
  }
  return text;
}

static char *json_message_unsubscribe(const char *topic) {
  yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
  yyjson_mut_val *root;
  char *text;

  if (!doc || !topic) {
    if (doc) yyjson_mut_doc_free(doc); /* LCOV_EXCL_LINE */
    return NULL;                       /* LCOV_EXCL_LINE */
  }
  root = yyjson_mut_obj(doc);
  yyjson_mut_doc_set_root(doc, root);
  yyjson_mut_obj_add_strcpy(doc, root, "unsub", topic);
  text = yyjson_mut_write(doc, YYJSON_WRITE_ALLOW_INF_AND_NAN, NULL);
  yyjson_mut_doc_free(doc);
  return text;
}

static char *json_message_cancel(int id) {
  yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
  yyjson_mut_val *root;
  char *text;

  if (!doc) return NULL;
  root = yyjson_mut_obj(doc);
  yyjson_mut_doc_set_root(doc, root);
  yyjson_mut_obj_add_int(doc, root, "cancel", id);
  text = yyjson_mut_write(doc, YYJSON_WRITE_ALLOW_INF_AND_NAN, NULL);
  yyjson_mut_doc_free(doc);
  return text;
}

static int connection_transport_send(channel_c_connection_t *connection, const char *text) {
  int result;
  channel_c_transport_ops_t ops;
  void *ctx;

  if (!connection || !text) return -1;

  pthread_mutex_lock(&connection->lock);
  if (connection->is_closed || !connection->transport_ops.send_text) {
    pthread_mutex_unlock(&connection->lock);
    return -1;
  }
  ops = connection->transport_ops;
  ctx = connection->transport_ctx;
  pthread_mutex_unlock(&connection->lock);

  result = ops.send_text(ctx, text);
  return result;
}

static size_t pending_index_slot(int id) { return ((size_t)(uint32_t)id) & (PENDING_INDEX_BUCKETS - 1); }

static void pending_index_insert_locked(channel_c_connection_t *connection, pending_request_t *pending) {
  size_t slot;
  if (!connection || !pending) return;
  slot = pending_index_slot(pending->id);
  pending->hash_next = connection->pending_index[slot];
  connection->pending_index[slot] = pending;
}

static void pending_index_remove_locked(channel_c_connection_t *connection, pending_request_t *pending) {
  pending_request_t **cursor;
  if (!connection || !pending) return;

  cursor = &connection->pending_index[pending_index_slot(pending->id)];
  while (*cursor) {
    if (*cursor == pending) {
      *cursor = pending->hash_next;
      break;
    }
    cursor = &(*cursor)->hash_next;
  }
  pending->hash_next = NULL;
}

static pending_request_t *pending_find_locked(channel_c_connection_t *connection, int id) {
  pending_request_t *pending;
  if (!connection) return NULL;
  pending = connection->pending_index[pending_index_slot(id)];
  while (pending) {
    if (pending->id == id && pending->attached) return pending;
    pending = pending->hash_next;
  }
  return NULL;
}

static void pending_attach_locked(channel_c_connection_t *connection, pending_request_t *pending) {
  if (!connection || !pending || pending->attached) return;

  pending->prev = connection->pending_tail;
  pending->next = NULL;
  if (connection->pending_tail) {
    connection->pending_tail->next = pending;
  } else {
    connection->pending = pending;
  }
  connection->pending_tail = pending;
  pending->attached = 1;
  pending_index_insert_locked(connection, pending);
}

static pending_request_t *pending_detach_locked(channel_c_connection_t *connection, pending_request_t *pending) {
  if (!connection || !pending || !pending->attached) return pending;

  if (pending->prev) {
    pending->prev->next = pending->next;
  } else {
    connection->pending = pending->next;
  }

  if (pending->next) {
    pending->next->prev = pending->prev;
  } else {
    connection->pending_tail = pending->prev;
  }

  pending_index_remove_locked(connection, pending);
  pending->attached = 0;
  pending->prev = NULL;
  pending->next = NULL;
  return pending;
}

static void pending_recycle_locked(channel_c_connection_t *connection, pending_request_t *pending) {
  if (!connection || !pending) return;
  if (pending->attached) pending_detach_locked(connection, pending);
  pending_clear_data(pending);
  pending->id = 0;
  pending->type = PENDING_SEND;
  pending->next = connection->pending_pool;
  connection->pending_pool = pending;
}

static void pending_recycle(channel_c_connection_t *connection, pending_request_t *pending) {
  if (!connection || !pending) return;
  pthread_mutex_lock(&connection->lock);
  pending_recycle_locked(connection, pending);
  pthread_mutex_unlock(&connection->lock);
}

static void pending_remove(channel_c_connection_t *connection, int id) {
  pending_request_t *current;
  if (!connection) return;
  current = pending_find_locked(connection, id);
  if (!current) return;
  pending_detach_locked(connection, current);
  pending_recycle_locked(connection, current);
}

static void pending_finalize_locked(channel_c_connection_t *connection, pending_request_t *pending) {
  if (!connection || !pending) return;
  pending_recycle_locked(connection, pending);
}

static int pending_wait(channel_c_connection_t *connection, pending_request_t *pending) {
  while (!pending->completed && !connection->is_closed) {
    pthread_cond_wait(&pending->cond, &connection->lock);
  }
  return connection->is_closed ? -1 : 0;
}

static void pending_complete_send(pending_request_t *pending, const char *body_json, const char *error_text) {
  if (pending->completed) return;
  pending->completed = 1;
  if (body_json && (pending->stream_done & PENDING_SEND_FLAG_KEEP_BODY)) {
    pending->body = channel_c_strdup(body_json);
  }
  if (error_text) {
    pending->stream_done |= PENDING_SEND_FLAG_HAS_ERROR;
    if (pending->stream_done & PENDING_SEND_FLAG_KEEP_ERROR) {
      pending->error = channel_c_strdup(error_text);
    }
  }
  pthread_cond_broadcast(&pending->cond);
}

static void pending_complete_sub(pending_request_t *pending, const char *topic, const char *body_json, const char *error_text) {
  if (pending->completed) return;
  pending->completed = 1;
  if (topic) pending->topic = channel_c_strdup(topic);
  if (body_json) pending->body = channel_c_strdup(body_json);
  if (error_text) pending->error = channel_c_strdup(error_text);
  pthread_cond_broadcast(&pending->cond);
}

static void pending_push_stream(pending_request_t *pending, const char *body_json, const char *error_text, int done) {
  stream_queue_item_t *item = (stream_queue_item_t *)calloc(1, sizeof(stream_queue_item_t));
  if (!item) return;

  item->done = done;
  if (body_json) item->body = channel_c_strdup(body_json);
  if (error_text) item->error = channel_c_strdup(error_text);

  if (pending->stream_tail) {
    pending->stream_tail->next = item;
  } else {
    pending->stream_head = item;
  }
  pending->stream_tail = item;

  if (done || error_text) pending->stream_done = 1;
  pthread_cond_broadcast(&pending->cond);
}

static stream_queue_item_t *pending_shift_stream(pending_request_t *pending) {
  stream_queue_item_t *item = pending->stream_head;
  if (!item) return NULL;
  pending->stream_head = item->next;
  if (!pending->stream_head) pending->stream_tail = NULL;
  item->next = NULL;
  return item;
}

static void pending_dispatch_async_result(
  channel_c_async_result_fn callback,
  channel_c_async_status_t status,
  const char *body_json,
  const char *error_text,
  void *user_data
) {
  if (!callback) return;
  callback(status, body_json, error_text, user_data);
}

static void connection_drain_completed_async(channel_c_connection_t *connection) {
  if (!connection) return;

  while (1) {
    pending_request_t *pending = NULL;
    channel_c_async_result_fn callback = NULL;
    void *user_data = NULL;
    channel_c_async_status_t status = CHANNEL_C_ASYNC_STATUS_CLOSED;
    char *body_json = NULL;
    char *error_text = NULL;

    pthread_mutex_lock(&connection->lock);
    pending = connection->pending;
    while (pending) {
      if (pending->type == PENDING_SEND_ASYNC && pending->completed) break;
      pending = pending->next;
    }

    if (!pending) {
      pthread_mutex_unlock(&connection->lock);
      break;
    }

    callback = pending->async_callback;
    user_data = pending->async_user_data;

    if (pending->error) {
      error_text = channel_c_strdup(pending->error);
      status = (pending->stream_done & PENDING_SEND_FLAG_ASYNC_CLOSED) ? CHANNEL_C_ASYNC_STATUS_CLOSED
                                                                        : CHANNEL_C_ASYNC_STATUS_ERROR;
    } else if (pending->body) {
      body_json = channel_c_strdup(pending->body);
      status = CHANNEL_C_ASYNC_STATUS_OK;
    } else if (pending->stream_done & PENDING_SEND_FLAG_ASYNC_CLOSED) {
      status = CHANNEL_C_ASYNC_STATUS_CLOSED;
    } else {
      status = CHANNEL_C_ASYNC_STATUS_OK;
    }

    pending_recycle_locked(connection, pending);
    pthread_mutex_unlock(&connection->lock);

    pending_dispatch_async_result(callback, status, body_json, error_text, user_data);
    free(body_json);
    free(error_text);
  }
}

static active_stream_t *active_stream_find(active_stream_t *head, int id) {
  while (head) {
    if (head->id == id) return head;
    head = head->next;
  }
  return NULL;
}

static void active_stream_remove(active_stream_t **head, int id) {
  active_stream_t *previous = NULL;
  active_stream_t *current = *head;

  while (current) {
    if (current->id == id) {
      if (previous) {
        previous->next = current->next;
      } else {
        *head = current->next;
      }
      free(current);
      return;
    }
    previous = current;
    current = current->next;
  }
}

static void connection_signal_all_pending_closed(channel_c_connection_t *connection) {
  pending_request_t *pending = connection->pending;
  while (pending) {
    if (!pending->completed && pending->type != PENDING_STREAM) {
      pending->completed = 1;
      if (pending->type == PENDING_SEND_ASYNC) {
        pending->stream_done |= PENDING_SEND_FLAG_ASYNC_CLOSED;
      }
      if (!pending->error) pending->error = channel_c_strdup("connection closed");
    }
    pthread_cond_broadcast(&pending->cond);
    pending = pending->next;
  }
}

static void channel_attach_connection(channel_c_channel_t *channel, channel_c_connection_t *connection) {
  channel_link_t *link = (channel_link_t *)calloc(1, sizeof(channel_link_t));
  if (!link) return;

  link->connection = connection;

  pthread_mutex_lock(&channel->lock);
  link->next = channel->connections;
  channel->connections = link;
  pthread_mutex_unlock(&channel->lock);

  pthread_mutex_lock(&connection->lock);
  connection->channel_link = link;
  pthread_mutex_unlock(&connection->lock);
}

static void channel_detach_connection(channel_c_channel_t *channel, channel_c_connection_t *connection) {
  channel_link_t *previous = NULL;
  channel_link_t *current;

  pthread_mutex_lock(&channel->lock);
  current = channel->connections;
  while (current) {
    if (current->connection == connection) {
      if (previous) {
        previous->next = current->next;
      } else {
        channel->connections = current->next;
      }
      free(current);
      break;
    }
    previous = current;
    current = current->next;
  }
  pthread_mutex_unlock(&channel->lock);

  pthread_mutex_lock(&connection->lock);
  connection->channel_link = NULL;
  pthread_mutex_unlock(&connection->lock);
}

channel_c_connection_t *channel_c_connection_create(
  channel_c_channel_t *channel,
  void *state,
  channel_c_state_destroy_fn state_destroy,
  void *state_destroy_user_data,
  void *transport_ctx,
  channel_c_transport_ops_t transport_ops
) {
  if (channel_c_internal_failpoint_hit("core.connection_create_null")) return NULL;
  channel_c_connection_t *connection = (channel_c_connection_t *)calloc(1, sizeof(channel_c_connection_t));
  if (!connection) return NULL;

  if (channel_c_internal_failpoint_hit("core.connection_mutex_init_fail") || pthread_mutex_init(&connection->lock, NULL) != 0) {
    free(connection);
    return NULL;
  }
  if (channel_c_internal_failpoint_hit("core.connection_cond_init_fail") || pthread_cond_init(&connection->connected_cond, NULL) != 0) {
    pthread_mutex_destroy(&connection->lock);
    free(connection);
    return NULL;
  }

  connection->ref_count = 1;
  connection->is_connected = 0;
  connection->is_closed = 0;
  connection->process_fast_path_enabled = 0;
  connection->next_request_id = 1;
  connection->next_callback_id = 1;

  connection->channel = channel;
  connection->sender.connection = connection;

  connection->state = state;
  connection->state_destroy = state_destroy;
  connection->state_destroy_user_data = state_destroy_user_data;

  connection->transport_ctx = transport_ctx;
  connection->transport_ops = transport_ops;

  channel_attach_connection(channel, connection);
  return connection;
}

void channel_c_connection_set_transport(
  channel_c_connection_t *connection,
  void *transport_ctx,
  channel_c_transport_ops_t transport_ops
) {
  if (!connection) return;
  pthread_mutex_lock(&connection->lock);
  connection->transport_ctx = transport_ctx;
  connection->transport_ops = transport_ops;
  pthread_mutex_unlock(&connection->lock);
}

channel_c_sender_t *channel_c_connection_sender(channel_c_connection_t *connection) {
  if (!connection) return NULL;
  return &connection->sender;
}

int channel_c_connection_is_connected(channel_c_connection_t *connection) {
  int connected;
  if (!connection) return 0;
  pthread_mutex_lock(&connection->lock);
  connected = connection->is_connected && !connection->is_closed;
  pthread_mutex_unlock(&connection->lock);
  return connected;
}

int channel_c_connection_is_remote_subscribed(channel_c_connection_t *connection, const char *topic) {
  int is_subscribed = 0;
  if (!connection || !topic) return 0;
  pthread_mutex_lock(&connection->lock);
  is_subscribed = topic_set_contains(connection->remote_topics, topic);
  pthread_mutex_unlock(&connection->lock);
  return is_subscribed;
}

void channel_c_connection_on_close(channel_c_connection_t *connection) {
  int already_closed;
  if (!connection) return;

  pthread_mutex_lock(&connection->lock);
  already_closed = connection->is_closed;
  connection->is_closed = 1;
  connection->is_connected = 0;
  connection_signal_all_pending_closed(connection);
  pthread_cond_broadcast(&connection->connected_cond);
  pthread_mutex_unlock(&connection->lock);

  connection_drain_completed_async(connection);

  if (already_closed) return;
  channel_detach_connection(connection->channel, connection);
}

static void connection_mark_connected(channel_c_connection_t *connection, int connected) {
  pthread_mutex_lock(&connection->lock);
  connection->is_connected = connected ? 1 : 0;
  pthread_cond_broadcast(&connection->connected_cond);
  pthread_mutex_unlock(&connection->lock);
}

static int connection_wait_connected(channel_c_connection_t *connection, int timeout_ms) {
  int rc = 0;
  struct timespec deadline;

  pthread_mutex_lock(&connection->lock);
  if (connection->is_connected) {
    pthread_mutex_unlock(&connection->lock);
    return 0;
  }
  if (connection->is_closed) {
    pthread_mutex_unlock(&connection->lock);
    return -1;
  }

  clock_gettime(CLOCK_REALTIME, &deadline);
  deadline.tv_sec += timeout_ms / 1000;
  deadline.tv_nsec += (timeout_ms % 1000) * 1000000L;
  if (channel_c_internal_failpoint_hit("core.force_deadline_overflow")) {
    deadline.tv_nsec += 1000000000L;
  }
  if (deadline.tv_nsec >= 1000000000L) {
    deadline.tv_sec += 1;
    deadline.tv_nsec -= 1000000000L;
  }

  while (!connection->is_connected && !connection->is_closed) {
    if (pthread_cond_timedwait(&connection->connected_cond, &connection->lock, &deadline) == ETIMEDOUT) {
      rc = -1;
      break;
    }
  }
  if (connection->is_closed) rc = -1;
  pthread_mutex_unlock(&connection->lock);
  return rc;
}

static void connection_dispatch_event(channel_c_connection_t *connection, const char *topic, const char *body_json) {
  listener_entry_t *listener;
  listener_callback_t *callback;
  int count = 0;
  int index = 0;
  listener_callback_t **callbacks = NULL;

  if (!topic || !body_json) return;

  pthread_mutex_lock(&connection->lock);
  listener = find_listener(connection->listeners, topic);
  if (listener) {
    callback = listener->callbacks;
    while (callback) {
      count += 1;
      callback = callback->next;
    }
    if (count > 0) callbacks = (listener_callback_t **)calloc((size_t)count, sizeof(listener_callback_t *));
    callback = listener->callbacks;
    while (callback && callbacks) {
      callbacks[index++] = callback;
      callback = callback->next;
    }
  }
  pthread_mutex_unlock(&connection->lock);

  if (!callbacks) return;
  for (index = 0; index < count; index += 1) {
    callbacks[index]->callback(body_json, callbacks[index]->user_data);
  }
  free(callbacks);
}

static void connection_publish_event(channel_c_connection_t *connection, const char *topic, const char *body_json) {
  char *message;

  if (!connection || !topic || !body_json) return;

  pthread_mutex_lock(&connection->lock);
  if (connection->is_closed || !topic_set_contains(connection->remote_topics, topic)) {
    pthread_mutex_unlock(&connection->lock);
    return;
  }
  pthread_mutex_unlock(&connection->lock);

  message = json_message_event(topic, body_json);
  if (!message) return;
  (void)connection_transport_send(connection, message);
  free(message);
}

static pending_request_t *pending_acquire(channel_c_connection_t *connection, int id, pending_type_t type) {
  pending_request_t *pending = NULL;

  if (!connection) return NULL;
  if (channel_c_internal_failpoint_hit("core.pending_create_fail")) return NULL;
  if (channel_c_internal_failpoint_hit("core.pending_cond_init_fail")) return NULL;

  pthread_mutex_lock(&connection->lock);
  if (connection->pending_pool) {
    pending = connection->pending_pool;
    connection->pending_pool = pending->next;
  }
  pthread_mutex_unlock(&connection->lock);

  if (!pending) {
    pending = (pending_request_t *)calloc(1, sizeof(pending_request_t));
    if (!pending) return NULL;
    if (pthread_cond_init(&pending->cond, NULL) != 0) {
      free(pending);
      return NULL;
    }
  }

  pending->id = id;
  pending->type = type;
  pending->completed = 0;
  pending->body = NULL;
  pending->error = NULL;
  pending->topic = NULL;
  pending->stream_done = 0;
  pending->stream_head = NULL;
  pending->stream_tail = NULL;
  pending->async_callback = NULL;
  pending->async_user_data = NULL;
  pending->attached = 0;
  pending->prev = NULL;
  pending->hash_next = NULL;
  pending->next = NULL;
  return pending;
}

static int connection_next_id(channel_c_connection_t *connection) {
  return __atomic_fetch_add(&connection->next_request_id, 1, __ATOMIC_RELAXED);
}

static void *post_task_main(void *argument) {
  post_task_t *task = (post_task_t *)argument;
  channel_c_post_request_t request;
  char *response_body = NULL;
  char *error_text = NULL;
  int result = 0;

  request.path = task->path;
  request.body_json = task->body_json;
  request.context_json = task->context_json;
  request.sender = channel_c_connection_sender(task->connection);
  request.state = task->connection->state;
  request.user_data = task->handler_user_data;

  if (channel_c_internal_failpoint_hit("core.post_handler_null")) task->handler = NULL;
  if (task->handler) {
    result = task->handler(&request, &response_body, &error_text);
  } else {
    result = -1;
  }

  if (task->request_id >= 0) {
    char *message;
    if (result != 0 && !error_text) error_text = channel_c_strdup("handler failed");
    message = json_message_response(task->request_id, response_body, error_text, NULL, 0, 0);
    if (message) {
      (void)connection_transport_send(task->connection, message);
      free(message);
    }
  }

  free(response_body);
  free(error_text);
  free(task->path);
  free(task->body_json);
  free(task->context_json);
  channel_c_connection_release(task->connection);
  free(task);
  return NULL;
}

static void *stream_task_main(void *argument) {
  stream_task_t *task = (stream_task_t *)argument;
  channel_c_stream_request_t request;
  channel_c_stream_emitter_t emitter;
  char *done_message;

  emitter.connection = task->connection;
  emitter.request_id = task->request_id;
  emitter.active = task->active;

  request.path = task->path;
  request.body_json = task->body_json;
  request.context_json = task->context_json;
  request.sender = channel_c_connection_sender(task->connection);
  request.state = task->connection->state;
  request.user_data = task->handler_user_data;
  request.emitter = &emitter;

  if (task->handler) task->handler(&request);

  done_message = json_message_response(task->request_id, NULL, NULL, NULL, 1, 1);
  if (done_message) {
    (void)connection_transport_send(task->connection, done_message);
    free(done_message);
  }

  pthread_mutex_lock(&task->connection->lock);
  active_stream_remove(&task->connection->active_streams, task->request_id);
  pthread_mutex_unlock(&task->connection->lock);

  free(task->path);
  free(task->body_json);
  free(task->context_json);
  channel_c_connection_release(task->connection);
  free(task);
  return NULL;
}

int channel_c_stream_emit_json(channel_c_stream_emitter_t *emitter, const char *body_json) {
  char *message;
  if (!emitter || !body_json) return -1;

  pthread_mutex_lock(&emitter->connection->lock);
  if (!emitter->active || emitter->active->cancelled || emitter->connection->is_closed) {
    pthread_mutex_unlock(&emitter->connection->lock);
    return -1;
  }
  pthread_mutex_unlock(&emitter->connection->lock);

  message = json_message_response(emitter->request_id, body_json, NULL, NULL, 0, 0);
  if (!message) return -1;
  (void)connection_transport_send(emitter->connection, message);
  free(message);
  return 0;
}

int channel_c_stream_is_cancelled(channel_c_stream_emitter_t *emitter) {
  int cancelled;
  if (!emitter) return 1;
  pthread_mutex_lock(&emitter->connection->lock);
  cancelled = !emitter->active || emitter->active->cancelled || emitter->connection->is_closed;
  pthread_mutex_unlock(&emitter->connection->lock);
  return cancelled;
}

static void process_response_object(channel_c_connection_t *connection, yyjson_val *object) {
  yyjson_val *path_val;
  yyjson_val *stream_val;
  yyjson_val *cancel_val;
  yyjson_val *sub_val;
  yyjson_val *unsub_val;
  yyjson_val *topic_val;
  yyjson_val *id_val;
  yyjson_val *body_val;
  yyjson_val *context_val;
  yyjson_val *error_val;

  char *body_json = NULL;
  char *context_json = NULL;
  char *error_text = NULL;

  path_val = yyjson_obj_get(object, "path");
  if (yyjson_is_str(path_val)) {
    const char *path = yyjson_get_str(path_val);
    int request_id;
    post_entry_t *handler;
    int skip_post_payload_json;

    id_val = yyjson_obj_get(object, "id");
    body_val = yyjson_obj_get(object, "body");
    context_val = yyjson_obj_get(object, "context");
    error_val = yyjson_obj_get(object, "error");
    request_id = yyjson_is_int(id_val) || yyjson_is_uint(id_val) ? (int)yyjson_get_sint(id_val) : -1;
    skip_post_payload_json = 0;

    if (yyjson_is_str(error_val)) error_text = channel_c_strdup(yyjson_get_str(error_val));

    pthread_mutex_lock(&connection->channel->lock);
    skip_post_payload_json = connection->channel->skip_post_payload_json;
    handler = find_post_handler(connection->channel, path);
    pthread_mutex_unlock(&connection->channel->lock);

    if (!skip_post_payload_json) {
      if (body_val) body_json = json_write_value(body_val);
      if (context_val) context_json = json_write_value(context_val);
    }

    if (!handler) {
      if (request_id >= 0) {
        char *message = json_message_response(request_id, NULL, "api not found", NULL, 0, 0);
        if (message) {
          (void)connection_transport_send(connection, message);
          free(message);
        }
      }
      free(body_json);
      free(context_json);
      free(error_text);
      return;
    }

    if (request_id < 0) {
      post_task_t *task = channel_c_internal_failpoint_hit("core.post_task_alloc_noid_fail")
                            ? NULL
                            : (post_task_t *)calloc(1, sizeof(post_task_t));
      pthread_t thread;
      if (!task) {
        free(body_json);
        free(context_json);
        free(error_text);
        return;
      }
      connection_retain(connection);
      task->connection = connection;
      task->request_id = -1;
      task->path = channel_c_strdup(path);
      task->body_json = body_json;
      task->context_json = context_json;
      task->handler = handler->handler;
      task->handler_user_data = handler->user_data;
      if (!channel_c_internal_failpoint_hit("core.post_task_thread_noid_fail") &&
          pthread_create(&thread, NULL, post_task_main, task) == 0) {
        pthread_detach(thread);
      } else {
        channel_c_connection_release(connection);
        free(task->path);
        free(task->body_json);
        free(task->context_json);
        free(task);
      }
      free(error_text);
      return;
    }

    {
      channel_c_post_request_t request;
      channel_c_post_handler_fn post_handler = handler->handler;
      char *response_body = NULL;
      char *handler_error = NULL;
      int result = -1;

      if (channel_c_internal_failpoint_hit("core.post_task_alloc_withid_fail") ||
          channel_c_internal_failpoint_hit("core.post_task_thread_withid_fail")) {
        char *message = json_message_response(request_id, NULL, "internal error", NULL, 0, 0);
        if (message) {
          (void)connection_transport_send(connection, message);
          free(message);
        }
        free(body_json);
        free(context_json);
        free(error_text);
        return;
      }

      if (channel_c_internal_failpoint_hit("core.post_handler_null")) post_handler = NULL;

      request.path = path;
      request.body_json = body_json;
      request.context_json = context_json;
      request.sender = channel_c_connection_sender(connection);
      request.state = connection->state;
      request.user_data = handler->user_data;

      if (post_handler) result = post_handler(&request, &response_body, &handler_error);

      if (result != 0 && !handler_error) handler_error = channel_c_strdup("handler failed");
      {
        char *message = json_message_response(request_id, response_body, handler_error, NULL, 0, 0);
        if (message) {
          (void)connection_transport_send(connection, message);
          free(message);
        }
      }

      free(response_body);
      free(handler_error);
      free(body_json);
      free(context_json);
      free(error_text);
      return;
    }
  }

  id_val = yyjson_obj_get(object, "id");
  path_val = yyjson_obj_get(object, "path");
  stream_val = yyjson_obj_get(object, "stream");
  cancel_val = yyjson_obj_get(object, "cancel");
  sub_val = yyjson_obj_get(object, "sub");
  unsub_val = yyjson_obj_get(object, "unsub");
  topic_val = yyjson_obj_get(object, "topic");
  body_val = yyjson_obj_get(object, "body");
  context_val = yyjson_obj_get(object, "context");
  error_val = yyjson_obj_get(object, "error");

  if ((yyjson_is_int(id_val) || yyjson_is_uint(id_val)) &&
      !yyjson_is_str(path_val) &&
      !yyjson_is_str(stream_val) &&
      !yyjson_is_str(sub_val) &&
      !yyjson_is_str(unsub_val) &&
      !yyjson_is_str(topic_val) &&
      !(cancel_val && (yyjson_is_int(cancel_val) || yyjson_is_uint(cancel_val)))) {
    int request_id = (int)yyjson_get_sint(id_val);
    int done = 0;
    const char *error_cstr = yyjson_is_str(error_val) ? yyjson_get_str(error_val) : NULL;
    channel_c_async_result_fn async_callback = NULL;
    void *async_user_data = NULL;
    channel_c_async_status_t async_status = CHANNEL_C_ASYNC_STATUS_OK;
    char *async_body = NULL;
    char *async_error = NULL;
    yyjson_val *done_val = yyjson_obj_get(object, "done");

    if (done_val && yyjson_is_bool(done_val) && yyjson_get_bool(done_val)) done = 1;

    pthread_mutex_lock(&connection->lock);
    {
      pending_request_t *pending = pending_find_locked(connection, request_id);

      if (pending) {
        switch (pending->type) {
          case PENDING_SEND: {
            pending_detach_locked(connection, pending);
            int keep_body = (pending->stream_done & PENDING_SEND_FLAG_KEEP_BODY) != 0;
            int keep_error = (pending->stream_done & PENDING_SEND_FLAG_KEEP_ERROR) != 0;
            if (!keep_body && !keep_error) {
              if (!pending->completed) {
                pending->completed = 1;
                if (error_cstr) pending->stream_done |= PENDING_SEND_FLAG_HAS_ERROR;
                pthread_cond_broadcast(&pending->cond);
              }
            } else {
              char *send_body = NULL;
              if (keep_body && body_val) send_body = json_write_value(body_val);
              pending_complete_send(pending, send_body, error_cstr);
              free(send_body);
            }
            break;
          }
          case PENDING_SUB: {
            pending_detach_locked(connection, pending);
            char *sub_body = body_val ? json_write_value(body_val) : NULL;
            char *sub_error = yyjson_is_str(error_val) ? channel_c_strdup(error_cstr) : NULL;
            pending_complete_sub(pending, NULL, sub_body, sub_error);
            free(sub_body);
            free(sub_error);
            break;
          }
          case PENDING_STREAM: {
            char *stream_body = body_val ? json_write_value(body_val) : NULL;
            char *stream_error = yyjson_is_str(error_val) ? channel_c_strdup(error_cstr) : NULL;
            pending_push_stream(pending, stream_body, stream_error, done);
            free(stream_body);
            free(stream_error);
            break;
          }
          case PENDING_SEND_ASYNC: {
            async_callback = pending->async_callback;
            async_user_data = pending->async_user_data;
            pending_detach_locked(connection, pending);
            if (error_cstr) {
              async_status = CHANNEL_C_ASYNC_STATUS_ERROR;
              async_error = channel_c_strdup(error_cstr);
            } else if (body_val) {
              async_status = CHANNEL_C_ASYNC_STATUS_OK;
              async_body = json_write_value(body_val);
            } else {
              async_status = CHANNEL_C_ASYNC_STATUS_OK;
            }
            pending_recycle_locked(connection, pending);
            break;
          }
        }
      }
    }
    pthread_mutex_unlock(&connection->lock);
    pending_dispatch_async_result(async_callback, async_status, async_body, async_error, async_user_data);
    free(async_body);
    free(async_error);
    return;
  }

  if (yyjson_is_str(path_val)) {
    const char *path = yyjson_get_str(path_val);
    int request_id = yyjson_is_int(id_val) || yyjson_is_uint(id_val) ? (int)yyjson_get_sint(id_val) : -1;
    post_entry_t *handler;
    int skip_post_payload_json = 0;

    if (yyjson_is_str(error_val)) error_text = channel_c_strdup(yyjson_get_str(error_val));

    pthread_mutex_lock(&connection->channel->lock);
    skip_post_payload_json = connection->channel->skip_post_payload_json;
    handler = find_post_handler(connection->channel, path);
    pthread_mutex_unlock(&connection->channel->lock);

    if (!skip_post_payload_json) {
      if (body_val) body_json = json_write_value(body_val);
      if (context_val) context_json = json_write_value(context_val);
    }

    if (!handler) {
      if (request_id >= 0) {
        char *message = json_message_response(request_id, NULL, "api not found", NULL, 0, 0);
        if (message) {
          (void)connection_transport_send(connection, message);
          free(message);
        }
      }
      free(body_json);
      free(context_json);
      free(error_text);
      return;
    }

    if (request_id < 0) {
      post_task_t *task = channel_c_internal_failpoint_hit("core.post_task_alloc_noid_fail")
                            ? NULL
                            : (post_task_t *)calloc(1, sizeof(post_task_t));
      pthread_t thread;
      if (!task) {
        free(body_json);
        free(context_json);
        free(error_text);
        return;
      }
      connection_retain(connection);
      task->connection = connection;
      task->request_id = -1;
      task->path = channel_c_strdup(path);
      task->body_json = body_json;
      task->context_json = context_json;
      task->handler = handler->handler;
      task->handler_user_data = handler->user_data;
      if (!channel_c_internal_failpoint_hit("core.post_task_thread_noid_fail") &&
          pthread_create(&thread, NULL, post_task_main, task) == 0) {
        pthread_detach(thread);
      } else {
        channel_c_connection_release(connection);
        free(task->path);
        free(task->body_json);
        free(task->context_json);
        free(task);
      }
      free(error_text);
      return;
    }

    {
      channel_c_post_request_t request;
      channel_c_post_handler_fn post_handler = handler->handler;
      char *response_body = NULL;
      char *handler_error = NULL;
      int result = -1;

      if (channel_c_internal_failpoint_hit("core.post_task_alloc_withid_fail") ||
          channel_c_internal_failpoint_hit("core.post_task_thread_withid_fail")) {
        char *message = json_message_response(request_id, NULL, "internal error", NULL, 0, 0);
        if (message) {
          (void)connection_transport_send(connection, message);
          free(message);
        }
        free(body_json);
        free(context_json);
        free(error_text);
        return;
      }

      if (channel_c_internal_failpoint_hit("core.post_handler_null")) post_handler = NULL;

      request.path = path;
      request.body_json = body_json;
      request.context_json = context_json;
      request.sender = channel_c_connection_sender(connection);
      request.state = connection->state;
      request.user_data = handler->user_data;

      if (post_handler) result = post_handler(&request, &response_body, &handler_error);

      if (result != 0 && !handler_error) handler_error = channel_c_strdup("handler failed");
      {
        char *message = json_message_response(request_id, response_body, handler_error, NULL, 0, 0);
        if (message) {
          (void)connection_transport_send(connection, message);
          free(message);
        }
      }

      free(response_body);
      free(handler_error);
      free(body_json);
      free(context_json);
      free(error_text);
      return;
    }
  }

  if (yyjson_is_str(stream_val)) {
    const char *path = yyjson_get_str(stream_val);
    int request_id = yyjson_is_int(id_val) || yyjson_is_uint(id_val) ? (int)yyjson_get_sint(id_val) : -1;
    stream_entry_t *handler;

    if (body_val) body_json = json_write_value(body_val);
    if (context_val) context_json = json_write_value(context_val);
    if (yyjson_is_str(error_val)) error_text = channel_c_strdup(yyjson_get_str(error_val));

    pthread_mutex_lock(&connection->channel->lock);
    handler = find_stream_handler(connection->channel, path);
    pthread_mutex_unlock(&connection->channel->lock);

    if (!handler) {
      if (request_id >= 0) {
        char *message = json_message_response(request_id, NULL, "api not found", NULL, 0, 0);
        if (message) {
          (void)connection_transport_send(connection, message);
          free(message);
        }
      }
      free(body_json);
      free(context_json);
      free(error_text);
      return;
    }

    if (request_id < 0) {
      free(body_json);
      free(context_json);
      free(error_text);
      return;
    }

    {
      stream_task_t *task = channel_c_internal_failpoint_hit("core.stream_task_alloc_fail")
                              ? NULL
                              : (stream_task_t *)calloc(1, sizeof(stream_task_t));
      active_stream_t *active = channel_c_internal_failpoint_hit("core.stream_active_alloc_fail")
                                  ? NULL
                                  : (active_stream_t *)calloc(1, sizeof(active_stream_t));
      pthread_t thread;
      if (!task || !active) {
        char *message = json_message_response(request_id, NULL, "internal error", NULL, 0, 0);
        if (message) {
          (void)connection_transport_send(connection, message);
          free(message);
        }
        free(task);
        free(active);
        free(body_json);
        free(context_json);
        free(error_text);
        return;
      }

      active->id = request_id;
      active->cancelled = 0;

      pthread_mutex_lock(&connection->lock);
      active->next = connection->active_streams;
      connection->active_streams = active;
      pthread_mutex_unlock(&connection->lock);

      connection_retain(connection);
      task->connection = connection;
      task->request_id = request_id;
      task->path = channel_c_strdup(path);
      task->body_json = body_json;
      task->context_json = context_json;
      task->handler = handler->handler;
      task->handler_user_data = handler->user_data;
      task->active = active;

      if (!channel_c_internal_failpoint_hit("core.stream_task_thread_fail") &&
          pthread_create(&thread, NULL, stream_task_main, task) == 0) {
        pthread_detach(thread);
      } else {
        char *message = json_message_response(request_id, NULL, "internal error", NULL, 0, 0);
        if (message) {
          (void)connection_transport_send(connection, message);
          free(message);
        }
        channel_c_connection_release(connection);
        pthread_mutex_lock(&connection->lock);
        active_stream_remove(&connection->active_streams, request_id);
        pthread_mutex_unlock(&connection->lock);
        free(task->path);
        free(task->body_json);
        free(task->context_json);
        free(task);
      }
      free(error_text);
      return;
    }
  }

  if (cancel_val && (yyjson_is_int(cancel_val) || yyjson_is_uint(cancel_val))) {
    int cancel_id = (int)yyjson_get_sint(cancel_val);
    pthread_mutex_lock(&connection->lock);
    {
      active_stream_t *active = active_stream_find(connection->active_streams, cancel_id);
      if (active) active->cancelled = 1;
    }
    pthread_mutex_unlock(&connection->lock);
    free(body_json);
    free(context_json);
    free(error_text);
    return;
  }

  if (yyjson_is_str(sub_val)) {
    const char *name = yyjson_get_str(sub_val);
    int request_id = yyjson_is_int(id_val) || yyjson_is_uint(id_val) ? (int)yyjson_get_sint(id_val) : -1;
    subscription_entry_t *subscription;

    if (body_val) body_json = json_write_value(body_val);
    if (context_val) context_json = json_write_value(context_val);
    if (yyjson_is_str(error_val)) error_text = channel_c_strdup(yyjson_get_str(error_val));

    pthread_mutex_lock(&connection->channel->lock);
    subscription = find_subscription(connection->channel, name);
    pthread_mutex_unlock(&connection->channel->lock);

    if (!subscription) {
      if (request_id >= 0) {
        char *message = json_message_response(request_id, NULL, "subscription not found", NULL, 0, 0);
        if (message) {
          (void)connection_transport_send(connection, message);
          free(message);
        }
      }
      free(body_json);
      free(context_json);
      free(error_text);
      return;
    }

    if (request_id >= 0) {
      char *topic = NULL;
      char *resolved_body = NULL;
      char *message;

      if (subscription->resolver) {
        channel_c_subscription_request_t request;
        int result;

        request.name = subscription->name;
        request.request_body_json = body_json;
        request.sender = channel_c_connection_sender(connection);
        request.state = connection->state;
        request.user_data = subscription->user_data;

        result = subscription->resolver(&request, &topic, &resolved_body, &error_text);
        if (result != 0 && !error_text) error_text = channel_c_strdup("subscription failed");
      } else {
        yyjson_doc *doc = NULL;
        yyjson_val *root = NULL;
        if (!body_json) {
          topic = channel_c_strdup("");
        } else {
          doc = yyjson_read(body_json, strlen(body_json), YYJSON_READ_ALLOW_INF_AND_NAN);
          if (doc) root = yyjson_doc_get_root(doc);
          if (root && yyjson_is_str(root)) {
            topic = channel_c_strdup(yyjson_get_str(root));
          } else {
            topic = channel_c_strdup(body_json);
          }
          if (doc) yyjson_doc_free(doc);
        }
      }

      if (!topic) topic = channel_c_strdup("");

      if (!error_text) {
        size_t full_len = strlen(subscription->prefix) + 1 + strlen(topic) + 1;
        char *full_topic = channel_c_internal_failpoint_hit("core.sub_full_topic_alloc_fail")
                             ? NULL
                             : (char *)malloc(full_len);
        if (full_topic) {
          snprintf(full_topic, full_len, "%s/%s", subscription->prefix, topic);
          pthread_mutex_lock(&connection->lock);
          topic_set_add(&connection->remote_topics, full_topic);
          pthread_mutex_unlock(&connection->lock);

          message = json_message_response(request_id, resolved_body, NULL, full_topic, 0, 0);
          if (message) {
            (void)connection_transport_send(connection, message);
            free(message);
          }
          free(full_topic);
        } else {
          message = json_message_response(request_id, NULL, "internal error", NULL, 0, 0);
          if (message) {
            (void)connection_transport_send(connection, message);
            free(message);
          }
        }
      } else {
        message = json_message_response(request_id, NULL, error_text, NULL, 0, 0);
        if (message) {
          (void)connection_transport_send(connection, message);
          free(message);
        }
      }

      free(topic);
      free(resolved_body);
    }

    free(body_json);
    free(context_json);
    free(error_text);
    return;
  }

  if (yyjson_is_str(unsub_val)) {
    const char *topic = yyjson_get_str(unsub_val);
    pthread_mutex_lock(&connection->lock);
    topic_set_remove(&connection->remote_topics, topic);
    pthread_mutex_unlock(&connection->lock);

    free(body_json);
    free(context_json);
    free(error_text);
    return;
  }

  if (yyjson_is_str(topic_val)) {
    const char *topic = yyjson_get_str(topic_val);
    int request_id = yyjson_is_int(id_val) || yyjson_is_uint(id_val) ? (int)yyjson_get_sint(id_val) : -1;

    if (body_val) body_json = json_write_value(body_val);
    if (yyjson_is_str(error_val)) error_text = channel_c_strdup(yyjson_get_str(error_val));

    if (request_id >= 0) {
      pthread_mutex_lock(&connection->lock);
      {
        pending_request_t *pending = pending_find_locked(connection, request_id);
        if (pending && pending->type == PENDING_SUB) {
          pending_detach_locked(connection, pending);
          pending_complete_sub(pending, topic, body_json, error_text);
        }
      }
      pthread_mutex_unlock(&connection->lock);
    }

    if (body_json) {
      connection_dispatch_event(connection, topic, body_json);
    }

    free(body_json);
    free(context_json);
    free(error_text);
    return;
  }

  if (id_val && (yyjson_is_int(id_val) || yyjson_is_uint(id_val))) {
    int request_id = (int)yyjson_get_sint(id_val);
    int done = 0;
    channel_c_async_result_fn async_callback = NULL;
    void *async_user_data = NULL;
    channel_c_async_status_t async_status = CHANNEL_C_ASYNC_STATUS_OK;
    char *async_body = NULL;
    char *async_error = NULL;
    yyjson_val *done_val = yyjson_obj_get(object, "done");
    if (done_val && yyjson_is_bool(done_val) && yyjson_get_bool(done_val)) done = 1;

    pthread_mutex_lock(&connection->lock);
    {
      pending_request_t *pending = pending_find_locked(connection, request_id);
      if (pending) {
        switch (pending->type) {
          case PENDING_SEND:
            pending_detach_locked(connection, pending);
            pending_complete_send(pending, body_json, error_text);
            break;
          case PENDING_SUB:
            pending_detach_locked(connection, pending);
            pending_complete_sub(pending, NULL, body_json, error_text);
            break;
          case PENDING_STREAM:
            pending_push_stream(pending, body_json, error_text, done);
            break;
          case PENDING_SEND_ASYNC:
            async_callback = pending->async_callback;
            async_user_data = pending->async_user_data;
            pending_detach_locked(connection, pending);
            if (error_text) {
              async_status = CHANNEL_C_ASYNC_STATUS_ERROR;
              async_error = channel_c_strdup(error_text);
            } else if (body_json) {
              async_status = CHANNEL_C_ASYNC_STATUS_OK;
              async_body = channel_c_strdup(body_json);
            } else {
              async_status = CHANNEL_C_ASYNC_STATUS_OK;
            }
            pending_recycle_locked(connection, pending);
            break;
        }
      }
    }
    pthread_mutex_unlock(&connection->lock);
    pending_dispatch_async_result(async_callback, async_status, async_body, async_error, async_user_data);
    free(async_body);
    free(async_error);

    free(body_json);
    free(context_json);
    free(error_text);
    return;
  }

  free(body_json);
  free(context_json);
  free(error_text);
}

void channel_c_connection_receive_text(channel_c_connection_t *connection, const char *text) {
  yyjson_doc *doc;
  yyjson_val *root;
  yyjson_val *item;
  size_t index;
  size_t max;

  if (!connection || !text) return;

  doc = yyjson_read(text, strlen(text), YYJSON_READ_ALLOW_INF_AND_NAN);
  if (!doc) {
    channel_c_connection_on_close(connection);
    return;
  }

  root = yyjson_doc_get_root(doc);
  if (yyjson_is_arr(root)) {
    yyjson_arr_foreach(root, index, max, item) {
      if (yyjson_is_obj(item)) process_response_object(connection, item);
    }
  } else if (yyjson_is_obj(root)) {
    process_response_object(connection, root);
  }

  yyjson_doc_free(doc);
}

channel_c_channel_t *channel_c_channel_new(void) {
  if (channel_c_internal_failpoint_hit("core.channel_new_null")) return NULL;
  channel_c_channel_t *channel = (channel_c_channel_t *)calloc(1, sizeof(channel_c_channel_t));
  if (!channel) return NULL;
  if (channel_c_internal_failpoint_hit("core.channel_mutex_init_fail") || pthread_mutex_init(&channel->lock, NULL) != 0) {
    free(channel);
    return NULL;
  }
  channel->skip_post_payload_json = 0;
  return channel;
}

void channel_c_channel_free(channel_c_channel_t *channel) {
  post_entry_t *post;
  stream_entry_t *stream;
  subscription_entry_t *subscription;
  channel_link_t *link;

  if (!channel) return;

  post = channel->posts;
  while (post) {
    post_entry_t *next = post->next;
    free(post->path);
    free(post);
    post = next;
  }

  stream = channel->streams;
  while (stream) {
    stream_entry_t *next = stream->next;
    free(stream->path);
    free(stream);
    stream = next;
  }

  subscription = channel->subscriptions;
  while (subscription) {
    subscription_entry_t *next = subscription->next;
    free(subscription->name);
    free(subscription->prefix);
    free(subscription);
    subscription = next;
  }

  pthread_mutex_lock(&channel->lock);
  link = channel->connections;
  channel->connections = NULL;
  pthread_mutex_unlock(&channel->lock);

  while (link) {
    channel_link_t *next = link->next;
    channel_c_connection_on_close(link->connection);
    channel_c_connection_release(link->connection);
    free(link);
    link = next;
  }

  pthread_mutex_destroy(&channel->lock);
  free(channel);
}

int channel_c_channel_post(
  channel_c_channel_t *channel,
  const char *path,
  channel_c_post_handler_fn handler,
  void *user_data
) {
  post_entry_t *entry;
  if (!channel || !path || !handler) return -1;

  entry = (post_entry_t *)calloc(1, sizeof(post_entry_t));
  if (!entry) return -1;
  if (!channel_c_internal_failpoint_hit("core.channel_post_path_strdup_fail")) {
    entry->path = channel_c_strdup(path);
  }
  entry->handler = handler;
  entry->user_data = user_data;
  if (!entry->path) {
    free(entry);
    return -1;
  }

  pthread_mutex_lock(&channel->lock);
  entry->next = channel->posts;
  channel->posts = entry;
  pthread_mutex_unlock(&channel->lock);
  return 0;
}

void channel_c_channel_set_skip_post_payload_json(channel_c_channel_t *channel, int skip) {
  if (!channel) return;
  pthread_mutex_lock(&channel->lock);
  channel->skip_post_payload_json = skip ? 1 : 0;
  pthread_mutex_unlock(&channel->lock);
}

int channel_c_channel_stream(
  channel_c_channel_t *channel,
  const char *path,
  channel_c_stream_handler_fn handler,
  void *user_data
) {
  stream_entry_t *entry;
  if (!channel || !path || !handler) return -1;

  entry = (stream_entry_t *)calloc(1, sizeof(stream_entry_t));
  if (!entry) return -1;
  if (!channel_c_internal_failpoint_hit("core.channel_stream_path_strdup_fail")) {
    entry->path = channel_c_strdup(path);
  }
  entry->handler = handler;
  entry->user_data = user_data;
  if (!entry->path) {
    free(entry);
    return -1;
  }

  pthread_mutex_lock(&channel->lock);
  entry->next = channel->streams;
  channel->streams = entry;
  pthread_mutex_unlock(&channel->lock);
  return 0;
}

int channel_c_channel_subscription(
  channel_c_channel_t *channel,
  const char *name,
  channel_c_subscription_resolver_fn resolver,
  void *user_data
) {
  subscription_entry_t *entry;
  if (!channel || !name) return -1;

  entry = (subscription_entry_t *)calloc(1, sizeof(subscription_entry_t));
  if (!entry) return -1;
  if (!channel_c_internal_failpoint_hit("core.channel_subscription_name_strdup_fail")) {
    entry->name = channel_c_strdup(name);
  }
  if (!channel_c_internal_failpoint_hit("core.channel_subscription_prefix_strdup_fail")) {
    entry->prefix = channel_c_strdup(name);
  }
  entry->resolver = resolver;
  entry->user_data = user_data;

  if (!entry->name || !entry->prefix) {
    free(entry->name);
    free(entry->prefix);
    free(entry);
    return -1;
  }

  pthread_mutex_lock(&channel->lock);
  entry->next = channel->subscriptions;
  channel->subscriptions = entry;
  pthread_mutex_unlock(&channel->lock);
  return 0;
}

static char *join_prefix(const char *prefix, const char *name) {
  size_t length;
  char *result;

  if (!has_prefix(prefix)) return channel_c_strdup(name);
  if (channel_c_internal_failpoint_hit("core.join_prefix_alloc_fail")) return NULL;
  length = strlen(prefix) + 1 + strlen(name) + 1;
  result = (char *)malloc(length);
  if (!result) return NULL;
  snprintf(result, length, "%s/%s", prefix, name);
  return result;
}

int channel_c_channel_merge(channel_c_channel_t *destination, channel_c_channel_t *source, const char *prefix) {
  post_entry_t *post;
  stream_entry_t *stream;
  subscription_entry_t *subscription;

  if (!destination || !source) return -1;

  pthread_mutex_lock(&source->lock);

  post = source->posts;
  while (post) {
    char *path = join_prefix(prefix, post->path);
    if (!path || channel_c_channel_post(destination, path, post->handler, post->user_data) != 0) {
      free(path);
      pthread_mutex_unlock(&source->lock);
      return -1;
    }
    free(path);
    post = post->next;
  }

  stream = source->streams;
  while (stream) {
    char *path = join_prefix(prefix, stream->path);
    if (!path || channel_c_channel_stream(destination, path, stream->handler, stream->user_data) != 0) {
      free(path);
      pthread_mutex_unlock(&source->lock);
      return -1;
    }
    free(path);
    stream = stream->next;
  }

  subscription = source->subscriptions;
  while (subscription) {
    char *name = join_prefix(prefix, subscription->name);
    if (!name || channel_c_channel_subscription(destination, name, subscription->resolver, subscription->user_data) != 0) {
      free(name);
      pthread_mutex_unlock(&source->lock);
      return -1;
    }
    free(name);
    subscription = subscription->next;
  }

  pthread_mutex_unlock(&source->lock);
  return 0;
}

int channel_c_channel_event_send(
  channel_c_channel_t *channel,
  const char *subscription_name,
  const char *request_body_json,
  const char *body_override_json,
  char **out_error
) {
  subscription_entry_t *subscription;
  char *topic = NULL;
  char *body = NULL;
  channel_link_t *link;
  channel_c_connection_t **connections = NULL;
  size_t count = 0;
  size_t index = 0;

  if (!channel || !subscription_name) {
    if (out_error) *out_error = channel_c_strdup("invalid arguments");
    return -1;
  }

  pthread_mutex_lock(&channel->lock);
  subscription = find_subscription(channel, subscription_name);
  if (!subscription) {
    pthread_mutex_unlock(&channel->lock);
    if (out_error) *out_error = channel_c_strdup("subscription not found");
    return -1;
  }

  if (subscription->resolver) {
    channel_c_subscription_request_t request;
    request.name = subscription->name;
    request.request_body_json = request_body_json;
    request.sender = NULL;
    request.state = NULL;
    request.user_data = subscription->user_data;

    if (subscription->resolver(&request, &topic, &body, out_error) != 0) {
      pthread_mutex_unlock(&channel->lock);
      free(topic);
      free(body);
      return -1;
    }
  } else {
    yyjson_doc *doc = NULL;
    yyjson_val *root = NULL;
    if (!request_body_json) {
      topic = channel_c_strdup("");
    } else {
      doc = yyjson_read(request_body_json, strlen(request_body_json), YYJSON_READ_ALLOW_INF_AND_NAN);
      if (doc) root = yyjson_doc_get_root(doc);
      if (root && yyjson_is_str(root)) {
        topic = channel_c_strdup(yyjson_get_str(root));
      } else {
        topic = channel_c_strdup(request_body_json);
      }
      if (doc) yyjson_doc_free(doc);
    }
  }

  if (!topic) topic = channel_c_strdup("");

  if (body_override_json) {
    body = channel_c_strdup(body_override_json);
  }

  if (!body) {
    pthread_mutex_unlock(&channel->lock);
    free(topic);
    if (out_error) *out_error = channel_c_strdup("body is required");
    return -1;
  }

  link = channel->connections;
  while (link) {
    count += 1;
    link = link->next;
  }

  if (count > 0) connections = (channel_c_connection_t **)calloc(count, sizeof(channel_c_connection_t *));
  link = channel->connections;
  while (link && connections) {
    connections[index++] = link->connection;
    connection_retain(link->connection);
    link = link->next;
  }
  pthread_mutex_unlock(&channel->lock);

  if (connections) {
    size_t i;
    size_t full_length = strlen(subscription->prefix) + 1 + strlen(topic) + 1;
    char *full_topic = channel_c_internal_failpoint_hit("core.event_full_topic_alloc_fail")
                         ? NULL
                         : (char *)malloc(full_length);
    if (!full_topic) {
      for (i = 0; i < count; i += 1) channel_c_connection_release(connections[i]);
      free(connections);
      free(topic);
      free(body);
      if (out_error) *out_error = channel_c_strdup("internal error");
      return -1;
    }

    snprintf(full_topic, full_length, "%s/%s", subscription->prefix, topic);
    for (i = 0; i < count; i += 1) {
      connection_publish_event(connections[i], full_topic, body);
      channel_c_connection_release(connections[i]);
    }

    free(full_topic);
    free(connections);
  }

  free(topic);
  free(body);
  return 0;
}

static int sender_send_common(
  channel_c_sender_t *sender,
  const char *field,
  const char *name,
  const char *body_json,
  const char *context_json,
  pending_type_t type,
  int send_keep_body,
  int send_keep_error,
  channel_c_async_result_fn async_callback,
  void *async_user_data,
  pending_request_t **out_pending,
  int *out_request_id,
  char **out_error
) {
  channel_c_connection_t *connection;
  int request_id;
  pending_request_t *pending;
  char *message;

  if (!sender || !field || !name) {
    if (out_error) *out_error = channel_c_strdup("invalid arguments");
    return -1;
  }

  connection = sender->connection;
  if (!connection) {
    if (out_error) *out_error = channel_c_strdup("sender is not attached");
    return -1;
  }

  if (channel_c_internal_failpoint_hit("core.connection_unavailable") || connection_wait_connected(connection, 3000) != 0) {
    if (out_error) *out_error = channel_c_strdup("connection is not available");
    return -1;
  }

  request_id = connection_next_id(connection);
  pending = pending_acquire(connection, request_id, type);
  if (!pending) {
    if (out_error) *out_error = channel_c_strdup("internal error");
    return -1;
  }

  if (type == PENDING_SEND) {
    if (send_keep_body) pending->stream_done |= PENDING_SEND_FLAG_KEEP_BODY;
    if (send_keep_error) pending->stream_done |= PENDING_SEND_FLAG_KEEP_ERROR;
  } else if (type == PENDING_SEND_ASYNC) {
    pending->async_callback = async_callback;
    pending->async_user_data = async_user_data;
  }

  if (channel_c_internal_failpoint_hit("core.request_json_fail")) {
    message = NULL;
  } else if (!body_json && !context_json) {
    message = json_message_request_fast(request_id, field, name);
    if (!message) message = json_message_request(request_id, field, name, body_json, context_json);
  } else {
    message = json_message_request(request_id, field, name, body_json, context_json);
  }
  if (!message) {
    pending_recycle(connection, pending);
    if (out_error) *out_error = channel_c_strdup("invalid json body or context");
    return -1;
  }

  pthread_mutex_lock(&connection->lock);
  pending_attach_locked(connection, pending);
  pthread_mutex_unlock(&connection->lock);

  if (connection_transport_send(connection, message) != 0) {
    pthread_mutex_lock(&connection->lock);
    pending_remove(connection, request_id);
    pthread_mutex_unlock(&connection->lock);
    free(message);
    if (out_error) *out_error = channel_c_strdup("failed to send request");
    return -1;
  }

  free(message);
  if (out_pending) *out_pending = pending;
  if (out_request_id) *out_request_id = request_id;
  return 0;
}

static int sender_is_process_fast_path_enabled(channel_c_sender_t *sender) {
  channel_c_connection_t *connection;
  int enabled = 0;

  if (!sender) return 0;
  connection = sender->connection;
  if (!connection) return 0;

  pthread_mutex_lock(&connection->lock);
  enabled = connection->process_fast_path_enabled;
  pthread_mutex_unlock(&connection->lock);
  return enabled;
}

static int sender_send_process_fast(
  channel_c_sender_t *sender,
  const char *path,
  const char *body_json,
  const char *context_json,
  char **out_body_json,
  char **out_error
) {
  channel_c_connection_t *connection;
  post_entry_t *handler_entry;
  channel_c_post_handler_fn handler;
  void *handler_user_data;
  channel_c_post_request_t request;
  char *response_body = NULL;
  char *error_text = NULL;
  int unavailable;
  int result;

  if (out_body_json) *out_body_json = NULL;
  if (out_error) *out_error = NULL;

  if (!sender || !path) {
    if (out_error) *out_error = channel_c_strdup("invalid arguments");
    return -1;
  }

  connection = sender->connection;
  if (!connection) {
    if (out_error) *out_error = channel_c_strdup("sender is not attached");
    return -1;
  }

  pthread_mutex_lock(&connection->lock);
  unavailable = connection->is_closed || !connection->is_connected;
  pthread_mutex_unlock(&connection->lock);
  if (unavailable) {
    if (out_error) *out_error = channel_c_strdup("connection is not available");
    return -1;
  }

  pthread_mutex_lock(&connection->channel->lock);
  handler_entry = find_post_handler(connection->channel, path);
  if (!handler_entry) {
    pthread_mutex_unlock(&connection->channel->lock);
    if (out_error) *out_error = channel_c_strdup("api not found");
    return -1;
  }
  handler = handler_entry->handler;
  handler_user_data = handler_entry->user_data;
  pthread_mutex_unlock(&connection->channel->lock);

  request.path = path;
  request.body_json = body_json;
  request.context_json = context_json;
  request.sender = sender;
  request.state = connection->state;
  request.user_data = handler_user_data;

  result = handler ? handler(&request, &response_body, &error_text) : -1;
  if (result != 0 && !error_text) {
    error_text = channel_c_strdup("handler failed");
  }

  if (result != 0 || error_text) {
    free(response_body);
    if (out_error) {
      *out_error = error_text;
      error_text = NULL;
    }
    free(error_text);
    return -1;
  }

  if (out_body_json) {
    *out_body_json = response_body;
    response_body = NULL;
  }
  free(response_body);
  free(error_text);
  return 0;
}

int channel_c_sender_send(
  channel_c_sender_t *sender,
  const char *path,
  const char *body_json,
  const char *context_json,
  char **out_body_json,
  char **out_error
) {
  channel_c_connection_t *connection;
  pending_request_t *pending = NULL;
  int request_id = -1;
  int result;

  if (out_body_json) *out_body_json = NULL;
  if (out_error) *out_error = NULL;

  if (sender_is_process_fast_path_enabled(sender)) {
    return sender_send_process_fast(sender, path, body_json, context_json, out_body_json, out_error);
  }

  result = sender_send_common(
    sender,
    "path",
    path,
    body_json,
    context_json,
    PENDING_SEND,
    out_body_json != NULL,
    out_error != NULL,
    NULL,
    NULL,
    &pending,
    &request_id,
    out_error
  );
  if (result != 0) return result;

  connection = sender->connection;
  pthread_mutex_lock(&connection->lock);
  if (pending_wait(connection, pending) != 0) {
    if (out_error) *out_error = channel_c_strdup("connection closed");
    pending_finalize_locked(connection, pending);
    pthread_mutex_unlock(&connection->lock);
    return -1;
  }

  if (pending->error || (pending->stream_done & PENDING_SEND_FLAG_HAS_ERROR)) {
    if (out_error) {
      if (pending->error) *out_error = channel_c_strdup(pending->error);
      else *out_error = channel_c_strdup("request failed");
    }
    pending_finalize_locked(connection, pending);
    pthread_mutex_unlock(&connection->lock);
    return -1;
  }

  if (out_body_json && pending->body) *out_body_json = channel_c_strdup(pending->body);
  pending_finalize_locked(connection, pending);
  pthread_mutex_unlock(&connection->lock);
  return 0;
}

int channel_c_sender_send_async(
  channel_c_sender_t *sender,
  const char *path,
  const char *body_json,
  const char *context_json,
  channel_c_async_result_fn callback,
  void *user_data,
  char **out_error
) {
  if (out_error) *out_error = NULL;
  if (!callback) {
    if (out_error) *out_error = channel_c_strdup("invalid arguments");
    return -1;
  }

  return sender_send_common(
    sender,
    "path",
    path,
    body_json,
    context_json,
    PENDING_SEND_ASYNC,
    0,
    0,
    callback,
    user_data,
    NULL,
    NULL,
    out_error
  );
}

channel_c_values_t *channel_c_sender_values(
  channel_c_sender_t *sender,
  const char *path,
  const char *body_json,
  const char *context_json,
  char **out_error
) {
  channel_c_values_t *values;
  pending_request_t *pending = NULL;
  int request_id = -1;

  if (out_error) *out_error = NULL;

  if (sender_send_common(
        sender,
        "stream",
        path,
        body_json,
        context_json,
        PENDING_STREAM,
        0,
        0,
        NULL,
        NULL,
        &pending,
        &request_id,
        out_error
      ) != 0) {
    return NULL;
  }

  (void)pending;

  values = channel_c_internal_failpoint_hit("core.values_alloc_fail")
             ? NULL
             : (channel_c_values_t *)calloc(1, sizeof(channel_c_values_t));
  if (!values) {
    char *cancel = json_message_cancel(request_id);
    if (cancel) {
      (void)connection_transport_send(sender->connection, cancel);
      free(cancel);
    }
    pthread_mutex_lock(&sender->connection->lock);
    pending_remove(sender->connection, request_id);
    pthread_mutex_unlock(&sender->connection->lock);
    if (out_error) *out_error = channel_c_strdup("internal error");
    return NULL;
  }

  values->connection = sender->connection;
  values->request_id = request_id;
  values->closed = 0;
  connection_retain(values->connection);
  return values;
}

int channel_c_values_next(channel_c_values_t *values, char **out_body_json, int *out_done, char **out_error) {
  channel_c_connection_t *connection;
  pending_request_t *pending;
  stream_queue_item_t *item;

  if (out_body_json) *out_body_json = NULL;
  if (out_done) *out_done = 0;
  if (out_error) *out_error = NULL;

  if (!values || values->closed) {
    if (out_done) *out_done = 1;
    return 0;
  }

  connection = values->connection;

  pthread_mutex_lock(&connection->lock);
  pending = pending_find_locked(connection, values->request_id);
  if (!pending || pending->type != PENDING_STREAM) {
    pthread_mutex_unlock(&connection->lock);
    if (out_done) *out_done = 1;
    values->closed = 1;
    return 0;
  }

  while (!connection->is_closed && pending->stream_head == NULL && !pending->stream_done) {
    pthread_cond_wait(&pending->cond, &connection->lock);
  }

  if (connection->is_closed) {
    pthread_mutex_unlock(&connection->lock);
    if (out_error) *out_error = channel_c_strdup("connection closed");
    return -1;
  }

  item = pending_shift_stream(pending);
  if (!item) {
    if (pending->stream_done) {
      values->closed = 1;
      pending_remove(connection, values->request_id);
      pthread_mutex_unlock(&connection->lock);
      if (out_done) *out_done = 1;
      return 0;
    }
    pthread_mutex_unlock(&connection->lock); /* LCOV_EXCL_LINE */
    return -1;                               /* LCOV_EXCL_LINE */
  }

  if (item->error) {
    if (out_error) *out_error = channel_c_strdup(item->error);
    free_stream_queue(item);
    values->closed = 1;
    pending_remove(connection, values->request_id);
    pthread_mutex_unlock(&connection->lock);
    return -1;
  }

  if (item->done) {
    values->closed = 1;
    pending_remove(connection, values->request_id);
    pthread_mutex_unlock(&connection->lock);
    if (out_done) *out_done = 1;
    free(item->body);
    free(item->error);
    free(item);
    return 0;
  }

  if (out_body_json && item->body) *out_body_json = channel_c_strdup(item->body);
  pthread_mutex_unlock(&connection->lock);

  free(item->body);
  free(item->error);
  free(item);
  if (out_done) *out_done = 0;
  return 0;
}

void channel_c_values_close(channel_c_values_t *values) {
  channel_c_connection_t *connection;
  char *cancel;

  if (!values) return;
  connection = values->connection;

  if (!values->closed) {
    cancel = json_message_cancel(values->request_id);
    if (cancel) {
      (void)connection_transport_send(connection, cancel);
      free(cancel);
    }
    pthread_mutex_lock(&connection->lock);
    pending_remove(connection, values->request_id);
    pthread_mutex_unlock(&connection->lock);
    values->closed = 1;
  }

  channel_c_connection_release(connection);
  free(values);
}

int channel_c_sender_subscribe(
  channel_c_sender_t *sender,
  const char *subscription_name,
  const char *body_json,
  const char *context_json,
  channel_c_event_callback_fn callback,
  void *user_data,
  channel_c_subscription_handle_t **out_handle,
  char **out_error
) {
  channel_c_connection_t *connection;
  pending_request_t *pending = NULL;
  int request_id = -1;
  listener_entry_t *listener;
  listener_callback_t *callback_entry;
  channel_c_subscription_handle_t *handle;

  if (out_handle) *out_handle = NULL;
  if (out_error) *out_error = NULL;

  if (!sender || !callback) {
    if (out_error) *out_error = channel_c_strdup("invalid arguments");
    return -1;
  }

  if (sender_send_common(
        sender,
        "sub",
        subscription_name,
        body_json,
        context_json,
        PENDING_SUB,
        0,
        0,
        NULL,
        NULL,
        &pending,
        &request_id,
        out_error
      ) != 0) {
    return -1;
  }

  connection = sender->connection;

  pthread_mutex_lock(&connection->lock);
  if (pending_wait(connection, pending) != 0) {
    if (out_error) *out_error = channel_c_strdup("connection closed");
    pending_finalize_locked(connection, pending);
    pthread_mutex_unlock(&connection->lock);
    return -1;
  }

  if (channel_c_internal_failpoint_hit("core.subscribe_missing_topic")) {
    free(pending->topic);
    pending->topic = NULL;
  }

  if (pending->error || !pending->topic) {
    if (out_error) {
      if (pending->error) *out_error = channel_c_strdup(pending->error);
      else *out_error = channel_c_strdup("subscription failed");
    }
    pending_finalize_locked(connection, pending);
    pthread_mutex_unlock(&connection->lock);
    return -1;
  }

  listener = find_listener(connection->listeners, pending->topic);
  if (!listener) {
    listener = channel_c_internal_failpoint_hit("core.listener_alloc_fail")
                 ? NULL
                 : (listener_entry_t *)calloc(1, sizeof(listener_entry_t));
    if (!listener) {
      if (out_error) *out_error = channel_c_strdup("internal error");
      pending_finalize_locked(connection, pending);
      pthread_mutex_unlock(&connection->lock);
      return -1;
    }
    if (!channel_c_internal_failpoint_hit("core.listener_topic_strdup_fail")) {
      listener->topic = channel_c_strdup(pending->topic);
    }
    if (!listener->topic) {
      if (out_error) *out_error = channel_c_strdup("internal error");
      free(listener);
      pending_finalize_locked(connection, pending);
      pthread_mutex_unlock(&connection->lock);
      return -1;
    }
    listener->next = connection->listeners;
    connection->listeners = listener;
  }

  callback_entry = channel_c_internal_failpoint_hit("core.callback_entry_alloc_fail")
                     ? NULL
                     : (listener_callback_t *)calloc(1, sizeof(listener_callback_t));
  if (!callback_entry) {
    if (out_error) *out_error = channel_c_strdup("internal error");
    pending_finalize_locked(connection, pending);
    pthread_mutex_unlock(&connection->lock);
    return -1;
  }

  callback_entry->id = connection->next_callback_id++;
  callback_entry->callback = callback;
  callback_entry->user_data = user_data;
  callback_entry->next = listener->callbacks;
  listener->callbacks = callback_entry;

  handle = channel_c_internal_failpoint_hit("core.subscription_handle_alloc_fail")
             ? NULL
             : (channel_c_subscription_handle_t *)calloc(1, sizeof(channel_c_subscription_handle_t));
  if (!handle) {
    listener->callbacks = callback_entry->next;
    free(callback_entry);
    if (out_error) *out_error = channel_c_strdup("internal error");
    pending_finalize_locked(connection, pending);
    pthread_mutex_unlock(&connection->lock);
    return -1;
  }

  handle->connection = connection;
  handle->topic = channel_c_strdup(pending->topic);
  handle->callback_id = callback_entry->id;
  handle->cancelled = 0;
  connection->ref_count += 1;

  pending_finalize_locked(connection, pending);
  pthread_mutex_unlock(&connection->lock);

  if (out_handle) *out_handle = handle;
  return 0;
}

void channel_c_subscription_cancel(channel_c_subscription_handle_t *handle) {
  channel_c_connection_t *connection;
  listener_entry_t *listener;
  listener_callback_t *previous;
  listener_callback_t *current;
  int has_callbacks;

  if (!handle || handle->cancelled) return;
  handle->cancelled = 1;

  connection = handle->connection;
  pthread_mutex_lock(&connection->lock);

  listener = find_listener(connection->listeners, handle->topic);
  if (listener) {
    previous = NULL;
    current = listener->callbacks;
    while (current) {
      if (current->id == handle->callback_id) {
        if (previous) {
          previous->next = current->next;
        } else {
          listener->callbacks = current->next;
        }
        free(current);
        break;
      }
      previous = current;
      current = current->next;
    }

    has_callbacks = listener->callbacks != NULL;
    if (!has_callbacks) {
      char *message = json_message_unsubscribe(handle->topic);
      listener_entry_t *listener_previous = NULL;
      listener_entry_t *listener_current = connection->listeners;
      while (listener_current) {
        if (listener_current == listener) {
          if (listener_previous) {
            listener_previous->next = listener_current->next;
          } else {
            connection->listeners = listener_current->next;
          }
          free(listener_current->topic);
          free(listener_current);
          break;
        }
        listener_previous = listener_current;
        listener_current = listener_current->next;
      }
      pthread_mutex_unlock(&connection->lock);
      if (message) {
        (void)connection_transport_send(connection, message);
        free(message);
      }
      free(handle->topic);
      channel_c_connection_release(connection);
      free(handle);
      return;
    }
  }

  pthread_mutex_unlock(&connection->lock);
  free(handle->topic);
  channel_c_connection_release(connection);
  free(handle);
}

void channel_c_sender_stop(channel_c_sender_t *sender) {
  if (!sender || !sender->connection) return;
  pthread_mutex_lock(&sender->connection->lock);
  if (sender->connection->transport_ops.stop) {
    sender->connection->transport_ops.stop(sender->connection->transport_ctx);
  }
  pthread_mutex_unlock(&sender->connection->lock);
}

static void set_pending_error_locked(pending_request_t *pending, const char *error) {
  if (!pending->completed && pending->type != PENDING_STREAM) {
    pending->completed = 1;
    if (pending->type == PENDING_SEND_ASYNC) {
      pending->stream_done |= PENDING_SEND_FLAG_ASYNC_CLOSED;
    }
    if (!pending->error) pending->error = channel_c_strdup(error);
    pthread_cond_broadcast(&pending->cond);
  }
}

/* transport helper hooks for websocket/process layers */
void channel_c_internal_connection_mark_connected(channel_c_connection_t *connection, int connected) {
  connection_mark_connected(connection, connected);
}

void channel_c_internal_connection_set_process_fast_path(channel_c_connection_t *connection, int enabled) {
  if (!connection) return;
  pthread_mutex_lock(&connection->lock);
  connection->process_fast_path_enabled = enabled ? 1 : 0;
  pthread_mutex_unlock(&connection->lock);
}

void channel_c_internal_connection_mark_failed(channel_c_connection_t *connection, const char *error_text) {
  pending_request_t *pending;
  pthread_mutex_lock(&connection->lock);
  pending = connection->pending;
  while (pending) {
    set_pending_error_locked(pending, error_text ? error_text : "connection failed");
    pending = pending->next;
  }
  pthread_mutex_unlock(&connection->lock);
  connection_drain_completed_async(connection);
}
