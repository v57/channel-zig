#ifndef CHANNEL_C_INTERNAL_H
#define CHANNEL_C_INTERNAL_H

#include <pthread.h>

#include "channel-c/channel_c.h"

typedef struct channel_c_connection channel_c_connection_t;

typedef struct {
  int (*send_text)(void *ctx, const char *text);
  void (*stop)(void *ctx);
} channel_c_transport_ops_t;

channel_c_connection_t *channel_c_connection_create(
  channel_c_channel_t *channel,
  void *state,
  channel_c_state_destroy_fn state_destroy,
  void *state_destroy_user_data,
  void *transport_ctx,
  channel_c_transport_ops_t transport_ops
);

void channel_c_connection_set_transport(
  channel_c_connection_t *connection,
  void *transport_ctx,
  channel_c_transport_ops_t transport_ops
);

void channel_c_connection_receive_text(channel_c_connection_t *connection, const char *text);
void channel_c_connection_on_close(channel_c_connection_t *connection);
void channel_c_connection_release(channel_c_connection_t *connection);
channel_c_sender_t *channel_c_connection_sender(channel_c_connection_t *connection);
int channel_c_connection_is_connected(channel_c_connection_t *connection);
int channel_c_connection_is_remote_subscribed(channel_c_connection_t *connection, const char *topic);

void channel_c_internal_connection_mark_connected(channel_c_connection_t *connection, int connected);
void channel_c_internal_connection_mark_failed(channel_c_connection_t *connection, const char *error_text);
void channel_c_internal_connection_set_process_fast_path(channel_c_connection_t *connection, int enabled);

void channel_c_internal_failpoint_reset(void);
void channel_c_internal_failpoint_set(const char *name, int count);
int channel_c_internal_failpoint_hit(const char *name);

channel_c_connection_t *channel_c_internal_client_connection(channel_c_client_t *client);
channel_c_connection_t *channel_c_internal_server_first_connection(channel_c_server_t *server);
channel_c_connection_t *channel_c_internal_process_connection(channel_c_process_t *process);
int channel_c_internal_process_send(channel_c_process_t *process, const char *text);

#endif
