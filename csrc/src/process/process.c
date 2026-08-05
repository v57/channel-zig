#include <pthread.h>
#include <stdlib.h>

#include "internal.h"

struct channel_c_process {
  pthread_mutex_t lock;
  int stopped;
  channel_c_connection_t *connection;
};

static int process_send_text(void *ctx, const char *text) {
  struct channel_c_process *process = (struct channel_c_process *)ctx;
  if (!process || !text) return -1;

  pthread_mutex_lock(&process->lock);
  if (process->stopped || !process->connection) {
    pthread_mutex_unlock(&process->lock);
    return -1;
  }
  pthread_mutex_unlock(&process->lock);

  channel_c_connection_receive_text(process->connection, text);
  return 0;
}

static void process_stop_transport(void *ctx) {
  struct channel_c_process *process = (struct channel_c_process *)ctx;
  if (!process) return;

  pthread_mutex_lock(&process->lock);
  if (!process->stopped) {
    process->stopped = 1;
    if (process->connection) channel_c_connection_on_close(process->connection);
  }
  pthread_mutex_unlock(&process->lock);
}

channel_c_process_t *channel_c_process_new(channel_c_channel_t *channel, void *state, channel_c_state_destroy_fn state_destroy) {
  struct channel_c_process *process;
  channel_c_transport_ops_t transport_ops;

  if (!channel) return NULL;

  process = channel_c_internal_failpoint_hit("process.alloc_fail")
              ? NULL
              : (struct channel_c_process *)calloc(1, sizeof(struct channel_c_process));
  if (!process) return NULL;

  if (channel_c_internal_failpoint_hit("process.mutex_init_fail") || pthread_mutex_init(&process->lock, NULL) != 0) {
    free(process);
    return NULL;
  }

  transport_ops.send_text = process_send_text;
  transport_ops.stop = process_stop_transport;

  process->connection = channel_c_internal_failpoint_hit("process.connection_create_fail")
                          ? NULL
                          : channel_c_connection_create(channel, state, state_destroy, NULL, process, transport_ops);
  if (!process->connection) {
    pthread_mutex_destroy(&process->lock);
    free(process);
    return NULL;
  }

  process->stopped = 0;
  channel_c_internal_connection_set_process_fast_path(process->connection, 1);
  channel_c_internal_connection_mark_connected(process->connection, 1);
  return process;
}

channel_c_sender_t *channel_c_process_sender(channel_c_process_t *process) {
  struct channel_c_process *p = (struct channel_c_process *)process;
  if (!p || !p->connection) return NULL;
  return channel_c_connection_sender(p->connection);
}

void channel_c_process_stop(channel_c_process_t *process) {
  struct channel_c_process *p = (struct channel_c_process *)process;
  if (!p) return;
  process_stop_transport(p);
}

void channel_c_process_free(channel_c_process_t *process) {
  struct channel_c_process *p = (struct channel_c_process *)process;
  if (!p) return;

  channel_c_process_stop(process);
  if (p->connection) {
    channel_c_connection_release(p->connection);
    p->connection = NULL;
  }

  pthread_mutex_destroy(&p->lock);
  free(p);
}

channel_c_connection_t *channel_c_internal_process_connection(channel_c_process_t *process) {
  struct channel_c_process *p = (struct channel_c_process *)process;
  if (!p) return NULL;
  return p->connection;
}

int channel_c_internal_process_send(channel_c_process_t *process, const char *text) {
  struct channel_c_process *p = (struct channel_c_process *)process;
  return process_send_text(p, text);
}
