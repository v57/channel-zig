#include <ctype.h>
#include <stdio.h>
#include <stdatomic.h>
#include <stdint.h>
#include <string.h>

#include "channel-c/channel_c.h"

static atomic_ullong g_zig_bench_requests = 0;

void channel_c_bench_reset_requests(void) { atomic_store_explicit(&g_zig_bench_requests, 0, memory_order_relaxed); }

uint64_t channel_c_bench_request_count(void) {
  return atomic_load_explicit(&g_zig_bench_requests, memory_order_relaxed);
}

static int channel_c_bench_text_count(const char *body_json) {
  const char *key;
  const char *cursor;
  int length = 0;

  if (!body_json) return 0;

  key = strstr(body_json, "\"text\"");
  if (!key) return 0;

  cursor = key + strlen("\"text\"");
  while (*cursor && isspace((unsigned char)*cursor)) cursor += 1;
  if (*cursor != ':') return 0;
  cursor += 1;
  while (*cursor && isspace((unsigned char)*cursor)) cursor += 1;
  if (*cursor != '"') return 0;
  cursor += 1;

  while (*cursor) {
    if (*cursor == '\\') {
      if (!cursor[1]) break;
      length += 1;
      cursor += 2;
      continue;
    }
    if (*cursor == '"') break;
    length += 1;
    cursor += 1;
  }

  return length;
}

int channel_c_bench_handle_text_count(const channel_c_post_request_t *request, char **out_body_json, char **out_error) {
  char buffer[32];
  atomic_fetch_add_explicit(&g_zig_bench_requests, 1, memory_order_relaxed);

  if (out_body_json) {
    snprintf(buffer, sizeof(buffer), "%d", channel_c_bench_text_count(request ? request->body_json : NULL));
    *out_body_json = channel_c_strdup(buffer);
  }
  if (out_error) *out_error = NULL;
  return 0;
}
