#include <stdio.h>

#include "utstring.h"
#include "uthash.h"
#include "utlist.h"
#include "event_loop.h"

/** Assertion definitions, with optional logging. */
#define CHECK(COND)                                                         \
  do {                                                                      \
    if (!(COND)) {                                                          \
      fprintf(stderr, "[CHECK] (%s:%d) %s \n", __FILE__, __LINE__, #COND);  \
      exit(-1);                                                             \
    }                                                                       \
  } while (0)

#define REQUEST_ID_SIZE 20

typedef struct { unsigned char id[REQUEST_ID_SIZE]; } request_id;

typedef struct redis_request {
  /* Key according to which we shard. Not necessarily unique. */
  request_id id;
  UT_string *command;
  struct redis_request *prev;
  struct redis_request *next;
} redis_request;

typedef struct {
  /* File descriptor of this redis server. */
  int fd;
  /* List of pending requests to be submitted to this redis shard. */
  redis_request *waiting;
  /* List of requests we are waiting an answer for. */
  redis_request *pending;
} redis_shard;

/* A redis context is a set of redis shards. */
typedef struct {
  /* The event loop that is handling these shards. */
  event_loop *loop;
  /* For now only one shard. */
  redis_shard shard;
} redis_context;

redis_context *redis_context_create(event_loop *loop);

void redis_context_destroy(redis_context *context);

redis_shard *redis_get_shard(redis_context *context, request_id id);

void redis_submit_request(redis_context *context, request_id id, UT_string *command);
