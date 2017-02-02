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

/* Type of callback that gets called when this command timed out. */
typedef void (*redis_operation_failed)(request_id req_id);

/* Callback that gets called when an operation succeeded. */
typedef void (*redis_reply_callback)(request_id req_id, UT_string *reply);

typedef struct redis_request {
  /* Key according to which we shard. Not necessarily unique. */
  request_id id;
  /* The redis command associated to this request. */
  UT_string *command;
  /* Callback to be called when this request succeeded. */
  redis_reply_callback reply_callback;
  /* Callback to be called when this request failed. */
  redis_operation_failed operation_failed;
  /* How many epochs has this request been in the pending queue. */
  int age;
  /* Previous request. */
  struct redis_request *prev;
  /* Next request. */
  struct redis_request *next;
} redis_request;

typedef struct {
  /* File descriptor of this redis server. */
  int fd;
  /* Circular double linked list of pending requests to be submitted to this
   * redis shard. This serves as a double ended queue. */
  redis_request *waiting;
  /* How many bytes have already been written of the head of the waiting
   * requests? */
  int64_t offset;
  /* Circular double linked list of requests we are waiting an answer for.
   * This serves as a double ended queue. */
  redis_request *pending;
} redis_shard;

/* A redis context is a set of redis shards. */
typedef struct {
  /* The event loop that is handling these shards. */
  event_loop *loop;
  /* For now only one shard. */
  redis_shard shard;
  /* Timeout in ms. */
  int64_t timeout_ms;
} redis_context;

redis_context *redis_context_create(event_loop *loop, int64_t timeout_ms);

void redis_context_destroy(redis_context *context);

redis_shard *redis_get_shard(redis_context *context, request_id id);

void redis_submit_request(redis_context *context, request_id id, UT_string *command);
