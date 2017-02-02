/* The Berkeley redis client. */

#include <netinet/in.h>
#include <netdb.h>
#include <sys/socket.h>
#include <arpa/inet.h>

#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>
#include <inttypes.h>

#include <sys/time.h>

#include "bredis.h"
#include "event_loop.h"

int redis_connect(const char *server_ip, uint16_t port) {
  struct hostent *hostinfo = NULL;
  struct sockaddr_in server_address;

  int sock = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
  CHECK(sock >= 0);

  memset(&server_address, 0, sizeof(server_address));
  server_address.sin_family = AF_INET;
  server_address.sin_port = htons(port);
  server_address.sin_addr.s_addr = inet_addr(server_ip);
  CHECK(connect(sock, (struct sockaddr *) &server_address, sizeof(server_address)) >= 0);
  return sock;
}

int write_bytes(int fd, const uint8_t *cursor, size_t length) {
  ssize_t nbytes = 0;
  size_t bytesleft = length;
  size_t offset = 0;
  while (bytesleft > 0) {
    /* While we haven't written the whole message, write to the file
     * descriptor, advance the cursor, and decrease the amount left to write. */
    nbytes = write(fd, cursor + offset, bytesleft);
    if (nbytes < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
        continue;
      }
      return -1; /* Errno will be set. */
    } else if (0 == nbytes) {
      /* Encountered early EOF. */
      return -1;
    }
    CHECK(nbytes > 0);
    bytesleft -= nbytes;
    offset += nbytes;
  }

  return 0;
}

/* Write the next bytes from the "waiting" buffer to a redis shard.
 * Returns true if we are done writing bytes, i.e. if the buffer is
 * exhausted. Return false if the file descriptor is busy.
 */
bool continue_writing_buffer(redis_shard *shard) {
  redis_request *request = shard->waiting;
  // printf("addr is %p\n", request->command);
  char *data = utstring_body(request->command) + shard->offset;
  // printf("string is %s", data);
  int64_t len = utstring_len(request->command) - shard->offset;
  // printf("len is %" PRId64 "\n", len);
  if (len > 0) {
    // printf("test\n");
    int nwritten = write(shard->fd, data, len);
    printf("wrote bytes %s\n, offset = %" PRId64 " len = %" PRId64 "\n", utstring_body(request->command), shard->offset, len);
    if (nwritten == -1) {
      /* Try again later. */
      CHECK(errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR);
      return false;
    } else {
      CHECK(nwritten > 0);
      shard->offset += nwritten;
      return nwritten == len;
    }
  }
  return true;
}

/* Write bytes to shard until no more bytes are available to write or
 * the file descriptor does not accept more bytes. */
void write_to_shard(event_loop *loop, redis_shard *shard) {
  while (shard->waiting && continue_writing_buffer(shard)) {
    /* Reset buffer. */
    redis_request *request = shard->waiting;
    CDL_DELETE(shard->waiting, request);
    CDL_PREPEND(shard->pending, request);
    shard->offset = 0;
  }
  if (!shard->waiting) {
    // printf("XXX removing\n");
    event_loop_remove_file_events(loop, shard->fd, EVENT_LOOP_WRITE);
    // event_loop_remove_file(loop, shard->fd);
    // printf("file events are %d\n", aeGetFileEvents(loop, shard->fd));
    // event_loop_stop(loop);
  }
}

#define CHUNKSIZE (16 * 1024)

int read_bytes(int fd) {
  char buf[CHUNKSIZE];
  int nread;
  do {
    nread = read(fd, buf, sizeof(buf));
    if (nread == -1) {
      CHECK(false);
    }
    printf("read %d bytes %*.*s\n", nread, 0, nread, buf);
  } while (nread < CHUNKSIZE);
  return 0;
}

void redis_submit_request(redis_context *context, request_id id, UT_string *command) {
  redis_request *req = (redis_request *) malloc(sizeof(redis_request));
  memset(req, 0, sizeof(redis_request));
  req->id = id;
  req->command = command;
  // printf("%p\n", command);
  redis_shard *shard = redis_get_shard(context, id);
  CDL_APPEND(shard->waiting, req);
}

redis_shard *redis_get_shard(redis_context *context, request_id id) {
  return &context->shard;
}

void redis_context_destroy(redis_context *context) {
  free(context);
}

int redis_timer_handler(event_loop *loop, timer_id timer_id, void *c) {
  redis_context *context = c;
  printf("timeout\n");
  return context->timeout_ms;
}

redis_context *redis_context_create(event_loop *loop, int64_t timeout_ms) {
  redis_context *context = (redis_context *) malloc(sizeof(redis_context));
  context->loop = loop;
  context->shard.fd = redis_connect("127.0.0.1", 6379);
  context->shard.waiting = NULL;
  context->shard.pending = NULL;
  context->shard.offset = 0;
  context->timeout_ms = timeout_ms;
  event_loop_add_timer(loop, timeout_ms, redis_timer_handler, context);
};

void file_event_handler(event_loop *loop,
                        int fd,
                        void *context,
                        int events) {
  redis_context *c = context;
  if (events & EVENT_LOOP_READ) {
    printf("reading\n");
    read_bytes(fd);
  }
  if (events & EVENT_LOOP_WRITE) {
    printf("writing\n");
    write_to_shard(loop, &c->shard);
  }
}

int main(void) {
  request_id NIL_ID;
  memset(&NIL_ID, 0, REQUEST_ID_SIZE);

  event_loop *loop = event_loop_create();

  redis_context *context = redis_context_create(loop);

  UT_string *s2;
  utstring_new(s2);
  utstring_printf(s2, "*3\r\n$5\r\nRPUSH\r\n$7\r\nmylist2\r\n$1\r\n1\r\n");
  
  for (int i = 0; i < 10; ++i) {
    redis_submit_request(context, NIL_ID, s2);
  }

  UT_string *s;
  utstring_new(s);
  utstring_printf(s, "*2\r\n$4\r\nLLEN\r\n$7\r\nmylist2\r\n");
  
  for (int i = 0; i < 100; ++i) {
    redis_submit_request(context, NIL_ID, s);
  }

  UT_string *s3;
  utstring_new(s3);
  utstring_printf(s3, "*1\r\n$4\r\nPING\r\n");

  redis_submit_request(context, NIL_ID, s3);

  event_loop_add_file(loop, context->shard.fd, EVENT_LOOP_READ | EVENT_LOOP_WRITE,
                      file_event_handler, context);
  // event_loop_remove_file(loop, conn);
  // printf("file events are %d\n", aeGetFileEvents(loop, conn));
  

  struct timeval  tv1, tv2;
  gettimeofday(&tv1, NULL);
  event_loop_run(loop);
  gettimeofday(&tv2, NULL);

  printf("Total time = %f seconds\n",
        (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 +
        (double) (tv2.tv_sec - tv1.tv_sec));

  // read_bytes(conn);
  // sleep(1);
  // read_bytes(conn);
}
