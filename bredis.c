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
  if (connect(sock, (struct sockaddr *) &server_address, sizeof(server_address)) < 0) {
    printf("connect() failed\n");
  }
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

#define CHUNKSIZE (16 * 1024)

int read_bytes(int fd) {
  char buf[CHUNKSIZE];
  int nread = read(fd, buf, sizeof(buf));
  if (nread == -1) {
    CHECK(false);
  }
  printf("read %d bytes %s\n", nread, buf);
  return 0;
}

void redis_submit_request(redis_context *context, request_id id, UT_string *command) {
  redis_request *req = (redis_request *) malloc(sizeof(redis_request));
  memset(req, 0, sizeof(redis_request));
  req->id = id;
  req->command = command;
  redis_shard *shard = redis_get_shard(context, id);
  DL_APPEND(shard->waiting, req);
}

redis_shard *redis_get_shard(redis_context *context, request_id id) {
  return &context->shard;
}

void redis_context_destroy(redis_context *context) {
  free(context);
}

redis_context *redis_context_create(event_loop *loop) {
  redis_context *context = (redis_context *) malloc(sizeof(redis_context));
  context->loop = loop;
  context->shard.waiting = NULL;
  context->shard.pending = NULL;
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
  }
}

int main(void) {
  int conn = redis_connect("127.0.0.1", 6379);
  event_loop *loop = event_loop_create();
  const char *cmd = "*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n";
  write_bytes(conn, cmd, strlen(cmd));
  const char *cmd2 = "*2\r\n$4\r\nLLEN\r\n$7\r\nmylist2\r\n";
  write_bytes(conn, cmd2, strlen(cmd2));
  event_loop_add_file(loop, conn, EVENT_LOOP_READ,
                      file_event_handler, NULL);
  event_loop_run(loop);
  // read_bytes(conn);
  // sleep(1);
  // read_bytes(conn);
}
