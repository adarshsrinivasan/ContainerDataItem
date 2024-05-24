//
// Created by Varsha Natarajan on 4/16/24.
//

#ifndef RDMA_WITH_PY_COMMON_H
#define RDMA_WITH_PY_COMMON_H

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <getopt.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netdb.h>
#include <time.h>
#include <sys/time.h>


#include <rdma/rdma_cma.h>
#include <infiniband/verbs.h>
#include "structs.h"

#define DEFAULT_RDMA_PORT (12345)
#define MAX_CONNECTION (7)
#define ENABLE_ERROR
//#define ENABLE_DEBUG

#define CQ_CAPACITY (16)
#define MAX_SGE (2)
#define MAX_WR (10)
#define TIMEOUTMS (2000)

#define HANDLE(x)  do { if (!(x)) error(#x " failed (returned zero/null).\n"); } while (0)
#define HANDLE_NZ(x) do { if ( (x)) error(#x " failed (returned non-zero)." ); } while (0)

#ifdef ENABLE_ERROR
#define error(msg, args...) do {\
        fprintf(stderr, "%s : %d : ERROR : "msg, __FILE__, __LINE__, ## args);\
    }while(0);
#else
#define error(msg, args...)
#endif

#ifdef ENABLE_DEBUG
#define debug(msg, args...) do {\
    printf("DEBUG: "msg, ## args);\
}while(0);

#else
#define debug(msg, args...)
#endif

#define info(msg, args...) do { \
    struct timeval tv; \
    gettimeofday(&tv, NULL); \
    struct tm *tm_info = localtime(&tv.tv_sec); \
    char buffer[40]; \
    strftime(buffer, 40, "%Y-%m-%d %H:%M:%S", tm_info); \
    fprintf(stdout, "log: %s.%03ld " msg, buffer, tv.tv_usec / 1000, ##args); \
} while (0);

#define DATA_SIZE (1024 * 1024 * 10)

int get_addr(char *dst, struct sockaddr *addr);

void show_memory_map(const char* memory_region);
void show_exchange_buffer(struct msg *attr);

int disconnect_server(struct client_resources* client_res, struct memory_region *frame, struct rdma_event_channel *cm_event_channel, struct rdma_cm_id *cm_server_id);
int disconnect_client(struct client_resources* client_res, struct rdma_event_channel *cm_event_channel, struct memory_region* region, struct exchange_buffer *server_buff, struct exchange_buffer *client_buff);

struct ibv_mr *rdma_buffer_register(struct ibv_pd *pd,
                                    void *addr,
                                    uint32_t length,
                                    enum ibv_access_flags permission);
struct ibv_mr *rdma_buffer_re_register(struct ibv_mr *buffer, int flags, struct ibv_pd *pd,
                                       void *addr, uint32_t length,
                                       enum ibv_access_flags permission);
void rdma_buffer_deregister(struct ibv_mr *mr);
void rdma_buffer_free(struct ibv_mr *mr);
struct ibv_mr* rdma_buffer_alloc(struct ibv_pd *pd, uint32_t size,
                                 enum ibv_access_flags permission);

int process_work_completion_events(struct ibv_comp_channel *comp_channel, struct ibv_wc *wc, int max_wc);

#endif //RDMA_WITH_PY_COMMON_H
