//
// Created by nvarsha on 4/20/24.
//

#ifndef RDMA_WITH_PY_STRUCTS_H
#define RDMA_WITH_PY_STRUCTS_H


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

#include <rdma/rdma_cma.h>
#include <infiniband/verbs.h>

#define DEFAULT_RDMA_PORT (12345)
#define ENABLE_ERROR

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

#define DATA_SIZE (1024 * 1024 * 20)

struct exchange_buffer {
    struct msg* message;
    struct ibv_mr* buffer;
};

struct frame_msg {
    long ftype;
    char *ftext;
};

struct thread_arguments {
    struct client_resources* client_resources;
    struct exchange_buffer server_buffer;
    struct exchange_buffer client_buffer;
    struct memory_region *frame;
    int msq_id;
    struct frame_msg *sbuf;
};

struct msg {
    enum {
        HELLO,
        FRAME
    } type;

    union {
        struct ibv_mr mr;
        unsigned long offset;
    } data;
};

struct client_resources {
    struct ibv_pd *pd;
    struct ibv_cq *cq;
    struct ibv_comp_channel *comp_channel;
    struct ibv_qp *qp;
    struct rdma_cm_id *id;
};

struct memory_region {
    struct ibv_mr server_mr;
    char *memory_region;
    struct ibv_mr *memory_region_mr;
    unsigned long *mapping_table_start;
};


#endif //RDMA_WITH_PY_STRUCTS_H
