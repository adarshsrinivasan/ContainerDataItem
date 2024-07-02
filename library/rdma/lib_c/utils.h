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

int get_addr(char *dst, struct sockaddr *addr);

void show_memory_map(const char* memory_region);
void show_exchange_buffer(struct msg *attr);

void disconnect_server(struct client_resources* client_res);
void disconnect_client_short(struct client_resources* client_res, struct rdma_event_channel *cm_event_channel, struct memory_region* region);
void disconnect_client_long(struct client_resources* client_res, struct rdma_event_channel *cm_event_channel, struct memory_region* region, struct exchange_buffer *server_buff, struct exchange_buffer *client_buff);

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
