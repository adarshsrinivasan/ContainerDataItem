#include "utils.h"

#include <poll.h>

void show_memory_map(const char* memory_region) {
    debug("-------------------\n")
    debug("Memory Map: %s \n", memory_region);
    debug("-------------------\n");
}

void show_exchange_buffer(struct msg *attr) {
    debug("---------------------------------------------------------\n");
    debug("------------EXCHANGE BUFFER------------\n");
    debug("---------------------------------------------------------\n");
    debug("message %p\n", attr);
    debug("message, type: %d\n", attr->type);
    if(attr->type == HELLO) {
        debug("message: hello: %lu \n", attr->data.offset);
    }
    if (attr->type == FRAME){
        debug("message: data.mr.address: %p \n", attr->data.mr.addr);
    }
    debug("---------------------------------------------------------\n");
}

struct ibv_mr* rdma_buffer_alloc(struct ibv_pd *pd, uint32_t size,
                                 enum ibv_access_flags permission)
{
    struct ibv_mr *mr = NULL;
    if (!pd) {
        error("Protection domain is NULL \n");
        return NULL;
    }
    void *buf = calloc(1, size);
    if (!buf) {
        error("failed to allocate buffer, -ENOMEM\n");
        return NULL;
    }
    debug("Buffer allocated: %p , len: %u \n", buf, size);
    mr = rdma_buffer_register(pd, buf, size, permission);
    if(!mr){
        free(buf);
    }
    return mr;
}

struct ibv_mr *rdma_buffer_register(struct ibv_pd *pd,
                                    void *addr, uint32_t length,
                                    enum ibv_access_flags permission)
{
    struct ibv_mr *mr = NULL;
    if (!pd) {
        error("Protection domain is NULL, ignoring \n");
        return NULL;
    }
    mr = ibv_reg_mr(pd, addr, length, permission);
    if (!mr) {
        error("Failed to create mr on buffer, errno: %d \n", -errno);
        return NULL;
    }
    debug("Registered: %p , len: %u , stag: 0x%x \n",
         mr->addr,
         (unsigned int) mr->length,
         mr->lkey);
    return mr;
}

struct ibv_mr *rdma_buffer_re_register(struct ibv_mr *buffer, int flags, struct ibv_pd *pd,
                                    void *addr, uint32_t length,
                                    enum ibv_access_flags permission)
{
    struct ibv_mr *mr = NULL;
    if (!pd) {
        error("Protection domain is NULL, ignoring \n");
        return NULL;
    }
    ibv_rereg_mr(buffer, flags, pd, addr, length, permission);
    debug("Registered: %p , len: %u , stag: 0x%x \n",
          mr->addr,
          (unsigned int) mr->length,
          mr->lkey);
    return buffer;
}

void rdma_buffer_free(struct ibv_mr *mr)
{
    if (!mr) {
        error("Passed memory region is NULL, ignoring\n");
        return ;
    }
    void *to_free = mr->addr;
    rdma_buffer_deregister(mr);
    debug("Buffer %p free'ed\n", to_free);
    free(to_free);
}

void rdma_buffer_deregister(struct ibv_mr *mr)
{
    if (!mr) {
        error("Passed memory region is NULL, ignoring\n");
        return;
    }
    ibv_dereg_mr(mr);
    debug("Deregistered: %p , len: %u , stag : 0x%x \n",
          mr->addr,
          (unsigned int) mr->length,
          mr->lkey);
}

int process_work_completion_events(struct ibv_comp_channel *comp_channel, struct ibv_wc *wc, int max_wc) {
    int total_wc, i;
    int ret = -1;
    void *context = NULL;
    struct ibv_cq *cq_ptr = NULL;

    struct pollfd my_pollfd;
    int ms_timeout = 1000;

    my_pollfd.fd      = comp_channel->fd;
    my_pollfd.events  = POLLIN;
    my_pollfd.revents = 0;
    int rc;
    int MAX_TIMEOUT = 10000;
    int cnt = 0;
    do {
        rc = poll(&my_pollfd, 1, ms_timeout);
        if (rc == 0) {
            debug("Timeout for poll \n");
        }
        cnt += 1;
    } while (rc == 0 && MAX_TIMEOUT != cnt * ms_timeout);
    if (rc < 0) {
        fprintf(stderr, "poll failed\n");
        return -1;
    }

    ret = ibv_get_cq_event(comp_channel, /* IO channel where we are expecting the notification */
                           &cq_ptr, /* which CQ has an activity. This should be the same as CQ we created before */
                           &context); /* Associated CQ user context, which we did set */
    if (ret) {
        error("Failed to get next CQ event due to %d \n", -errno);
        return -errno;
    }
    ibv_ack_cq_events(cq_ptr, 1);
    ret = ibv_req_notify_cq(cq_ptr, 0);
    if (ret) {
        error("Failed to request further notifications %d \n", -errno);
        return -errno;
    }
    total_wc = 0;
    do {
        ret = ibv_poll_cq(cq_ptr /* the CQ, we got notification for */,
                          max_wc - total_wc /* number of remaining WC elements*/,
                          wc + total_wc/* where to store */);
        if (ret < 0) {
            error("Failed to poll cq for wc due to %d \n", ret);
            return ret;
        }
        total_wc += ret;
    } while (total_wc < max_wc);
    debug("%d WC are completed \n", total_wc)
    for (i = 0; i < total_wc; i++) {
        if (wc[i].status != IBV_WC_SUCCESS) {
            error("Work completion (WC) has error status: %s at index %d \n",
                  ibv_wc_status_str(wc[i].status), i);
            return -(wc[i].status);
        }
    }
    ibv_ack_cq_events(cq_ptr, 1);

    return total_wc;
}

void disconnect_client(struct client_resources* client_res, struct rdma_event_channel *cm_event_channel, struct memory_region* region, struct exchange_buffer *server_buff, struct exchange_buffer *client_buff)
{
    int ret = -1;

    /* Destroy client.sh cm id */
    ret = rdma_destroy_id(client_res->id);
    if (ret) {
        error("Failed to destroy client.sh id cleanly, %d \n", -errno);
    }

    if ( region->memory_region_mr != NULL)
        rdma_buffer_deregister(region->memory_region_mr);
//    if (client_buff->buffer != NULL)
//        rdma_buffer_deregister(client_buff->buffer);
//    if (server_buff != NULL && server_buff->buffer != NULL)
//        rdma_buffer_deregister(server_buff->buffer);

    // rdma_destroy_event_channel(cm_event_channel);
    debug("Client resource clean up is complete \n");
}

void disconnect_server(struct client_resources* client_res)
{
    int ret = -1;

    /* Destroy QP */
    rdma_destroy_qp(client_res->id);

//    /* Destroy rdma server id */
//    ret = rdma_destroy_id(cm_server_id);
//    if (ret) {
//        error("Failed to destroy server id cleanly, %d \n", -errno);
//    }
//    rdma_destroy_event_channel(cm_event_channel);
//    free(client_res);

    debug("Server shut-down is complete \n");
}

/* Code acknowledgment: rping.c from librdmacm/examples */
int get_addr(char *dst, struct sockaddr *addr)
{
    struct addrinfo *res;
    int ret = -1;
    ret = getaddrinfo(dst, NULL, NULL, &res);
    if (ret) {
        error("getaddrinfo failed - invalid hostname or IP address\n");
        return ret;
    }
    memcpy(addr, res->ai_addr, sizeof(struct sockaddr_in));
    freeaddrinfo(res);
    return ret;
}
