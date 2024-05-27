#include "utils.h"
#include "structs.h"
#include <sys/msg.h>
#include <sys/ipc.h>
#include <fcntl.h>
#include <pthread.h>

static struct rdma_event_channel *cm_event_channel = NULL;
static struct ibv_recv_wr client_recv_wr, *bad_client_recv_wr = NULL;
static struct ibv_send_wr server_send_wr, *bad_server_send_wr = NULL;
static struct ibv_sge client_recv_sge, server_send_sge;
static struct ibv_qp_init_attr qp_init_attr;
static struct rdma_cm_id *cm_server_id = NULL;

static char* received_frame = NULL;

/* Setup client resources like PD, CC, CQ and QP */
static void setup_client_resources(struct rdma_cm_id *cm_client_id, struct client_resources *_client_struct) {
    if (!cm_client_id) {
        error("Client id is still NULL \n");
        return;
    }
    _client_struct->id = cm_client_id;

    /* Init the Protection Domain for the client */
    HANDLE(_client_struct->pd = ibv_alloc_pd(cm_client_id->verbs));
    debug("Protection domain (PD) allocated: %p \n", _client_struct->pd)

    /* Init the Completion Channel for the client */
    HANDLE(_client_struct->comp_channel = ibv_create_comp_channel(cm_client_id->verbs));
    debug("I/O completion event channel created: %p \n",
          _client_struct->comp_channel)

    int flags = fcntl(_client_struct->comp_channel->fd, F_GETFL);
    int rc = fcntl(_client_struct->comp_channel->fd, F_SETFL, flags | O_NONBLOCK);
    if (rc < 0) {
        fprintf(stderr, "Failed to change file descriptor of Completion Event Channel\n");
    }

    /* Init the Completion Queue for the client */
    HANDLE(_client_struct->cq = ibv_create_cq(cm_client_id->verbs,
                                          CQ_CAPACITY,
                                          NULL,
                                              _client_struct->comp_channel,
                                          0));
    debug("Completion queue (CQ) created: %p with %d elements \n",
          _client_struct->cq, _client_struct->cq->cqe)

    /* Ask for the event for all activities in the completion queue */
    HANDLE_NZ(ibv_req_notify_cq(_client_struct->cq,
                                0));
    bzero(&qp_init_attr, sizeof qp_init_attr);
    qp_init_attr.cap.max_recv_sge = MAX_SGE; /* Maximum SGE per receive posting */
    qp_init_attr.cap.max_recv_wr = MAX_WR; /* Maximum receive posting capacity */
    qp_init_attr.cap.max_send_sge = MAX_SGE; /* Maximum SGE per send posting */
    qp_init_attr.cap.max_send_wr = MAX_WR; /* Maximum send posting capacity */
    qp_init_attr.qp_type = IBV_QPT_RC; /* QP type, RC = Reliable connection */
    qp_init_attr.recv_cq = _client_struct->cq;
    qp_init_attr.send_cq = _client_struct->cq;
    HANDLE_NZ(rdma_create_qp(_client_struct->id,
                             _client_struct->pd,
                             &qp_init_attr));
    _client_struct->qp = cm_client_id->qp;
    debug("Client QP created: %p \n", _client_struct->qp)
}

/*
 * Receive HELLO message from client
 * Create a buffer for receiving client's message. Register ibv_reg_mr using client's pd,
 * message addr, length of message, and permissions for the buffer.
 * Post the client buffer as a Receive Request (RR) to the Work Queue (WQ)
 * */
static void post_recv_hello(struct client_resources* _client_struct, struct exchange_buffer *client_buffer) {
    client_buffer->message = malloc(sizeof(struct msg));
    HANDLE(client_buffer->buffer = rdma_buffer_register(_client_struct->pd,
                                                     client_buffer->message,
                                                     sizeof(struct msg),
                                                     (IBV_ACCESS_LOCAL_WRITE)));

    client_recv_sge.addr = (uint64_t) client_buffer->buffer->addr;
    client_recv_sge.length = client_buffer->buffer->length;
    client_recv_sge.lkey = client_buffer->buffer->lkey;

    bzero(&client_recv_wr, sizeof(client_recv_wr));
    client_recv_wr.sg_list = &client_recv_sge;
    client_recv_wr.num_sge = 1;

    HANDLE_NZ(ibv_post_recv(_client_struct->qp,
                            &client_recv_wr,
                            &bad_client_recv_wr));

    info("Pre-posting Receive HELLO \n");
}

/*
 * Send HELLO message to initiate conversation
 */
static void post_send_hello(struct client_resources* _client_struct, struct exchange_buffer *client_buffer, struct exchange_buffer *server_buffer) {
    struct ibv_wc wc;
    server_buffer->message = malloc(sizeof(struct msg));
    server_buffer->message->type = HELLO;
    server_buffer->message->data.offset = client_buffer->message->data.offset + 1;
    server_buffer->buffer = rdma_buffer_register(_client_struct->pd,
                                              server_buffer->message,
                                              sizeof(struct msg),
                                              (IBV_ACCESS_LOCAL_WRITE |
                                               IBV_ACCESS_REMOTE_READ |
                                               IBV_ACCESS_REMOTE_WRITE));

    debug("Sending HELLO message: \n")
    show_exchange_buffer(server_buffer->message);

    server_send_sge.addr = (uint64_t) server_buffer->message;
    server_send_sge.length = (uint32_t) sizeof(struct msg);
    server_send_sge.lkey = server_buffer->buffer->lkey;

    bzero(&server_send_wr, sizeof(server_send_wr));
    server_send_wr.sg_list = &server_send_sge;
    server_send_wr.num_sge = 1;
    server_send_wr.opcode = IBV_WR_SEND;
    server_send_wr.send_flags = IBV_SEND_SIGNALED;

    HANDLE_NZ(ibv_post_send(_client_struct->qp, &server_send_wr, &bad_server_send_wr));
    process_work_completion_events(_client_struct->comp_channel,  &wc, 1);

    info("Sending HELLO \n");
}

static void post_send_ACK(struct client_resources* _client_struct, struct exchange_buffer *server_buffer) {
    struct ibv_wc wc;
    //server_buff.message = malloc(sizeof(struct msg));
    server_buffer->message->type = HELLO;
    server_buffer->message->data.offset = 111;

//    HANDLE(server_buff.buffer = rdma_buffer_register(client_res->pd,
//                                                      server_buff.message,
//                                                      sizeof(struct msg),
//                                                      (IBV_ACCESS_LOCAL_WRITE |
//                                                       IBV_ACCESS_REMOTE_READ |
//                                                       IBV_ACCESS_REMOTE_WRITE)));

    debug("Sending ACK message: \n")
    show_exchange_buffer(server_buffer->message);

    server_send_sge.addr = (uint64_t) server_buffer->message;
    server_send_sge.length = (uint32_t) sizeof(struct msg);
    server_send_sge.lkey = server_buffer->buffer->lkey;

    bzero(&server_send_wr, sizeof(server_send_wr));
    server_send_wr.sg_list = &server_send_sge;
    server_send_wr.num_sge = 1;
    server_send_wr.opcode = IBV_WR_SEND;
    server_send_wr.send_flags = IBV_SEND_SIGNALED;

    HANDLE_NZ(ibv_post_send(_client_struct->qp, &server_send_wr, &bad_server_send_wr));
    //process_work_completion_events(client_res->comp_channel,  &wc, 1);

    info("Sending POST ACK \n");
}

/*
 * Create Memory Buffer to receive message from client
 */
static void build_message_buffer(struct memory_region *region, struct client_resources* _client_struct) {
    region->memory_region = malloc(DATA_SIZE);
    memset(region->memory_region, 0, DATA_SIZE);

    region->memory_region_mr = rdma_buffer_register(_client_struct->pd,
                                                    region->memory_region ,
                                                  DATA_SIZE,
                                                  (IBV_ACCESS_LOCAL_WRITE |
                                                   IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE));
    info("Memory Map built: ADDR: %p\n", (unsigned long *) region->memory_region );
}

/*
 * Post a QR to receive the message buffer from the client
 */
static int post_recv_frame(struct client_resources* _client_struct, struct exchange_buffer *client_buffer) {
    struct ibv_wc wc;

    client_recv_sge.addr = (uint64_t) client_buffer->message;
    client_recv_sge.length = (uint32_t) sizeof(struct msg);
    client_recv_sge.lkey = client_buffer->buffer->lkey;

    bzero(&client_recv_wr, sizeof(client_recv_wr));
    client_recv_wr.sg_list = &client_recv_sge;
    client_recv_wr.num_sge = 1;

    HANDLE_NZ(ibv_post_recv(_client_struct->qp /* which QP */,
                            &client_recv_wr /* receive work request*/,
                            &bad_client_recv_wr /* error WRs */));
    info("Pre-posting Receive Buffer for Address \n");
    return 0;
}

/*
 * Post a QR with RDMA_READ to read_message_buffer
 */
static void read_message_buffer(struct memory_region *region, struct client_resources* _client_struct, struct exchange_buffer *client_buffer) {
    memcpy(&region->server_mr, &client_buffer->message->data.mr, sizeof(region->server_mr));

    server_send_sge.addr = (uintptr_t) (region->memory_region);
    server_send_sge.length = (uint32_t) DATA_SIZE;
    server_send_sge.lkey = region->memory_region_mr->lkey;

    bzero(&server_send_wr, sizeof(server_send_wr));
    server_send_wr.sg_list = &server_send_sge;
    server_send_wr.num_sge = 1;
    server_send_wr.opcode = IBV_WR_RDMA_READ;
    server_send_wr.send_flags = IBV_SEND_SIGNALED;
    server_send_wr.wr.rdma.remote_addr = (uintptr_t) region->server_mr.addr;
    server_send_wr.wr.rdma.rkey = region->server_mr.rkey;
    HANDLE_NZ(ibv_post_send(_client_struct->qp,
                            &server_send_wr,
                            &bad_server_send_wr));
    info("RDMA read the remote memory map. \n");
}

/* Establish connection with the client */
static void accept_conn(struct rdma_cm_id *cm_client_id) {
    struct rdma_conn_param conn_param;
    memset(&conn_param, 0, sizeof(conn_param));
    conn_param.initiator_depth = 5;
    conn_param.responder_resources = 5;

    HANDLE_NZ(rdma_accept(cm_client_id, &conn_param));
    debug("Wait for : RDMA_CM_EVENT_ESTABLISHED event \n")
}

void* wait_for_event(void *args) {
    struct thread_arguments *arguments = (struct thread_arguments*) args;
    struct client_resources *_client_struct = arguments->client_resources;
    struct exchange_buffer *server_buffer = &arguments->server_buffer;
    struct exchange_buffer *client_buffer = &arguments->client_buffer;
    struct memory_region *frame = arguments->frame;
    struct rdma_cm_event *received_event = NULL;

    while (rdma_get_cm_event(cm_event_channel, &received_event) == 0) {
        /* Initialize the received event */
        struct rdma_cm_event cm_event;
        struct ibv_wc wc;
        memcpy(&cm_event, received_event, sizeof(*received_event));
        debug("%s event received \n", rdma_event_str(cm_event.event));

        HANDLE_NZ(rdma_ack_cm_event(received_event));
        /* SWITCH case to check what type of event was received */
        switch (cm_event.event) {
            /* Initially Server receives and Client Connect Request */
            case RDMA_CM_EVENT_CONNECT_REQUEST:
                setup_client_resources(cm_event.id, _client_struct); // send a recv req for client_metadata
                build_message_buffer(frame, _client_struct);
                post_recv_hello(_client_struct, client_buffer);
                accept_conn(cm_event.id);
                break;

            /*  After the client establishes the connection */
            case RDMA_CM_EVENT_ESTABLISHED:
                process_work_completion_events(_client_struct->comp_channel, &wc, 1);
                //show_exchange_buffer(client_buffer->message);
                post_recv_frame(_client_struct, client_buffer);
                post_send_hello(_client_struct, client_buffer, server_buffer);

                // wait for receiving the frame details
                process_work_completion_events(_client_struct->comp_channel, &wc, 1);

                read_message_buffer(frame, _client_struct, client_buffer);

                int count = 0;
                while (strcmp(frame->memory_region, "") == 0 && count < 5) {
                    read_message_buffer(frame, _client_struct, client_buffer);
                    count += 1;
                }
                if (count >= 5) {
                    error("RDMA read returns empty data. Disconnecting Server \n");
                    rdma_buffer_deregister(frame->memory_region_mr);
                    disconnect_server(_client_struct, frame, cm_event_channel, cm_server_id);
                    pthread_exit(NULL);
                }
                received_frame = frame->memory_region;
                post_send_ACK(_client_struct, server_buffer);
                break;

            /* Disconnect and Cleanup */
            case RDMA_CM_EVENT_DISCONNECTED:
                rdma_buffer_deregister(frame->memory_region_mr);
                disconnect_server(_client_struct, frame, cm_event_channel, cm_server_id);
                debug("Cleanup complete - starting over \n");
                pthread_exit(NULL);
            default:
                error("Event not found %s\n", rdma_event_str(cm_event.event));
                pthread_exit(NULL);
        }
    }
    pthread_exit(NULL);
}

static void send_msg_to_queue(int msq_id, char* frame, struct frame_msg* sbuf) {
    size_t buf_length;

    (void) strcpy(sbuf->ftext, frame);
    buf_length = strlen(sbuf->ftext) + 1;

    // setting the message type to 1 - can change in case sending different types of messages
    sbuf->ftype = 1;

    if (msgsnd(msq_id, sbuf, buf_length, IPC_NOWAIT) < 0) {
        error ("%d, %s, %ld\n", msq_id, sbuf->ftext, buf_length);
        exit(1);
    }
    else
        info("Message: %d %ld %s %ld sent \n", msq_id, sbuf->ftype, sbuf->ftext, buf_length);
}

const char* start_rdma_server(struct sockaddr_in *server_sockaddr, int msq_id) {
    // Create RDMA Event Channel
    HANDLE(cm_event_channel = rdma_create_event_channel());

    // Using the RDMA EC, create ID to track communication information
    HANDLE_NZ(rdma_create_id(cm_event_channel, &cm_server_id, NULL, RDMA_PS_TCP));

    info("Received at: %s , port: %d \n",
         inet_ntoa(server_sockaddr->sin_addr),
         ntohs(server_sockaddr->sin_port));

    // Using the ID, bind the socket information
    HANDLE_NZ(rdma_bind_addr(cm_server_id, (struct sockaddr *) server_sockaddr));

    // Server Listening...
    HANDLE_NZ(rdma_listen(cm_server_id, 8));
    info("Server is listening successfully at: %s , port: %d \n",
         inet_ntoa(server_sockaddr->sin_addr),
         ntohs(server_sockaddr->sin_port));

    struct frame_msg sbuf;
    sbuf.ftext = malloc(DATA_SIZE);
    /* Init the client resources */
    for(;;) {
        pthread_t thread_id;
        info("SPINNING NEW THREAD\n");
        struct thread_arguments args;
        struct exchange_buffer server_buffer, client_buffer;
        args.client_resources = (struct client_resources *) malloc(sizeof(struct client_resources));
        args.server_buffer = server_buffer;
        args.client_buffer = client_buffer;
        args.frame = (struct memory_region *) malloc(sizeof(struct memory_region *));
        int ret = pthread_create(&thread_id, NULL, (void*) wait_for_event, (void *) &args);
        if (ret != 0) { info("Error from pthread: %d\n", ret); exit(1); }
        pthread_join(thread_id, 0);

        // send frame to the queue
        send_msg_to_queue(msq_id, received_frame, &sbuf);

        // clear the memory region
        memset(received_frame, 0, DATA_SIZE);
        free(args.frame);
        free(args.client_resources);
    }
}

const char* start_server(struct sockaddr_in *server_sockaddr, int msq_id) {
    return start_rdma_server(server_sockaddr, msq_id);
}

int main(int argc, char **argv) {
    struct sockaddr_in server_sockaddr;

    bzero(&server_sockaddr, sizeof(server_sockaddr));
    server_sockaddr.sin_family = AF_INET;
    server_sockaddr.sin_addr.s_addr = htonl(INADDR_ANY);
    int ret = get_addr("10.10.1.1", (struct sockaddr*) &server_sockaddr);
    if (ret) {
        error("Invalid IP");
        return ret;
    }
    server_sockaddr.sin_port = htons(12345);
    start_rdma_server(&server_sockaddr, 0);
}
