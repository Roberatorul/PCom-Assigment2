#include <pthread.h>
#include <cstdlib>
#include <map>
#include <cstdint>
#include "lib.h"
#include "utils.h"
#include "protocol.h"
#include <cassert>
#include <poll.h>
#include <memory.h>
#include <sys/timerfd.h>

using namespace std;

std::map<int, struct connection *> cons;

struct pollfd data_fds[MAX_CONNECTIONS];
/* Used for timers per connection */
struct pollfd timer_fds[MAX_CONNECTIONS];
int fdmax = 0;

int send_data(int conn_id, char *buffer, int len)
{
    int size = 0;

    pthread_mutex_lock(&cons[conn_id]->con_lock);

    /* We will write code here as to not have sync problems with sender_handler */

    pthread_mutex_unlock(&cons[conn_id]->con_lock);

    return size;
}

void *sender_handler(void *arg)
{
    int res = 0;
    char buf[MAX_SEGMENT_SIZE];

    while (1) {

        if (cons.size() == 0) {
            continue;
        }
        int conn_id = -1;
        do {
            res = recv_message_or_timeout(buf, MAX_SEGMENT_SIZE, &conn_id);
        } while(res == -14);

        pthread_mutex_lock(&cons[conn_id]->con_lock);

        /* Handle segment received from the receiver. We use this between locks
        as to not have synchronization issues with the send_data calls which are
        on the main thread */

        pthread_mutex_unlock(&cons[conn_id]->con_lock);
    }
}

int setup_connection(uint32_t ip, uint16_t port)
{
    /* Implement the sender part of the Three Way Handshake. Blocks
    until the connection is established */

    struct connection *con = (struct connection *)malloc(sizeof(struct connection));
    memset(con, 0, sizeof(struct connection));
    con->sockfd = socket(AF_INET, SOCK_DGRAM, 0);

    /* Fill server information fields
     * port field will be changed with the one that the server
     * sends on SYN ACK payload */
    socklen_t slen = sizeof(struct sockaddr_in);
    fill_ipv4_address(&con->servaddr, port, ip);

    /* // This can be used to set a timer on a socket 
    struct timeval tv;
    tv.tv_sec = 2;
    tv.tv_usec = 100000;
    if (setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO,&tv,sizeof(tv)) < 0) {
        perror("Error");
    } */

    /* We will send the SYN on 8031. Then we will receive a SYN-ACK with the connection
     * port. We can use con->sockfd for both cases, but we will need to update server_addr
     * with the port received via SYN-ACK */

    /* Create a SYN segment 
     * num (ack_num) is set to 0 (dummy value, connection not established yet)
     * len (recv_window) is set to 0 because the server is the one who
     * establishes recv_window size */
    char send_message[MAX_SEGMENT_SIZE];
    size_t message_len = sizeof(struct poli_tcp_ctrl_hdr); /* Only send the header */
    
    /* conn_id is set to 0 initially. We will extract the real one from the server's SYN-ACK */
    build_segment_hdr(send_message, MAX_SEGMENT_SIZE, POLI_PROTOCOL_ID, 0, 
                     TYPE_SYN, 0, 0);
                     
    int rc = sendto(con->sockfd, send_message, message_len, 0, 
                    (const struct sockaddr *)&con->servaddr, slen);
    DIE(rc < 0, "Failed to send control SYN segment to server!");

    /* Wait for a SYN ACK from server */
    struct pollfd pfd;
    pfd.fd = con->sockfd;
    pfd.events = POLLIN;

    int timeout = 500; // ms
    bool syn_ack_received = false;
    char ack_buffer[MAX_SEGMENT_SIZE];
    int assigned_conn_id = -1;

    while (!syn_ack_received) {
        rc = poll(&pfd, 1, timeout);

        if (rc > 0) {
            struct sockaddr_in ack_addr;
            socklen_t ack_len = sizeof(struct sockaddr_in);

            int n = recvfrom(con->sockfd, ack_buffer, sizeof(ack_buffer), 0,
                             (struct sockaddr *)&ack_addr, &ack_len);
            DIE(n < 0, "Receive SYN-ACK from server failed!");

            struct poli_tcp_ctrl_hdr* ack_hdr = (struct poli_tcp_ctrl_hdr *)ack_buffer;
            if (ack_hdr->type == TYPE_SYN_ACK) {
                syn_ack_received = true;
                
                /* Store the connection ID assigned by the server */
                assigned_conn_id = ack_hdr->conn_id; 
                con->conn_id = assigned_conn_id;
            }
        } else if (rc == 0) { 
            /* Timeout! Resend SYN segment */
            rc = sendto(con->sockfd, send_message, message_len, 0, 
                        (const struct sockaddr *)&con->servaddr, slen);
            DIE(rc < 0, "Failed to resend SYN!");
        } else {
            fprintf(stderr, "Poll error!");
            return -1;
        }
    }

    /* At this stage the server filled ack_buffer payload with the port
     * that the client will use in future communication */
    char *payload_ptr = ack_buffer + sizeof(struct poli_tcp_ctrl_hdr);
    uint16_t comm_port_net;

    /* Extract the port */
    memcpy(&comm_port_net, payload_ptr, sizeof(uint16_t));

    /* Update the server address to the new port */
    con->servaddr.sin_port = comm_port_net;

    /* Send final ACK to the new port */
    char final_ack[MAX_SEGMENT_SIZE];
    build_segment_hdr(final_ack, MAX_SEGMENT_SIZE, POLI_PROTOCOL_ID, assigned_conn_id, 
                      TYPE_ACK, 1, 0); 

    rc = sendto(con->sockfd, final_ack, sizeof(struct poli_tcp_ctrl_hdr), 0, 
                (const struct sockaddr *)&con->servaddr, slen);
    DIE(rc < 0, "Failed to send final ACK!");

    /* Since we can have multiple connection, we want to know if data is available
       on the socket used by a given connection. We use POLL for this */
    data_fds[fdmax].fd = con->sockfd;    
    data_fds[fdmax].events = POLLIN;    

    /* This creates a timer and sets it to trigger every 1 sec. We use this
       to know if a timeout has happend on our connection */
    timer_fds[fdmax].fd = timerfd_create(CLOCK_REALTIME,  0);    
    timer_fds[fdmax].events = POLLIN;    
    struct itimerspec spec;     
    spec.it_value.tv_sec = 1;    
    spec.it_value.tv_nsec = 0;    
    spec.it_interval.tv_sec = 1;    
    spec.it_interval.tv_nsec = 0;    
    timerfd_settime(timer_fds[fdmax].fd, 0, &spec, NULL);    
    fdmax++;

    pthread_mutex_init(&con->con_lock, NULL);
    cons.insert({assigned_conn_id, con});

    printf("Client connection %d established! Target port updated to %d\n", assigned_conn_id, ntohs(comm_port_net));
    DEBUG_PRINT("Connection established!");

    return assigned_conn_id;
}

void init_sender(int speed, int delay)
{
    pthread_t thread1;
    int ret;

    /* Create a thread that will*/
    ret = pthread_create( &thread1, NULL, sender_handler, NULL);
    assert(ret == 0);
}
