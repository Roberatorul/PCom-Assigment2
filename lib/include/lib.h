
#pragma once

#include <cstdint>
#include "utils.h"
#include <arpa/inet.h>
#include <deque>
#include "protocol.h"
#include <map>

/* Maximum segment size, change as you see fit */
#define MAX_DATA_SIZE 1400
#define MAX_SEGMENT_SIZE (MAX_DATA_SIZE + sizeof(poli_tcp_data_hdr))

#define MAX_CONNECTIONS 32

struct segment {
    char data[MAX_SEGMENT_SIZE]; /* Raw buffer to be sent directly over the socket */
    size_t len; /* header size + data payload size*/
    uint16_t seq_num;  /* Used for ordering and acknowledgments */
};

/* Protocol control block. Used to track different parameters about a connection. */
struct connection {

    int sockfd; /* socket used for this connection */
    int conn_id; /* connection identifier */
    struct sockaddr_in servaddr; /* used to identify the destination */
    pthread_mutex_t con_lock; /* Used for syncronization with the handler thread and read/send calls.*/

	/* Sliding window storing unacknowledged inflight segments */
	std::deque<segment> window;

    /* Parameters used only by the sender */
	uint16_t send_base; /* smallest unacknowledged seq_num */
	int dup_acks; /* Counter for duplicate ACKs: triggers Fast Retransmit after 3 dup ACKs */
	uint16_t next_seq_num; /* The seq num that will be assigned to the new payload segment */
	std::deque<segment> send_queue; /* Backlog of segments waiting to enter the sliding window */
	uint16_t peer_recv_window; /* Flow control: available buffer space reported by the receiver */

    /* Parameters used only by the receiver */
	uint16_t expected_recv_seq; /* The exact seq num the receiver needs next to maintain strict order */
	std::deque<segment> recv_queue; /* Buffer containing segments ready for the application to read */
	std::map<uint16_t, segment> early_packets; /* Holding area for early segments (key = seq_num)*/
	struct sockaddr_in cliaddr; /* address of the client that sends SYN */
	uint16_t recv_data_port; /* data port allocated for this connection */
};

/* ########## API that we expose to the application ########### */

/* Equivalent of listen. Ran by the server to waits for a connection from a
 * client. Returns a connection id. Blocking untill it receives a connection
 * request */
int wait4connect(uint32_t ip, uint16_t port);
/* Equivalent of connect. Used by the client to connect to a server. */
int setup_connection(uint32_t ip, uint16_t port);
/* Equivalent to recv. Blocking if there is no data to be written in buffer */
int recv_data(int connectionid, char *buffer, int len);
/* Equivalent to send. Used by the client to send a stream of bytes as segments */
int send_data(int conn_id, char *buffer, int len);
/* Used to initialize your protocol on the receiver side. */
void init_receiver(int recv_buffer_bytes);
/* Used to initialize your protocol on the sender side */
void init_sender(int speed, int delay);

/* ######### Internal API used by sender and receiver ########### */
int recv_message_or_timeout(char *buff, size_t len, int *conn_id);
