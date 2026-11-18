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
#include <unistd.h>
#include <vector>

using namespace std;

#define DUP_ACKS_THRESHOLD 3

std::map<int, struct connection *> cons;

struct pollfd data_fds[MAX_CONNECTIONS];
/* Used for timers per connection */
struct pollfd timer_fds[MAX_CONNECTIONS];
int fdmax = 0;

/* Used to secure map data structure usage */
pthread_mutex_t map_lock = PTHREAD_MUTEX_INITIALIZER;

static int sender_window = 1;
static long rto_ns = 20000000;

int send_data(int conn_id, char *buffer, int len)
{
    int size = 0;

	pthread_mutex_lock(&map_lock);
    if (cons.find(conn_id) == cons.end() || cons[conn_id] == nullptr) {
		pthread_mutex_unlock(&map_lock);
		return -1;
	}

    struct connection *con = cons[conn_id];
	pthread_mutex_unlock(&map_lock);

    pthread_mutex_lock(&con->con_lock);

	/* How much space is available (without poli_tcp_data_hdr) */
    size_t max_payload = MAX_SEGMENT_SIZE - sizeof(struct poli_tcp_data_hdr);

    int offset = 0;
    while (offset < len) {
        size_t chunk_size = std::min((size_t)len - offset, max_payload);

        struct segment seg;
        memset(&seg, 0, sizeof(struct segment));

		/* Segment header is also in seg.data field */
        build_segment_hdr(seg.data, MAX_SEGMENT_SIZE, POLI_PROTOCOL_ID, conn_id, 
                          TYPE_MESSAGE, con->next_seq_num, chunk_size);

		/* Copy buffer into seg.data */
        char* payload_ptr = seg.data + sizeof(struct poli_tcp_data_hdr);        
        memcpy(payload_ptr, buffer + offset, chunk_size);
        seg.len = sizeof(struct poli_tcp_data_hdr) + chunk_size;
        seg.seq_num = con->next_seq_num;

        con->send_queue.push_back(seg);

        con->next_seq_num++;

        offset += chunk_size;
        size += chunk_size;
    }

    pthread_mutex_unlock(&con->con_lock);

    return size;
}

void *sender_handler(void *arg)
{
    char buf[MAX_SEGMENT_SIZE];
	socklen_t slen = sizeof(struct sockaddr_in);
	int rc;

    while (1) {
		pthread_mutex_lock(&map_lock);
		bool is_empty = cons.empty();
		pthread_mutex_unlock(&map_lock);

        if (is_empty) {
			usleep(1000); // Wait a bit for a connection to be established
            continue;
        }

        int conn_id = -1;

        int res = recv_message_or_timeout(buf, MAX_SEGMENT_SIZE, &conn_id);

		struct connection *con = nullptr;
		pthread_mutex_lock(&map_lock);
		if (conn_id != -1 && cons.find(conn_id) != cons.end())
			con = cons[conn_id];
		pthread_mutex_unlock(&map_lock);

        if (con != nullptr) {
            pthread_mutex_lock(&con->con_lock);

            if (res > 0) { // data is available
                /* Check if we received ACK from server */
                struct poli_tcp_ctrl_hdr* hdr = (struct poli_tcp_ctrl_hdr*)buf;
				if (hdr->protocol_id != POLI_PROTOCOL_ID) {
					/* Ignore packet */
				} else if (hdr->type == TYPE_ACK) {
                    uint16_t acked_seq = ntohs(hdr->ack_num);

                    con->peer_recv_window = ntohs(hdr->recv_window);

                    if (acked_seq >= con->send_base) {
						con->dup_acks = 0;

						/* Move window base and remove all segments that have 
						 * been acknowledged */
                        con->send_base = acked_seq + 1;
                        while (!con->window.empty() &&
                               con->window.front().seq_num <= acked_seq)
                            con->window.pop_front();
                    } else {
						/* Resend the unacknowledged segment */
                        con->dup_acks++;
                        if (con->dup_acks >= DUP_ACKS_THRESHOLD && !con->window.empty()) {
                            segment &s = con->window.front();
                            rc = sendto(con->sockfd, s.data, s.len, 0,
                                        (struct sockaddr*)&con->servaddr, slen);
                            DIE(rc < 0, "Failed to fast retransmit");
                            con->dup_acks = 0;
                        }
                    }
                } else if (hdr->type == TYPE_SYN_ACK) {
					/* Respond to a late SYN ACK */
					char final_ack[MAX_SEGMENT_SIZE];
                    build_segment_hdr(final_ack, MAX_SEGMENT_SIZE, POLI_PROTOCOL_ID,
									  conn_id, TYPE_ACK, 1, 0); 
                    rc = sendto(con->sockfd, final_ack, sizeof(struct poli_tcp_ctrl_hdr),
								0, (struct sockaddr*)&con->servaddr, slen);
					DIE(rc < 0, "Send ACK!");
				}
            } else if (res == -1) { // Timeout
				/* Resend the first segment */
                if (!con->window.empty()) {
                    segment &s = con->window.front();
                    rc = sendto(con->sockfd, s.data, s.len, 0,
                                (struct sockaddr*)&con->servaddr, slen);
                    DIE(rc < 0, "Failed to send packets");
                }
            }

            pthread_mutex_unlock(&con->con_lock);
        }

        int per_conn_window = sender_window;
        if (per_conn_window < 1) per_conn_window = 1;

		/* Instead of locking the entire loop below, we first
         * safely copy the connection pointers into active_cons.
         * This significantly reduces the amount of time the mutex is held. */
		std::vector<struct connection*> active_cons;
		pthread_mutex_lock(&map_lock);
		for (const std::pair<const int, struct connection*>& pair : cons)
			active_cons.push_back(pair.second);
		pthread_mutex_unlock(&map_lock);

		/* Update state for all active connections */
        for (struct connection* c : active_cons) {

            pthread_mutex_lock(&c->con_lock);

			/* Compute how many segments can be sent to the server based on
			 * con->peer_recv_window */
            int eff_window = std::min(per_conn_window, (int)c->peer_recv_window);
			if (eff_window < 1) eff_window = 1;

			/* Send eff_window segments *from sending queue */
            while (c->window.size() < (size_t)eff_window && !c->send_queue.empty()) {
                segment new_seg;
                new_seg = c->send_queue.front();
                c->send_queue.pop_front();

                int rc = sendto(c->sockfd, new_seg.data, new_seg.len, 0, 
								(struct sockaddr*)&c->servaddr, slen);
				DIE(rc < 0, "Failed to send packet");

                c->window.push_back(new_seg);
            }

            pthread_mutex_unlock(&c->con_lock);
        }
    }
}

int setup_connection(uint32_t ip, uint16_t port)
{
    struct connection *con = new connection();
    con->sockfd = socket(AF_INET, SOCK_DGRAM, 0);
	con->next_seq_num = 1;
	con->send_base = 1;
	con->dup_acks = 0;
	con->peer_recv_window = 0; /* 0 = dummy value, will be filled after the first segment ACK */

    /* Fill server information fields
     * port field will be changed with the one that the server
     * sends on SYN ACK payload */
    socklen_t slen = sizeof(struct sockaddr_in);
    fill_ipv4_address(&con->servaddr, port, ip);

    /* Create a SYN segment 
     * num (ack_num) is set to 0 (dummy value, connection not established yet)
     * len (recv_window) is set to 0 because the server is the one who
     * establishes recv_window size */
    char send_message[MAX_SEGMENT_SIZE];
    size_t message_len = sizeof(struct poli_tcp_ctrl_hdr); /* Only send the header */
    /* conn_id is set to 0 initially. We will extract the real one from the server's SYN-ACK */
    build_segment_hdr(send_message, MAX_SEGMENT_SIZE, POLI_PROTOCOL_ID, 0, 
                      TYPE_SYN, 0, 0);

	/* Send SYN request to server */
    int rc = sendto(con->sockfd, send_message, message_len, 0, 
                    (const struct sockaddr *)&con->servaddr, slen);
    DIE(rc < 0, "Failed to send control SYN segment to server!");

    /* Wait for a SYN ACK from server */
    struct pollfd pfd;
    pfd.fd = con->sockfd;
    pfd.events = POLLIN;

    int timeout = 500;
    bool syn_ack_received = false;
    char ack_buffer[MAX_SEGMENT_SIZE];
    int assigned_conn_id = -1;

    while (!syn_ack_received) {
        rc = poll(&pfd, 1, timeout);

        if (rc > 0) { /* socket is available */
            struct sockaddr_in ack_addr;
            socklen_t ack_len = sizeof(struct sockaddr_in);

			/* Receive SYN-ACK from server */
            int n = recvfrom(con->sockfd, ack_buffer, sizeof(ack_buffer), 0,
                             (struct sockaddr *)&ack_addr, &ack_len);
            DIE(n < 0, "Receive SYN-ACK from server failed!");

			/* Check received segment integrity */
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
    uint16_t comm_port;

    /* Extract the port */
    memcpy(&comm_port, payload_ptr, sizeof(uint16_t));

    /* Update the server address to the new port */
    con->servaddr.sin_port = comm_port;

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
       to know if a timeout has happened on our connection */
    timer_fds[fdmax].fd = timerfd_create(CLOCK_REALTIME,  0);    
    timer_fds[fdmax].events = POLLIN;    
    struct itimerspec spec;
    spec.it_value.tv_sec = 0;
    spec.it_value.tv_nsec = rto_ns;
    spec.it_interval.tv_sec = 0;    
    spec.it_interval.tv_nsec = rto_ns;
    timerfd_settime(timer_fds[fdmax].fd, 0, &spec, NULL);    
    fdmax++;

    pthread_mutex_init(&con->con_lock, NULL);

	/* Make sure that the connection is added properly */
	pthread_mutex_lock(&map_lock);
    cons.insert({assigned_conn_id, con});
	pthread_mutex_unlock(&map_lock);

    return assigned_conn_id;
}

void init_sender(int speed, int delay)
{
    long long speed_bytes_sec = (long long)speed * 1000000 / 8;
    double rtt_sec = (2.0 * delay) / 1000.0;
    long long bdp_bytes = speed_bytes_sec * rtt_sec;
    int bdp_segments = bdp_bytes / MAX_SEGMENT_SIZE;

    sender_window = bdp_segments * 2;
    if (sender_window < 2) sender_window = 2;

    long rtt_ms = 2 * delay;
    rto_ns = rtt_ms * 4 * 1000000L;
    if (rto_ns < 10000000L) rto_ns = 10000000L; 

    pthread_t thread1;
    int ret = pthread_create(&thread1, NULL, sender_handler, NULL);
    assert(ret == 0);
}
