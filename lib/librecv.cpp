#include <pthread.h>
#include <cstdlib>
#include <map>
#include <cstdint>
#include "lib.h"
#include "utils.h"
#include "protocol.h"
#include <poll.h>
#include <cassert>
#include <memory.h>
#include <sys/timerfd.h>
#include <unistd.h>

using namespace std;

std::map<int, struct connection *> cons;
static int next_conn_id = 1;

struct pollfd data_fds[MAX_CONNECTIONS];
/* Used for timers per connection */
struct pollfd timer_fds[MAX_CONNECTIONS];
int fdmax = 0;

/* Receiver window size */
static uint16_t recv_window = 0;

/* Used to secure map data structure usage */
pthread_mutex_t map_lock = PTHREAD_MUTEX_INITIALIZER;

int recv_data(int conn_id, char *buffer, int len)
{
	pthread_mutex_lock(&map_lock);
    if (cons.find(conn_id) == cons.end() || cons[conn_id] == nullptr) {
		pthread_mutex_unlock(&map_lock);
		return -1;
	}

    struct connection *con = cons[conn_id];
	pthread_mutex_unlock(&map_lock);
    int size = 0;
	socklen_t slen = sizeof(struct sockaddr_in);

	/* Wait for data to arrive in deque */
    while (size == 0) {
        pthread_mutex_lock(&con->con_lock);

        if (!con->recv_queue.empty()) {
            struct segment seg = con->recv_queue.front();

            size_t header_size = sizeof(struct poli_tcp_data_hdr);
            size_t payload_len = seg.len - header_size;

            size_t read_size = std::min((size_t)len, payload_len);

			/* Copy payload into the application buffer */
            memcpy(buffer, seg.data + header_size, read_size);

            if (read_size < payload_len) {
				/* If the application requested fewer bytes than the segment holds,
				 * we must shift the remaining unread payload to the beginning
				 * of the data section and keep the segment in the queue */
                memmove(seg.data + header_size, seg.data + header_size +
						read_size, payload_len - read_size);
                seg.len -= read_size;

                con->recv_queue.front() = seg; 
            } else {
				/* Otherwise, segment was fully consumed and it is safe to
				 * remove it from the queue */
                con->recv_queue.pop_front();
            }

            size = read_size;

            int free_pkts = (int)recv_window - (int)(con->recv_queue.size()
							+ con->early_packets.size());
            if (free_pkts < 1) free_pkts = 1;

            char ack_msg[MAX_SEGMENT_SIZE];
            uint16_t cum_ack = con->expected_recv_seq - 1;

            build_segment_hdr(ack_msg, MAX_SEGMENT_SIZE, POLI_PROTOCOL_ID, conn_id,
                              TYPE_ACK, cum_ack, (uint16_t)free_pkts);
            sendto(con->sockfd, ack_msg, sizeof(struct poli_tcp_ctrl_hdr), 0,
                   (struct sockaddr*)&con->servaddr, slen);
        }

        pthread_mutex_unlock(&con->con_lock);

        if (size == 0) {
            usleep(1000);
        }
    }

    return size;
}

void *receiver_handler(void *arg)
{
    char segment[MAX_SEGMENT_SIZE];
    socklen_t slen = sizeof(struct sockaddr_in);

    while (1) {
        int conn_id = -1;

        int res = recv_message_or_timeout(segment, MAX_SEGMENT_SIZE, &conn_id);

		if (res == -14) {
			continue;
		}

		if (res <= 0) continue;

		struct poli_tcp_data_hdr* hdr = (struct poli_tcp_data_hdr*)segment;
		if (hdr->protocol_id != POLI_PROTOCOL_ID || hdr->type != TYPE_MESSAGE) continue;

		/* If the message doesn't belong to an active connection, skip it */
		pthread_mutex_lock(&map_lock);
		if (!(conn_id != -1 && cons.find(conn_id) != cons.end())) {
			pthread_mutex_unlock(&map_lock);
			continue;
		}

        struct connection *con = cons[conn_id];
		pthread_mutex_unlock(&map_lock);

        pthread_mutex_lock(&con->con_lock);

        uint16_t seq_num = ntohs(hdr->seq_num);

        if (seq_num == con->expected_recv_seq) {
			/* The exact segment we were waiting for has arrived
			 * Copy its data and push it into the recv queue */
            struct segment new_seg;
            memcpy(new_seg.data, segment, res);
            new_seg.len = res;
            new_seg.seq_num = seq_num;

            con->recv_queue.push_back(new_seg);
            con->expected_recv_seq++;

			/* Now that the seq gap is filled, check if any future segments
			 * previously arrived and are waiting in early_packets map */
            while (con->early_packets.count(con->expected_recv_seq) > 0) {
                /* Move the buffered early packets into the main recv_queue and
				 * remove it from the holding area */
                con->recv_queue.push_back(con->early_packets[con->expected_recv_seq]);
                con->early_packets.erase(con->expected_recv_seq);
                con->expected_recv_seq++;
            }
        } else if (seq_num > con->expected_recv_seq) {
			/* Put the segment into the holding area */
            if (con->early_packets.count(seq_num) == 0) {
                struct segment new_seg;
                memcpy(new_seg.data, segment, res);
                new_seg.len = res;
                new_seg.seq_num = seq_num;
                con->early_packets[seq_num] = new_seg;
            }
        } else {
			/* Do nothing */
		}

        uint16_t cum_ack = con->expected_recv_seq - 1;
        int free_pkts = (int)recv_window - (int)(con->recv_queue.size()
						+ con->early_packets.size());
        if (free_pkts < 1) free_pkts = 1;

        char ack_msg[MAX_SEGMENT_SIZE];
        build_segment_hdr(ack_msg, MAX_SEGMENT_SIZE, POLI_PROTOCOL_ID, conn_id, 
                              TYPE_ACK, cum_ack, (uint16_t)free_pkts);
        sendto(con->sockfd, ack_msg, sizeof(struct poli_tcp_ctrl_hdr), 0,
               (struct sockaddr*)&con->servaddr, slen);

		pthread_mutex_unlock(&con->con_lock);
    }
}

int wait4connect(uint32_t ip, uint16_t port)
{
	static int listenfd = -1;
	int rc;

	/* Create a listening socket for server to receive incoming connections on
	 * Using static keyword for listenfd allows us to initialize listenfd and serv_addr
	 * only once, that being the first time when wait4connect() is called 
	 * This approach helps us with multiple clients implementation, where the
	 * server calls wait4connect() for each cleint */
	if (listenfd == -1) { // We only create once for the server 
		listenfd = socket(AF_INET, SOCK_DGRAM, 0);
		DIE(listenfd < 0, "Create server socket failed!");

		/* Fill server fields */
		struct sockaddr_in serv_addr;
		socklen_t slen = sizeof(struct sockaddr_in);
		fill_ipv4_address(&serv_addr, port, ip);

		/* Bind server listenfd to server port */
		rc = bind(listenfd, (const struct sockaddr *)&serv_addr, slen);
		DIE(rc < 0, "Binding server socket to port failed!");
	}

	/* Wait for a SYN request. A SYN retransmitted by a client we have already
	 * accepted (because SYN-ACK was lost) must not create a new connection
	  * otherwise the <conn_id, file_mapping> gets corrupted. */
	char recv_message[MAX_SEGMENT_SIZE];
	struct sockaddr_in cli_addr;
	socklen_t clen = sizeof(struct sockaddr_in);

	while (1) {
		rc = recvfrom(listenfd, recv_message, sizeof(recv_message), 0,
						(struct sockaddr *)&cli_addr, &clen);
		DIE(rc < 0, "Receive SYN message from client failed!");

		/* Check message integrity */
		struct poli_tcp_ctrl_hdr *packet_hdr = (struct poli_tcp_ctrl_hdr *)recv_message;
		if (packet_hdr->protocol_id != POLI_PROTOCOL_ID
			|| packet_hdr->type != TYPE_SYN) {
			continue;
		}

		/* Check cons for an already accepted client */
		bool duplicate = false;
		pthread_mutex_lock(&map_lock);
		for (const std::pair<const int, struct connection*>& x : cons) {
			struct connection* c = x.second;
			bool is_same_client = c->cliaddr.sin_addr.s_addr == cli_addr.sin_addr.s_addr &&
								  c->cliaddr.sin_port == cli_addr.sin_port;

			if (is_same_client) {
				/* Retransmit SYN-ACK for this connections */
				char resend[MAX_SEGMENT_SIZE];
				build_segment_hdr(resend, MAX_SEGMENT_SIZE, POLI_PROTOCOL_ID,
								  c->conn_id, TYPE_SYN_ACK, 1, recv_window);
				memcpy(resend + sizeof(struct poli_tcp_ctrl_hdr),
					  &c->recv_data_port, sizeof(uint16_t));
				size_t resend_len = sizeof(struct poli_tcp_ctrl_hdr) + sizeof(uint16_t);
				sendto(listenfd, resend, resend_len, 0,
					  (const struct sockaddr *)&cli_addr, clen);
				duplicate = true;
				break;
			}
		}
		pthread_mutex_unlock(&map_lock);

		if (duplicate) continue;

		/* New client: proceed with the handshake below */
		break;
	}

	/* Create a new connection */
	struct connection *con = new connection();
	int conn_id = next_conn_id++;
	con->conn_id = conn_id;
	con->expected_recv_seq = 1;
	con->cliaddr = cli_addr;

	con->sockfd = socket(AF_INET, SOCK_DGRAM, 0);

	/* Create a reliable connection with the client 
	 * Server uses bind to get a specific port from the OS, port that will
	 * be used by the client in future to send data to */
	fill_ipv4_address(&con->servaddr, 0, INADDR_ANY);

	/* Bind client socket to get a specific port */
	rc = bind(con->sockfd, (const struct sockaddr *)&con->servaddr, clen);
	DIE(rc < 0, "Binding client socket failed!");

	/* Extract the port given by the OS after bind */
	struct sockaddr_in asgn_addr;
	socklen_t alen = sizeof(struct sockaddr_in);
	memset(&asgn_addr, 0, alen);
	rc = getsockname(con->sockfd, (struct sockaddr *)&asgn_addr, &alen);
	DIE(rc < 0, "Failed to extract port with getsockname!");
	uint16_t asgn_port = ntohs(asgn_addr.sin_port);

	/* Build a SYN-ACK segment for the client */
	char send_message[MAX_SEGMENT_SIZE];
	build_segment_hdr(send_message, MAX_SEGMENT_SIZE, POLI_PROTOCOL_ID, conn_id,
				  TYPE_SYN_ACK, 1, recv_window);
	char* payload_ptr = send_message + sizeof(struct poli_tcp_ctrl_hdr);
	uint16_t sending_port = htons(asgn_port);
	memcpy(payload_ptr, &sending_port, sizeof(uint16_t));

	/* Store the data port so that we can rebuild the SYN-ACK if the
	 * client retransmits its SYN later */
	con->recv_data_port = sending_port;

	/* Send packet to the client */
	size_t message_len = sizeof(struct poli_tcp_ctrl_hdr) + sizeof(uint16_t);
	rc = sendto(listenfd, send_message, message_len, 0,
				(const struct sockaddr *)&cli_addr, clen);
	DIE(rc < 0, "Send SYN ACK to client failed!");

	/* Insert into cons now (before waiting for the final ACK) so that a
	 * retransmitted SYN arriving dyring this wait is recognised as a duplicate */
	pthread_mutex_init(&con->con_lock, NULL);
	pthread_mutex_lock(&map_lock);
	cons.insert({conn_id, con});
	pthread_mutex_unlock(&map_lock);

	/* Wait for ACK from client */
	struct pollfd pfd;
	pfd.fd = con->sockfd;
	pfd.events = POLLIN;

	int timeout = 500;
	bool ack_received = false;
	char ack_buffer[MAX_SEGMENT_SIZE];

	while (!ack_received) {
		rc = poll(&pfd, 1, timeout);

		if (rc > 0) { // socket is available
			struct sockaddr_in ack_addr;
			socklen_t ack_len = sizeof(struct sockaddr_in);

			int n = recvfrom(con->sockfd, ack_buffer, sizeof(ack_buffer), 0,
							 (struct sockaddr *)&ack_addr, &ack_len);
			DIE(n < 0, "Receive control ACK from client failed!");

			struct poli_tcp_ctrl_hdr* ack_hdr = (struct poli_tcp_ctrl_hdr *)ack_buffer;
			if (ack_hdr->type == TYPE_ACK || ack_hdr->type == TYPE_MESSAGE) {
				/* If we receive a TYPE_MESSAGE segment it means that the 
				 * connection was established and the client is 
				 * already sending data */
				ack_received = true;
			}

			con->servaddr = ack_addr;
		} else if (rc == 0) { // Timeout! Resend SYN ACK
			rc = sendto(listenfd, send_message, message_len, 0, 
                        (const struct sockaddr *)&cli_addr, clen);
            DIE(rc < 0, "Failed to resend SYN-ACK!");
		} else {
			fprintf(stderr, "Poll error!");
			return -1;
		}
	}

	/* Since we can have multiple connection, we want to know if data is available
	on the socket used by a given connection. We use POLL for this */
    data_fds[fdmax].fd = con->sockfd;    
    data_fds[fdmax].events = POLLIN;    

    /* This creates a timer and sets it to trigger every 1 sec. We use this
       to know if a timeout has happend on a connection */
    timer_fds[fdmax].fd = timerfd_create(CLOCK_REALTIME,  0);    
    timer_fds[fdmax].events = POLLIN;    
	struct itimerspec spec;
    spec.it_value.tv_sec = 0; 
    spec.it_value.tv_nsec = 10000000;
    spec.it_interval.tv_sec = 0;
    spec.it_interval.tv_nsec = 10000000;
    timerfd_settime(timer_fds[fdmax].fd, 0, &spec, NULL);
    fdmax++;

    return conn_id;
}

void init_receiver(int recv_buffer_bytes)
{
    pthread_t thread1;
    int ret;

    recv_window = recv_buffer_bytes / MAX_SEGMENT_SIZE;
	if (recv_window == 0) recv_window = 1;

    ret = pthread_create( &thread1, NULL, receiver_handler, NULL);
    assert(ret == 0);
}
