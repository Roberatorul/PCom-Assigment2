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

using namespace std;

std::map<int, struct connection *> cons;
static int next_conn_id = 1;

struct pollfd data_fds[MAX_CONNECTIONS];
/* Used for timers per connection */
struct pollfd timer_fds[MAX_CONNECTIONS];
int fdmax = 0;

/* Receiver window size */
static uint16_t recv_window = 0;

int recv_data(int conn_id, char *buffer, int len)
{
    int size = 0;

    pthread_mutex_lock(&cons[conn_id]->con_lock);
    
    /* We will write code here as to not have sync problems with recv_handler */

    pthread_mutex_unlock(&cons[conn_id]->con_lock);

    return size;
}

void *receiver_handler(void *arg)
{

    char segment[MAX_SEGMENT_SIZE];
    int res;
    DEBUG_PRINT("Starting recviver handler\n");

    while (1) {

        int conn_id = -1;
        do {
            res = recv_message_or_timeout(segment, MAX_SEGMENT_SIZE, &conn_id);
        } while(res == -14);

        pthread_mutex_lock(&cons[conn_id]->con_lock);

        /* Handle segment received from the sender. We use this between locks
        as to not have synchronization issues with the recv_data calls which are
        on the main thread */

        pthread_mutex_unlock(&cons[conn_id]->con_lock);
    }

    
}

int wait4connect(uint32_t ip, uint16_t port)
{
    /* TODO: Implement the Three Way Handshake on the receiver part. This blocks
     * until a connection is established. */

	
	/* This can be used to set a timer on a socket, useful once we received a
     * SYN. You may want to disable by setting the time to 0 (tv_sec = 0,
     * tv_usec = 0)
	 struct timeval tv;
	 tv.tv_sec = 2;
	 tv.tv_usec = 100000;
	 if (setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO,&tv,sizeof(tv)) < 0) {
        perror("Error");
		} */

	/* Create a listening socket for server to receive incoming connections on */
	int rc;
	int listenfd = socket(AF_INET, SOCK_DGRAM, 0);
	DIE(listenfd < 0, "Create server socket failed!");

	/* Fill server fields */
	struct sockaddr_in serv_addr;
	socklen_t slen = sizeof(struct sockaddr_in);
	fill_ipv4_address(&serv_addr, port, ip);

	/* Bind server listenfd to server port */
	rc = bind(listenfd, (const struct sockaddr *)&serv_addr, slen);
	DIE(rc < 0, "Binding server socket to port failed!");

	/* Wait for a SYN request */
	char recv_message[MAX_SEGMENT_SIZE];
	struct sockaddr_in cli_addr;
	socklen_t clen = sizeof(struct sockaddr_in);

	rc = recvfrom(listenfd, recv_message, sizeof(recv_message), 0,
					 (struct sockaddr *)&cli_addr, &clen);
	DIE(rc < 0, "Receive SYN message from client failed!");

	/* Check message integrity */
	struct poli_tcp_ctrl_hdr *packet_hdr = (struct poli_tcp_ctrl_hdr *)recv_message;
	if (packet_hdr->type != TYPE_SYN) return -1;

	/* Create a new connection */
	struct connection *con = (struct connection *)malloc(sizeof(struct connection));
	int conn_id = next_conn_id++; // must be modified
	memset(con, 0, sizeof(struct connection));

    /* Receive SYN on the connection socket. Create a new socket and bind it to
	* the chosen port. Send the data port number via SYN-ACK to the client */
	con->sockfd = socket(AF_INET, SOCK_DGRAM, 0);

	/* Create a reliable connection with the client */
	fill_ipv4_address(&con->servaddr, 0, INADDR_ANY);

	/* Bind client socket to it's specific address */
	rc = bind(con->sockfd, (const struct sockaddr *)&con->servaddr, clen);
	DIE(rc < 0, "Binding client socket failed!");

	/* Extract the port given by the OS after bind */
	struct sockaddr_in asgn_addr;
	socklen_t alen = sizeof(struct sockaddr_in);
	memset(&asgn_addr, 0, slen);
	rc = getsockname(con->sockfd, (struct sockaddr *)&asgn_addr, &alen);
	DIE(rc < 0, "Failed to extract port with getsockname!");
	uint16_t asgn_port = ntohs(asgn_addr.sin_port);

	/* Build a SYN ACK segment for the client */
	char send_message[MAX_SEGMENT_SIZE];
	build_segment_hdr(send_message, MAX_SEGMENT_SIZE, POLI_PROTOCOL_ID, conn_id,
				  TYPE_SYN_ACK, 1, recv_window);
	char* payload_ptr = send_message + sizeof(struct poli_tcp_ctrl_hdr);
	uint16_t sending_port = htons(asgn_port);
	memcpy(payload_ptr, &sending_port, sizeof(uint16_t));

	/* Send packet to the client */
	size_t message_len = sizeof(struct poli_tcp_ctrl_hdr) + sizeof(uint16_t);
	rc = sendto(listenfd, send_message, message_len, 0,
				(const struct sockaddr *)&cli_addr, clen);
	DIE(rc < 0, "Send SYN ACK to client failed!");

	/* Wait for ACK from client */
	struct pollfd pfd;
	pfd.fd = con->sockfd;
	pfd.events = POLLIN;

	int timeout = 500; // ms
	bool ack_received = false;
	char ack_buffer[MAX_SEGMENT_SIZE];

	while (!ack_received) {
		rc = poll(&pfd, 1, timeout);

		if (rc > 0) {
			struct sockaddr_in ack_addr;
			socklen_t ack_len = sizeof(struct sockaddr_in);

			int n = recvfrom(con->sockfd, ack_buffer, sizeof(ack_buffer), 0,
							 (struct sockaddr *)&ack_addr, &ack_len);
			DIE(n < 0, "Receive control ACK from client failed!");

			struct poli_tcp_ctrl_hdr* ack_hdr = (struct poli_tcp_ctrl_hdr *)ack_buffer;
			if (ack_hdr->type == TYPE_ACK)
				ack_received = true;
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
    spec.it_value.tv_sec = 1;    
    spec.it_value.tv_nsec = 0;    
    spec.it_interval.tv_sec = 1;    
    spec.it_interval.tv_nsec = 0;    
    timerfd_settime(timer_fds[fdmax].fd, 0, &spec, NULL);    
    fdmax++;

    pthread_mutex_init(&con->con_lock, NULL);
    cons.insert({conn_id, con});

	printf("Connection %d established! Server waits data on port %d\n", conn_id, asgn_port);
    //DEBUG_PRINT("Connection established!");

    return conn_id;
}

void init_receiver(int recv_buffer_bytes)
{
    pthread_t thread1;
    int ret;

    /* TODO: Create the connection socket and bind it to 8031 */

    ret = pthread_create( &thread1, NULL, receiver_handler, NULL);
    assert(ret == 0);
}
