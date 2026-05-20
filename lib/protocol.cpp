#include <memory.h>
#include <arpa/inet.h>
#include "protocol.h"

void fill_ipv4_address(struct sockaddr_in* dest, uint16_t port, uint32_t ip) {
	memset(dest, 0, sizeof(struct sockaddr_in));
	dest->sin_family = PF_INET;
	dest->sin_port = htons(port);
	dest->sin_addr.s_addr = htonl(ip);
}

/* Creates a segment based on it's `type` */
void build_segment(char segment[], size_t seg_size, uint8_t protocol_id,
				   uint8_t conn_id, uint8_t type, uint16_t num, uint16_t len) {

	memset(segment, 0, sizeof(segment));

	if (type == TYPE_MESSAGE) {
		struct poli_tcp_data_hdr* hdr = (struct poli_tcp_data_hdr *)segment;
		hdr->protocol_id = POLI_PROTOCOL_ID;
		hdr->conn_id = conn_id;
		hdr->type = type;
		hdr->seq_num = num;
		hdr->len = len;
	} else {
		struct poli_tcp_ctrl_hdr* hdr = (struct poli_tcp_ctrl_hdr *)segment;
		hdr->protocol_id = protocol_id;
		hdr->conn_id = conn_id;
		hdr->type = type;
		hdr->ack_num = num;
		hdr->recv_window = len;
	}
}
