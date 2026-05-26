#include <memory.h>
#include <arpa/inet.h>
#include "protocol.h"

void fill_ipv4_address(struct sockaddr_in* dest, uint16_t port, uint32_t ip) {
    memset(dest, 0, sizeof(struct sockaddr_in));
    dest->sin_family = AF_INET;
    dest->sin_port = htons(port);
    dest->sin_addr.s_addr = ip; 
}

/* Creates a segment based on it's `type` */
void build_segment_hdr(char segment[], size_t seg_size, uint8_t protocol_id,
                   uint8_t conn_id, uint8_t type, uint16_t num, uint16_t len) {

    memset(segment, 0, seg_size);

    if (type == TYPE_MESSAGE) {
        struct poli_tcp_data_hdr* hdr = (struct poli_tcp_data_hdr *)segment;
        hdr->protocol_id = POLI_PROTOCOL_ID;
        hdr->conn_id = conn_id;
        hdr->type = type;
        hdr->seq_num = htons(num);
        hdr->len = htons(len);
    } else {
        struct poli_tcp_ctrl_hdr* hdr = (struct poli_tcp_ctrl_hdr *)segment;
        hdr->protocol_id = protocol_id;
        hdr->conn_id = conn_id;
        hdr->type = type;	
        hdr->ack_num = htons(num);
        hdr->recv_window = htons(len);
    }
}