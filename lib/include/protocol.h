#include <cstdint>

/* Values used for protocol ID and type. */
#define POLI_PROTOCOL_ID 42

#define TYPE_ACK 0
#define TYPE_SYN 1
#define TYPE_SYN_ACK 2
#define TYPE_MESSAGE 3

/* Header for Data segments. Must be used in your implementation as it is. */
struct __attribute__((packed)) poli_tcp_data_hdr {
    uint8_t protocol_id;
    uint8_t conn_id;
    uint8_t type;
    uint16_t seq_num;
    uint16_t len;
};

/* Header for Control segments. Must be used in your implementation as it is. */
struct __attribute__((packed)) poli_tcp_ctrl_hdr {
    uint8_t protocol_id;
    uint8_t conn_id;
    uint8_t type;
    uint16_t ack_num;
    uint16_t recv_window;
};

/* Fills dest with specific IPv4 information */
void fill_ipv4_address(struct sockaddr_in* dest, uint16_t port, uint32_t ip);

/* Creates a segment based on it's `type` */
void build_segment(char segment[], size_t seg_size, uint8_t protocol_id,
				   uint8_t conn_id, uint8_t type, uint16_t num, uint16_t len);
