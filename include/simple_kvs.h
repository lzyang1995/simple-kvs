/*
 *
 */

#ifndef SIMPLE_KVS_H
#define SIMPLE_KVS_H

#include <rdma/rdma_cma.h>
#include <endian.h>
#include <byteswap.h>

//#define CREATE_EVENT_CHANNEL_FAILURE 1
//#define MIGRATE_EVENT_CHANNEL_FAILURE 2
//#define THREAD_CREATION_FAILURE 3
#define PD_ALLOC_FAILURE 			4
#define COMP_CHAN_CREATION_FAILURE 	5
#define CQ_CREATION_FAILURE 		6
#define ARM_NOTIFY_FAILURE_BEF_ACC 	7
#define MALLOC_FAILURE 				8
#define MEM_REG_FAILURE 			9
#define QP_CREATION_FAILURE 		10
#define POST_RECV_FAILURE_BEF_ACC 	11
#define ACC_FAILURE 				12
#define GET_CHAN_EVENT_FAILURE 		13
#define EVENT_UNMATCH 				14
#define GET_CQ_EVENT_FAILURE 		15
#define ARM_NOTIFY_FAILURE_AFT_ACC 	16
#define POLL_CQ_FAILURE 			17
#define WC_UNSUCCESSFUL 			18
#define POST_RECV_FAILURE_AFT_ACC	19
#define POST_SEND_FAILURE			20
#define MALLOC_FAILURE_AFT_ACC		21

#define SUCCESS 		0
#define ERROR   		1
#define KET_NOT_FOUND	2

#define CMD_PUT 		1
#define CMD_GET 		2
#define CMD_DEL 		3
#define CMD_DISCONNECT 	4

#define SEND_WR_ID 0
#define RECV_WR_ID 1

#define RESOLVE_TIMEOUT_MS 5000

#define CQE 1

#define byte uint8_t

/* TODO: this is going to be a server starting parameter */
#define MAX_SERVER_REV_BUF_SIZE 1024
/* TODO: this is going to be a server starting parameter */
#define MAX_SERVER_SED_BUF_SIZE 1024

#if __BYTE_ORDER == __LITTLE_ENDIAN
static inline uint64_t
htonll (uint64_t x)
{
    return bswap_64 (x);
}

static inline uint64_t
ntohll (uint64_t x)
{
    return bswap_64 (x);
}
#elif __BYTE_ORDER == __BIG_ENDIAN

static inline uint64_t
htonll (uint64_t x)
{
    return x;
}

static inline uint64_t
ntohll (uint64_t x)
{
    return x;
}
#else
#error __BYTE_ORDER is neither __LITTLE_ENDIAN nor __BIG_ENDIAN
#endif

struct pdata_t
{
	uint64_t raddr; 
    uint32_t rkey; 
};
typedef struct pdata_t pdata_t;

struct thread_param_t
{
	struct rdma_cm_id *cm_id;
	struct rdma_cm_id *listen_id;
	struct rdma_event_channel *cm_channel;
	pdata_t pdata;
};
typedef struct thread_param_t thread_param_t;

struct message_t
{
	uint8_t cmd;
	uint32_t key_len;
	uint32_t data_len;
	byte key_and_data[0];
} __attribute__ ((packed));
typedef struct message_t message_t;

struct reply_t
{
	uint8_t state;
	uint32_t data_len;
	byte 	data[0];
} __attribute__ ((packed));
typedef struct reply_t reply_t;

struct db_t
{
	struct rdma_event_channel 	*cm_channel;
	struct rdma_cm_id 			*cm_id;
	struct ibv_pd				*pd;
	struct ibv_comp_channel		*comp_channel;
	struct ibv_cq				*cq;
	byte 						*send_buf;
	byte 						*rev_buf;
	struct ibv_mr				*send_mr;
	struct ibv_mr				*rev_mr;
	byte 						has_qp_created;
};
typedef struct db_t db_t;

#endif