/*
 *
 */

#ifndef SIMPLE_KVS_H
#define SIMPLE_KVS_H

#include <rdma/rdma_cma.h>

#define CREATE_EVENT_CHANNEL_FAILURE 1
#define MIGRATE_EVENT_CHANNEL_FAILURE 2
#define THREAD_CREATION_FAILURE 3

struct pdata_t
{
	uint64_t raddr; 
    uint32_t rkey; 
};
typedef struct pdata_t pdata_t;

struct thread_param_t
{
	struct rdma_cm_id *cm_id;
	struct rdma_event_channel *cm_channel;
	pdata_t pdata;
};
typedef struct thread_param_t thread_param_t;



#endif