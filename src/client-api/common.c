#include <rdma/rdma_cma.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

#include <simple_kvs.h>

/*
 *	Resolve the RDMA address and route
 *	
 *	success: return 0
 *	failure: return -1
 */
int resolve(db_t * db, struct addrinfo *res)
{
	struct addrinfo 		*t;
	struct rdma_cm_event	*event;

	for (t = res; t; t = t->ai_next) 
	{
		if(rdma_resolve_addr(cm_id, NULL, t->ai_addr, RESOLVE_TIMEOUT_MS) != 0)
			continue;
		else
			break;
	}

	if(t == NULL)
		return -1;

	if(rdma_get_cm_event(db->cm_channel, &event) != 0)
		return -1;

	

			if (err)
					return err;
			err = rdma_get_cm_event(cm_channel, &event);
			if (err)
					return err;
			if (event->event != RDMA_CM_EVENT_ADDR_RESOLVED)
					return 1;
			rdma_ack_cm_event(event);
			err = rdma_resolve_route(cm_id, RESOLVE_TIMEOUT_MS);
			if (err)
					return err;
			err = rdma_get_cm_event(cm_channel, &event);
			if (err)
					return err;
			if (event->event != RDMA_CM_EVENT_ROUTE_RESOLVED)
					return 1;
			rdma_ack_cm_event(event);
}