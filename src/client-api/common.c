#include <rdma/rdma_cma.h>
#include <errno.h>
#include <string.h>
#include <locale.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <stdint.h>

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

	if(event->event != RDMA_CM_EVENT_ADDR_RESOLVED)
		return -1;

	rdma_ack_cm_event(event);

	if(rdma_resolve_route(db->cm_id, RESOLVE_TIMEOUT_MS) != 0)
		return -1;

	if(rdma_get_cm_event(db->cm_channel, &event) != 0)
		return -1;

	if (event->event != RDMA_CM_EVENT_ROUTE_RESOLVED)
		return -1;

	rdma_ack_cm_event(event);

	return 0;
}

/*	
 *	Free all the resources
 */
void free_resourses(db_t *db)
{
	locale_t default_locale = uselocale((locale_t)0);

	if(db == NULL)
		return;

	/* detroy QP, if it has been created */
	if(db->cm_id != NULL && db->has_qp_created)
	{
		rdma_destroy_qp(db->cm_id);
	}

	/* deregister MRs */
	if(db->rev_mr != NULL)
	{
		if(ibv_dereg_mr(db->rev_mr) != 0)
		{
			fprintf(stderr, "Fails to deregister memory rev_mr: %s\n", strerror_l(errno, default_locale));
		}
	}
	if(db->send_mr != NULL)
	{
		if(ibv_dereg_mr(db->send_mr) != 0)
		{
			fprintf(stderr, "Fails to deregister memory send_mr: %s\n", strerror_l(errno, default_locale));
		}
	}

	/* free buffers */
	if(db->rev_buf != NULL)
		free(db->rev_buf);
	if(db->send_buf != NULL)
		free(db->send_buf);

	/* destroy CQ */
	if(db->cq != NULL)
	{
		if(ibv_destroy_cq(db->cq) != 0)
		{
			fprintf(stderr, "Fails to destroy CQ: %s\n", strerror_l(errno, default_locale));
		}
	}

	/* destroy completion channel */
	if(db->comp_channel != NULL)
	{
		if(ibv_destroy_comp_channel(db->comp_channel) != 0)
		{
			fprintf(stderr, "Fails to destroy completion channel: %s\n", strerror_l(errno, default_locale));
		}
	}

	/* deallocate PD */
	if(db->pd != NULL)
	{
		if(ibv_dealloc_pd(db->pd) != 0)
		{
			fprintf(stderr, "Fails to deallocate PD: %s\n", strerror_l(errno, default_locale));
		}
	}

	/* destroy cm id */
	if(rdma_destroy_id(db->cm_id) != 0)
	{
		fprintf(stderr, "Fails to destroy rdma_cm_id: %s\n", strerror_l(errno, default_locale));
	}

	/* estroy cm channel */
	rdma_destroy_event_channel(db->cm_channel);

	free(db);

	return;
}

/*
 *	Setup resources for communication
 *
 *	success: return 0
 *	failure: return -1
 */
int setup_resources(db_t *db, uint32_t buf_size)
{
	if(db == NULL || db->cm_id == NULL || db->cm_channel == NULL)
		return -1;

	/* PD */
	db->pd = ibv_alloc_pd(db->cm_id->verbs); 
	if(!db->pd)
		return -1;

	/* completion channel */
	db->comp_channel = ibv_create_comp_channel(db->cm_id->verbs);
	if (!db->comp_channel) 
		return -1; 
	
	/* CQ */	
	db->cq = ibv_create_cq(db->cm_id->verbs, CQE, NULL, db->comp_channel, 0); 
	if (!db->cq) 
		return -1; 

	/* Arm notification on CQ */
  	if (ibv_req_notify_cq(db->cq,  0) != 0) 
		return -1; 

	/* allocate memory for buffers */
	db->rev_buf = (byte *)malloc(buf_size);
	if(!db->rev_buf)
		return -1;

	db->send_buf = (byte *)malloc(buf_size);
	if(!db->send_buf)
		return -1;

	/* register memory */
	db->rev_mr = ibv_reg_mr(db->pd, db->rev_buf, buf_size, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE); 
	if(!db->rev_mr)
		return -1;

	db->send_mr = ibv_reg_mr(db->pd, db->send_buf, buf_size, IBV_ACCESS_LOCAL_WRITE); 
	if(!db->send_mr)
		return -1;

	/* create QP */
	if(qp_create(db->cm_id, db->pd, db->cq) != 0)
		return -1;
	db->has_qp_created = 1;

	return 0;
}

/*
 *	Create QP
 *
 *	success: return 0
 *	failure: return -1
 */
int qp_create(struct rdma_cm_id *cm_id, struct ibv_pd *pd, struct ibv_cq *cq)
{
	struct ibv_qp_init_attr qp_attr;

	memset(&qp_attr, 0, sizeof(struct ibv_qp_init_attr));
	qp_attr.cap.max_send_wr = 2; 
    qp_attr.cap.max_send_sge = 1; 
    qp_attr.cap.max_recv_wr = 2; 
    qp_attr.cap.max_recv_sge = 1; 
    qp_attr.send_cq = cq; 
    qp_attr.recv_cq = cq; 
    qp_attr.qp_type = IBV_QPT_RC; 
    if(rdma_create_qp(cm_id, pd, &qp_attr) != 0)
    	return -1;
    else
    	return 0;
}

/*
 *	Connect to kvs server
 *
 *	success: return 0
 *	failure: return -1
 */
int connect_server(db_t *db)
{
	struct rdma_conn_param 	conn_param;
	pdata_t					pdata;

	memset(&conn_param, 0, sizeof(struct rdma_conn_param));

	pdata.raddr = htonll((uintptr_t)db->rev_buf);
    pdata.rkey = htonl(db->rev_mr->rkey);

	conn_param.initiator_depth = 1;
	conn_param.retry_count     = 7;	
    conn_param.responder_resources = 1;
    conn_param.private_data = &pdata;
    conn_param.private_data_len = sizeof(pdata);

    if(rdma_connect(db->cm_id, &conn_param) != 0)
    	return -1;
}