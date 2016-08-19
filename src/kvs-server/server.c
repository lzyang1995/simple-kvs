/*
 *
 */

#define PORT 11111
#define ERROR 1

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <locale.h>
#include <pthread.h>
#include <arpa/inet.h>
#include <rdma/rdma_cma.h>
#include <inttypes.h>

#include <simple_kvs.h>
#include <simple_hash.h>

void * thread_func(void *arg);

int main()
{
	struct rdma_event_channel   *cm_channel;
	locale_t 					default_locale = uselocale((locale_t)0);
	struct rdma_cm_id 			*listen_id;
	struct sockaddr_in			sin;
	struct rdma_cm_event		*event;
	pdata_t						client_data;
	thread_param_t				*thread_param_p;
	pthread_t					pid;
	int 						rc;



	cm_channel = rdma_create_event_channel();
	if(!cm_channel)
	{
		/* use strerror_l for thread safety */
		fprintf(stderr, "rdma_create_event_channel fails: %s\n", strerror_l(errno, default_locale));
		exit(ERROR);
	}

	if(rdma_create_id(cm_channel,&listen_id, NULL, RDMA_PS_TCP) != 0)
	{
		fprintf(stderr, "rdma_create_id fails: %s\n", strerror_l(errno, default_locale));
		exit(ERROR);
	}

	sin.sin_family = AF_INET;
    sin.sin_port = htons(PORT);
    sin.sin_addr.s_addr = INADDR_ANY;

    if(rdma_bind_addr(listen_id, (struct sockaddr *)&sin) != 0)
    {
    	fprintf(stderr, "rdma_bind_addr fails: %s\n", strerror_l(errno, default_locale));
		exit(ERROR);
    }

    if(rdma_listen(listen_id, 1) != 0)
    {
    	fprintf(stderr, "rdma_bind_addr fails: %s\n", strerror_l(errno, default_locale));
		exit(ERROR);
    }

    while(1)
    {
    	if(rdma_get_cm_event(cm_channel, &event) == 0)
    	{
    		if(event->event == RDMA_CM_EVENT_CONNECT_REQUEST)
    		{
    			thread_param_p = (thread_param_t *)malloc(sizeof(thread_param_t));
    			/* the new thread is responsible to free it */
    			thread_param_p->listen_id = listen_id;
    			thread_param_p->cm_id = event->id;
    			memcpy(&thread_param_p->pdata, event->param.conn.private_data, sizeof(pdata_t));
    			rdma_ack_cm_event(event);

				if((thread_param_p->cm_channel = rdma_create_event_channel()) == NULL)
				{
					fprintf(stderr, "Fails to create event channel for new thread: %s\n", strerror_l(errno, default_locale));
					// for test
					if(rdma_destroy_id(thread_param_p->cm_id) != 0)
					{
						fprintf(stderr, "Fails to destroy the communication identifier: %s\n", strerror_l(errno, default_locale));
						continue;
					}

					free(thread_param_p);
					
					if(rdma_reject(listen_id, NULL, 0) != 0)
					{
						fprintf(stderr, "Fails to reject the connection request: %s\n", strerror_l(errno, default_locale));
						continue;
					}

					continue;
				}

				if(rdma_migrate_id(thread_param_p->cm_id, thread_param_p->cm_channel) != 0)
				{
					fprintf(stderr, "Fails to migrate event channel for new thread: %s\n", strerror_l(errno, default_locale));
					
					if(rdma_destroy_id(thread_param_p->cm_id) != 0)
					{
						fprintf(stderr, "Fails to destroy the communication identifier: %s\n", strerror_l(errno, default_locale));
						continue;
					}

					rdma_destroy_event_channel(thread_param_p->cm_channel);

					free(thread_param_p);
					
					if(rdma_reject(listen_id, NULL, 0) != 0)
					{
						fprintf(stderr, "Fails to reject the connection request: %s\n", strerror_l(errno, default_locale));
						continue;
					}

					continue;
				}

				rc = pthread_create(&pid, NULL, thread_func, (void *)thread_param_p);
				if(rc != 0)
				{
					fprintf(stderr, "Fails to migrate event channel for new thread: %s\n", strerror_l(errno, default_locale));
					
					if(rdma_destroy_id(thread_param_p->cm_id) != 0)
					{
						fprintf(stderr, "Fails to destroy the communication identifier: %s\n", strerror_l(errno, default_locale));
						continue;
					}

					rdma_destroy_event_channel(thread_param_p->cm_channel);

					free(thread_param_p);
					
					if(rdma_reject(listen_id, NULL, 0) != 0)
					{
						fprintf(stderr, "Fails to reject the connection request: %s\n", strerror_l(errno, default_locale));
						continue;
					}

					continue;
				}
    		}
    		else 
    		{
    			rdma_ack_cm_event(event);
    		}
    	}
    	else
    	{
    		fprintf(stderr, "rdma_get_cm_event fails: %s\n", strerror_l(errno, default_locale));
			continue;
    	}
    }
}

/* the new thread is responsible to free thread_param_p */
void * thread_func(void *arg)
{
	thread_param_t 				*thread_param_p = (thread_param_t *)arg;
	struct ibv_pd				*pd;
	struct ibv_comp_channel		*comp_channel;
	struct ibv_cq				*cq;
	struct ibv_mr				*rev_mr, *send_mr;
	struct ibv_qp_init_attr		qp_attr; 
	locale_t 					default_locale = uselocale((locale_t)0);
	struct ibv_sge				rev_sge, send_sge;
	struct ibv_recv_wr			recv_wr;
	struct ibv_recv_wr			*bad_recv_wr;
	pdata_t						pdata;
	struct rdma_conn_param		conn_param;
	struct rdma_cm_event		*event;
	struct ibv_cq				*evt_cq;
	void 						*cq_context;
	struct ibv_wc				wc;
	struct ibv_send_wr			send_wr;
	struct ibv_send_wr 			*bad_send_wr;
	byte						errmsg = 0;
	message_t					*msg;
	hashmap_t					*map = create_hashmap();
	blob_t						*key_blob = NULL, *data_blob = NULL;
	uint32_t					key_len, data_len;
	reply_t						*reply;
	int 						has_qp_created = 0;

	byte						*rev_buf = NULL, *send_buf = NULL;

	pd = ibv_alloc_pd(thread_param_p->cm_id->verbs);
	if(pd == NULL)
	{
		fprintf(stderr, "ibv_alloc_pd fails.\n");
		errmsg = PD_ALLOC_FAILURE;
		goto clean_exit;
	}

	comp_channel = ibv_create_comp_channel(thread_param_p->cm_id->verbs);
	if(comp_channel == NULL)
	{
		fprintf(stderr, "ibv_create_comp_channel fails.\n");
		errmsg = COMP_CHAN_CREATION_FAILURE;
		goto clean_exit;
	}

	cq = ibv_create_cq(thread_param_p->cm_id->verbs, CQE, NULL, comp_channel, 0);
	if(cq == NULL)
	{
		fprintf(stderr, "ibv_create_cq fails.\n");
		errmsg = CQ_CREATION_FAILURE;
		goto clean_exit;
	}

	if(ibv_req_notify_cq(cq, 0) != 0)
	{
		fprintf(stderr, "ibv_req_notify_cq fails: %s\n", strerror_l(errno, default_locale));
		errmsg = ARM_NOTIFY_FAILURE_BEF_ACC;
		goto clean_exit;
	}

	rev_buf = (byte *)malloc(MAX_SERVER_REV_BUF_SIZE);
	if(rev_buf == NULL)
	{
		fprintf(stderr, "Fails to allocate memory for receiving messages.\n");
		errmsg = MALLOC_FAILURE;
		goto clean_exit;
	}

	send_buf = (byte *)malloc(MAX_SERVER_SED_BUF_SIZE);
	if(send_buf == NULL)
	{
		fprintf(stderr, "Fails to allocate memory for sending messages.\n");
		errmsg = MALLOC_FAILURE;
		goto clean_exit;
	}

	reply = (reply_t *)send_buf;

	rev_mr = ibv_reg_mr(pd, rev_buf, MAX_SERVER_REV_BUF_SIZE, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
	if(rev_mr == NULL)
	{
		fprintf(stderr, "Memory Registry for receiving messages fails.\n");
		errmsg = MEM_REG_FAILURE;
		goto clean_exit;
	}

	send_mr = ibv_reg_mr(pd, send_buf, MAX_SERVER_SED_BUF_SIZE, IBV_ACCESS_LOCAL_WRITE);
	if(send_mr == NULL)
	{
		fprintf(stderr, "Memory Registry for sending messages fails.\n");
		errmsg = MEM_REG_FAILURE;
		goto clean_exit;
	}

	memset(&qp_attr, 0, sizeof(struct ibv_qp_init_attr));
	qp_attr.cap.max_send_wr = 2; 
    qp_attr.cap.max_send_sge = 1; 
    qp_attr.cap.max_recv_wr = 2; 
    qp_attr.cap.max_recv_sge = 1; 
    qp_attr.send_cq = cq; 
    qp_attr.recv_cq = cq; 
    qp_attr.qp_type = IBV_QPT_RC; 
    if(rdma_create_qp(thread_param_p->cm_id, pd, &qp_attr) != 0)
    {
    	fprintf(stderr, "rdma_create_qp fails: %s\n", strerror_l(errno, default_locale));
    	errmsg = QP_CREATION_FAILURE;
		goto clean_exit;
    }
    has_qp_created = 1;

    memset(&recv_wr, 0, sizeof(struct ibv_recv_wr));
    memset(&conn_param, 0, sizeof(struct rdma_conn_param));
    
    rev_sge.addr    = (uintptr_t)rev_buf;
    rev_sge.length  = MAX_SERVER_REV_BUF_SIZE;
    rev_sge.lkey    = rev_mr->lkey;
    recv_wr.sg_list = &rev_sge;
    recv_wr.num_sge = 1;
    recv_wr.wr_id 	= RECV_WR_ID;
    if (ibv_post_recv(thread_param_p->cm_id->qp, &recv_wr,  &bad_recv_wr) != 0)
    {
    	fprintf(stderr, "ibv_post_recv fails: %s\n", strerror_l(errno, default_locale));
    	errmsg = POST_RECV_FAILURE_BEF_ACC;
		goto clean_exit;
    }

    send_sge.addr 					= (uintptr_t)send_buf;
    //send_sge.length 				= MAX_SERVER_SED_BUF_SIZE;
    send_sge.lkey					= send_mr->lkey;
    send_wr.sg_list 				= &send_sge;
    send_wr.num_sge 				= 1;
    send_wr.wr_id   				= SEND_WR_ID;
    send_wr.opcode					= IBV_WR_RDMA_WRITE_WITH_IMM;
    send_wr.send_flags 				= IBV_SEND_SIGNALED;
    send_wr.wr.rdma.rkey         	= ntohl(thread_param_p->pdata.rkey);
	send_wr.wr.rdma.remote_addr   	= ntohll(thread_param_p->pdata.raddr);

    pdata.raddr = htonll((uintptr_t)rev_buf);
    pdata.rkey = htonl(rev_mr->rkey);
    conn_param.responder_resources = 1;
    conn_param.private_data = &pdata;
    conn_param.private_data_len = sizeof(pdata);

    if(rdma_accept(thread_param_p->cm_id, &conn_param) != 0)
    {
    	fprintf(stderr, "rdma_accept fails: %s\n", strerror_l(errno, default_locale));
    	errmsg = ACC_FAILURE;
		goto clean_exit;
    }
/* the latter should be disconnect */
    if(rdma_get_cm_event(thread_param_p->cm_channel, &event) != 0)
    {
    	fprintf(stderr, "rdma_get_cm_event fails: %s\n", strerror_l(errno, default_locale));
    	errmsg = GET_CHAN_EVENT_FAILURE;
		goto clean_exit;
    }

    if (event->event != RDMA_CM_EVENT_ESTABLISHED)
    {
    	fprintf(stderr, "event is not ESTABLISHED.\n");
    	errmsg = EVENT_UNMATCH;
    	rdma_ack_cm_event(event);
    	goto clean_exit;
    }

    rdma_ack_cm_event(event);

    while(1)
    {
    	if(ibv_get_cq_event(comp_channel, &evt_cq, &cq_context) != 0)
    	{
    		fprintf(stderr, "ibv_get_cq_event fails: %s\n", strerror_l(errno, default_locale));
    		errmsg = GET_CQ_EVENT_FAILURE;
			goto clean_exit;
    	}

    	ibv_ack_cq_events(evt_cq, 1);

    	if(ibv_req_notify_cq(evt_cq, 0) != 0)
		{
			fprintf(stderr, "ibv_req_notify_cq fails: %s\n", strerror_l(errno, default_locale));
			errmsg = ARM_NOTIFY_FAILURE_AFT_ACC;
			goto clean_exit;
		}

		if(ibv_poll_cq(cq, 1, &wc) == -1)
		{
			fprintf(stderr, "ibv_poll_cq fails.\n");
			errmsg = POLL_CQ_FAILURE;
    		goto clean_exit;
		}

		if (wc.status != IBV_WC_SUCCESS)
		{
			fprintf(stderr, "Receive WC not success.\n");
			errmsg = WC_UNSUCCESSFUL;
    		goto clean_exit;
		}

		msg = (message_t *)rev_buf;
		switch(msg->cmd)
		{
			case CMD_PUT:
			{
				/*TODO*/
				/*
				uint64_t key_len = ntohll(msg->key_len);
				uint64_t data_len = ntohll(msg->data_len);
				char *ch = (char *)msg->key_and_data;
				printf("key: %s\n", ch);
				ch += key_len;
				printf("data: %s\n", ch);
				*/
				key_len = msg->key_len;
				data_len = msg->data_len;
				key_blob = (blob_t *)malloc(sizeof(blob_t) + key_len);
				data_blob = (blob_t *)malloc(sizeof(blob_t) + data_len);
				if(key_blob == NULL || data_blob == NULL)
				{
					fprintf(stderr, "Out of Memory.\n");
					errmsg = MALLOC_FAILURE_AFT_ACC;
    				goto clean_exit;
				}

				key_blob->len = key_len;
				memcpy(key_blob->data, msg->key_and_data, key_len);
				data_blob->len = data_len;
				memcpy(data_blob->data, msg->key_and_data + key_len, data_len);

				if(hashmap_put(map, key_blob, data_blob) != 0)
				{
					fprintf(stderr, "Fails to put data.\n");
					reply->state = ERROR;
				}
				else
				{
					reply->state = SUCCESS;
				}

				send_sge.length = sizeof(uint8_t);

				break;
			}
			case CMD_GET:
			{
				key_len = msg->key_len;
				key_blob = (blob_t *)malloc(sizeof(blob_t) + key_len);
				if(key_blob == NULL)
				{
					fprintf(stderr, "Out of Memory.\n");
					errmsg = MALLOC_FAILURE_AFT_ACC;
    				goto clean_exit;
				}

				key_blob->len = key_len;
				memcpy(key_blob->data, msg->key_and_data, key_len);

				if(hashmap_get(map, key_blob, data_blob) != 0)
				{
					fprintf(stderr, "Fails to get data.\n");
					reply->state = ERROR;
					send_sge.length = sizeof(uint8_t);
				}
				else
				{
					if(data_blob != NULL)
					{
						reply->state = SUCCESS;
						memcpy(&reply->data_len, data_blob, sizeof(blob_t) + data_blob->len);
						send_sge.length = sizeof(reply_t) + reply->data_len;
					}
					else
					{
						reply->state = KET_NOT_FOUND;
						send_sge.length = sizeof(uint8_t);
					}
				}

				break;
			}
			case CMD_DEL:
			{
				key_len = msg->key_len;
				key_blob = (blob_t *)malloc(sizeof(blob_t) + key_len);
				if(key_blob == NULL)
				{
					fprintf(stderr, "Out of Memory.\n");
					errmsg = MALLOC_FAILURE_AFT_ACC;
    				goto clean_exit;
				}

				key_blob->len = key_len;
				memcpy(key_blob->data, msg->key_and_data, key_len);

				if(hashmap_delete(map, key_blob, data_blob) != 0)
				{
					fprintf(stderr, "Fails to delete data.\n");
					reply->state = ERROR;
					send_sge.length = sizeof(uint8_t);
				}
				else
				{
					if(data_blob != NULL)
					{
						reply->state = SUCCESS;
						memcpy(&reply->data_len, data_blob, sizeof(blob_t) + data_blob->len);
						send_sge.length = sizeof(reply_t) + reply->data_len;
					}
					else
					{
						reply->state = KET_NOT_FOUND;
						send_sge.length = sizeof(uint8_t);
					}
				}

				break;
			}
			case CMD_DISCONNECT:
			{
				goto clean_exit;
			}
			default:
			{
				fprintf(stderr, "Unknown request command.\n");
				reply->state = ERROR;
				send_sge.length = sizeof(uint8_t);
			}
		}

		if (ibv_post_recv(thread_param_p->cm_id->qp, &recv_wr,  &bad_recv_wr) != 0)
    	{
    		fprintf(stderr, "ibv_post_recv fails: %s\n", strerror_l(errno, default_locale));
    		errmsg = POST_RECV_FAILURE_AFT_ACC;
			goto clean_exit;
   	 	}

   	 	if(ibv_post_send(thread_param_p->cm_id->qp, &send_wr, &bad_send_wr) != 0)
   	 	{
   	 		fprintf(stderr, "ibv_post_recv fails: %s\n", strerror_l(errno, default_locale));
    		errmsg = POST_SEND_FAILURE;
			goto clean_exit;
   	 	}

   	 	if(ibv_get_cq_event(comp_channel, &evt_cq, &cq_context) != 0)
    	{
    		fprintf(stderr, "ibv_get_cq_event fails: %s\n", strerror_l(errno, default_locale));
    		errmsg = GET_CQ_EVENT_FAILURE;
			goto clean_exit;
    	}

    	ibv_ack_cq_events(evt_cq, 1);

    	if(ibv_req_notify_cq(evt_cq, 0) != 0)
		{
			fprintf(stderr, "ibv_req_notify_cq fails: %s\n", strerror_l(errno, default_locale));
			errmsg = ARM_NOTIFY_FAILURE_AFT_ACC;
			goto clean_exit;
		}

		if(ibv_poll_cq(cq, 1, &wc) == -1)
		{
			fprintf(stderr, "ibv_poll_cq fails.\n");
			errmsg = POLL_CQ_FAILURE;
    		goto clean_exit;
		}

		if (wc.status != IBV_WC_SUCCESS)
		{
			fprintf(stderr, "Send WC not success.\n");
			errmsg = WC_UNSUCCESSFUL;
    		goto clean_exit;
		}

		if(key_blob != NULL)
		{
			free(key_blob);
			key_blob = NULL;
		}	

		if(data_blob != NULL)
		{
			free(data_blob);
			data_blob = NULL;
		}
    }
/* the following is for testing */
/*
	buf = (uint32_t *)calloc(1, sizeof(uint32_t));
	if(buf == NULL)
	{
		fprintf(stderr, "calloc fails.\n");
		return NULL;
	}

	mr = ibv_reg_mr(pd, buf, 1 * sizeof(uint32_t), IBV_ACCESS_LOCAL_WRITE |IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
	if(mr == NULL)
	{
		fprintf(stderr, "ibv_reg_mr fails.\n");
		return NULL;
	}

	memset(&qp_attr, 0, sizeof(ibv_qp_init_attr));
	qp_attr.cap.max_send_wr = 1; 
    qp_attr.cap.max_send_sge = 1; 
    qp_attr.cap.max_recv_wr = 1; 
    qp_attr.cap.max_recv_sge = 1; 
    qp_attr.send_cq = cq; 
    qp_attr.recv_cq = cq; 
    qp_attr.qp_type = IBV_QPT_RC; 

    if(rdma_create_qp(thread_param_p->cm_id, pd, &qp_attr) != 0)
    {
    	fprintf(stderr, "rdma_create_qp fails: %s\n", strerror_l(errno, default_locale));
		return NULL;
    }

    memset(&recv_wr, 0, sizeof(struct ibv_recv_wr));
    memset(&conn_param, 0, sizeof(struct rdma_conn_param));
    memset(&send_wr, 0, sizeof(struct ibv_send_wr));

    sge.addr    = (uintptr_t)buf;
    sge.length  = sizeof(uint32_t);
    sge.lkey    = mr->lkey;
    recv_wr.sg_list =  &sge;
    recv_wr.num_sge    = 1;
    recv_wr.wr_id = 0;
    if (ibv_post_recv(thread_param_p->cm_id->qp, &recv_wr,  &bad_recv_wr) != 0)
    {
    	fprintf(stderr, "ibv_post_recv fails: %s\n", strerror_l(errno, default_locale));
		return NULL;
    }

    pdata.raddr = htonll((uintptr_t)buf);
    pdata.rkey = htonl(mr->rkey);
    conn_param.responder_resources = 1;
    conn_param.private_data          = &pdata;
    conn_param.private_data_len = sizeof(pdata);

    if(rdma_accept(thread_param_p->cm_id, &conn_param) != 0)
    {
    	fprintf(stderr, "rdma_accept fails: %s\n", strerror_l(errno, default_locale));
		return NULL;
    }

    if(rdma_get_cm_event(thread_param_p->cm_channel, &event) != 0)
    {
    	fprintf(stderr, "rdma_get_cm_event fails: %s\n", strerror_l(errno, default_locale));
		return NULL;
    }

    if (event->event != RDMA_CM_EVENT_ESTABLISHED)
    {
    	fprintf(stderr, "event is not ESTABLISHED.\n");
    	return NULL;
    }

    rdma_ack_cm_event(event);

    if(ibv_get_cq_event(comp_channel, &evt_cq, &cq_context) != 0)
    {
    	fprintf(stderr, "ibv_get_cq_event fails: %s\n", strerror_l(errno, default_locale));
		return NULL;
    }

    ibv_ack_cq_events(evt_cq, 1);

    if(ibv_req_notify_cq(evt_cq, 0) != 0)
	{
		fprintf(stderr, "ibv_req_notify_cq fails: %s\n", strerror_l(errno, default_locale));
		return NULL;
	}

	if(ibv_poll_cq(cq, 1, &wc) == -1)
	{
		fprintf(stderr, "ibv_poll_cq fails.\n");
    	return NULL;
	}

	if (wc.status != IBV_WC_SUCCESS)
	{
		fprintf(stderr, "WC not success.\n");
    	return NULL;
	}

	printf("%"PRIu32"\n", ntohl(buf[0]));
	buf[0] = ntohl(buf[0]);
	buf[0] = buf[0] + 1;
	buf[0] = htonl(buf[0]);

	sge.addr    = (uintptr_t)buf; 
    sge.length  = sizeof(uint32_t); 
    sge.lkey    = mr->lkey; 
                                                              
    send_wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM; 
    send_wr.send_flags = IBV_SEND_SIGNALED; 
    send_wr.sg_list    = &sge;
    send_wr.num_sge = 1;
    send_wr.wr_id = 1;
    send_wr.wr.rdma.rkey          = ntohl(thread_param_p->pdata.rkey);
	send_wr.wr.rdma.remote_addr   = ntohll(thread_param_p->pdata.raddr);

    if (ibv_post_send(thread_param_p->cm_id->qp, &send_wr, &bad_send_wr) != 0)
    {
    	fprintf(stderr, "ibv_req_notify_cq fails: %s\n", strerror_l(errno, default_locale));
		return NULL;
    }

    if (ibv_get_cq_event(comp_channel, &evt_cq, &cq_context) != 0)
        return NULL;
    ibv_ack_cq_events(evt_cq, 1);

    if(ibv_poll_cq(cq, 1, &wc) < 1)
        return NULL;
    if(wc.status != IBV_WC_SUCCESS)
        return NULL;

    printf("thread exits\n");
    return NULL;
*/
clean_exit:
	/* TODO */
	if(has_qp_created)
		rdma_destroy_qp(thread_param_p->cm_id);
	if(rev_mr != NULL)
	{
		if(ibv_dereg_mr(rev_mr) != 0)
		{
			fprintf(stderr, "Fails to deregister memory rev_mr: %s\n", strerror_l(errno, default_locale));
		}
	}
	if(send_mr != NULL)
	{
		if(ibv_dereg_mr(send_mr) != 0)
		{
			fprintf(stderr, "Fails to deregister memory send_mr: %s\n", strerror_l(errno, default_locale));
		}
	}
	if(rev_buf != NULL)
		free(rev_buf);
	if(send_buf != NULL)
		free(send_buf);
	if(key_blob != NULL)
		free(key_blob);	
	if(data_blob != NULL)
		free(data_blob);
	if(cq != NULL)
	{
		if(ibv_destroy_cq(cq) != 0)
		{
			fprintf(stderr, "Fails to destroy CQ: %s\n", strerror_l(errno, default_locale));
		}
	}
	if(comp_channel != NULL)
	{
		if(ibv_destroy_comp_channel(comp_channel) != 0)
		{
			fprintf(stderr, "Fails to destroy completion channel: %s\n", strerror_l(errno, default_locale));
		}
	}
	if(pd != NULL)
	{
		if(ibv_dealloc_pd(pd) != 0)
		{
			fprintf(stderr, "Fails to deallocate PD: %s\n", strerror_l(errno, default_locale));
		}
	}

	if(errmsg != 0)
	{
		if(errmsg < GET_CHAN_EVENT_FAILURE)
		{
			if(rdma_reject(thread_param_p->listen_id, NULL, 0) != 0)
			{
				fprintf(stderr, "Fails to reject the connection request: %s\n", strerror_l(errno, default_locale));
			}
		}
		else
		{
			if(rdma_disconnect(thread_param_p->cm_id) != 0)
			{
				fprintf(stderr, "Fails to disconnect: %s\n", strerror_l(errno, default_locale));
			}
		}
	}

	if(rdma_destroy_id(thread_param_p->cm_id) != 0)
	{
		fprintf(stderr, "Fails to destroy rdma_cm_id: %s\n", strerror_l(errno, default_locale));
	}

	rdma_destroy_event_channel(thread_param_p->cm_channel);
	
	free(thread_param_p);
	
	return NULL;
}