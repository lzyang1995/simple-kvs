#include <rdma/rdma_cma.h>
#include <errno.h>
#include <string.h>
#include <locale.h>

#include <simple_kvs.h>

/*
 *	Initialize the connection with the kvs server
 *
 *	params:
 *	addr: the IP address or network hostname of the server
 *	port: the port number of the kvs service
 *
 *	return value:
 *	if failure, NULL is returned
 *	if success, a pointer to db_t structure which contains information of the connection is returned
 *	(the pointer must be destroyed by calling kvsclose())
 *	
 */

db_t * kvsopen(char *addr, char *port)
{
	locale_t 	default_locale = uselocale((locale_t)0);
	int 	 	rv;
	db_t     	*db = (db_t *)malloc(sizeof(db_t));

	if(db == NULL)
		return NULL;

	memset(db, 0, sizeof(db_t));

	/* create event channel */
	db->cm_channel = rdma_create_event_channel(); 
	if(db->cm_channel == NULL)
	{
		fprintf(stderr, "Fails to create event channel: %s\n", strerror_l(errno, default_locale));
		kvsclose(db);
		return NULL;
	}

	/* create cm id */
	if(rdma_create_id(db->cm_channel, &db->cm_id, NULL, RDMA_PS_TCP) != 0)
	{
		fprintf(stderr, "Fails to create communication identifier: %s\n", strerror_l(errno, default_locale));
		kvsclose(db);
		return NULL;
	}
	
	/* get address info */
	struct addrinfo		*res; 
    struct addrinfo		hints = { 
    	.ai_family    = AF_INET, 
       	.ai_socktype  = SOCK_STREAM 
    };
	rv = getaddrinfo(addr, port, &hints, &res);
	if(rv != 0)
	{
		fprintf(stderr, "Fails to get address information: %s\n", gai_strerror(rv));
		kvsclose(db);
		return NULL;
	}

	/* Resolve the address and route */
	resolve(db, res);
}