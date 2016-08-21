#include <rdma/rdma_cma.h>
#include <errno.h>
#include <string.h>
#include <locale.h>
#include <stdint.h>

#include <simple_kvs.h>

/*
 *	Initialize the connection with the kvs server
 *
 *	params:
 *	addr: 		the IP address or network hostname of the server
 *	port: 		the port number of the kvs service
 *	buf_size: 	the size of RDMA write and receive buffer (in bytes)
 *
 *	return value:
 *	if failure, NULL is returned
 *	if success, a pointer to db_t structure which contains information of the connection is returned
 *	(the pointer must be destroyed by calling kvsclose())
 *	
 */

db_t * kvsopen(char *addr, char *port, uint32_t buf_size)
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
		free_resourses(db);
		return NULL;
	}

	/* create cm id */
	if(rdma_create_id(db->cm_channel, &db->cm_id, NULL, RDMA_PS_TCP) != 0)
	{
		fprintf(stderr, "Fails to create communication identifier: %s\n", strerror_l(errno, default_locale));
		free_resourses(db);
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
		free_resourses(db);
		return NULL;
	}

	/* Resolve the address and route */
	if(resolve(db, res) != 0)
	{
		fprintf(stderr, "Fails to resolve the RDMA address and route: %s\n", strerror_l(errno, default_locale));
		free_resourses(db);
		return NULL;
	}

	/* setup_resources */
	if(setup_resources(db, buf_size) != 0)
	{
		fprintf(stderr, "Fails to setup resourses for communication: %s\n", strerror_l(errno, default_locale));
		free_resourses(db);
		return NULL;
	}

	/* connect to server */
	if(connect_server(db) != 0)
	{
		fprintf(stderr, "Fails to connect to server: %s\n", strerror_l(errno, default_locale));
		free_resourses(db);
		return NULL;
	}
	
	return db;
}

/*
 *	Get data associated with key
 *	
 *	params:
 *	db: 	the pointer returned by kvsopen()
 *	key:	the key
 *	data:	used to store the data retrieved
 *
 *	return value:
 *	if success, 0 is returned. 
 *		if the key is not found, data.data will be NULL
 *		if the key is found, data.len will denote the length of data(in bytes), 
 *		and data.data will point to the data, and caller is responsible for freeing it.
 *	if failure, -1 is returned. data.data will be NULL.
 */

int kvsget(db_t *db, entry_t *key, entry_t *data)
{	
	
}

int kvsclose(db_t *db)
{
	//send CMD_DISCONNECT to server
	//free resourses
}