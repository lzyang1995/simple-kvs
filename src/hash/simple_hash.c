/*
 * 	These functions are aimed to implement a hashmap for storing key/value pairs.
 * 	The dense_hasp_map in google sparsehash project is a great implemetation of 
 *	hashmap. But it is written in C++, and I am not familiar with C++ ...
 *	So I implement a hashmap from scratch, following the design idea of sparsehash
 *	
 *	reference: https://github.com/sparsehash/sparsehash
 *
 *	Author: Zhiyang Li <lzyang1995@outlook.com>
 */

#include <stdint.h>
#include <stdlib.h>
#include <simple_hash.h>

/*
 *	SuperFastHash: the hash function from Paul Hsieh 
 *	(http://www.azillionmonkeys.com/qed/hash.html)
 *	The hash function is very critical to the performance
 *	of hashmap. I choose this hash function because it has
 *	been tested and is proved to be excellent.
 */
uint32_t SuperFastHash (const char * data, uint32_t len) 
{
	uint32_t hash = len, tmp;
	int rem;

    if (len <= 0 || data == NULL) return 0;

    rem = len & 3;
    len >>= 2;

    /* Main loop */
    for (;len > 0; len--) {
        hash  += get16bits (data);
        tmp    = (get16bits (data+2) << 11) ^ hash;
        hash   = (hash << 16) ^ tmp;
        data  += 2*sizeof (uint16_t);
        hash  += hash >> 11;
    }

    /* Handle end cases */
    switch (rem) {
        case 3: hash += get16bits (data);
                hash ^= hash << 16;
                hash ^= ((signed char)data[sizeof (uint16_t)]) << 18;
                hash += hash >> 11;
                break;
        case 2: hash += get16bits (data);
                hash ^= hash << 11;
                hash += hash >> 17;
                break;
        case 1: hash += (signed char)*data;
                hash ^= hash << 10;
                hash += hash >> 1;
    }

    /* Force "avalanching" of final 127 bits */
    hash ^= hash << 3;
    hash += hash >> 5;
    hash ^= hash << 4;
    hash += hash >> 17;
    hash ^= hash << 25;
    hash += hash >> 6;

    return hash;
}

/*
 *	Create the hashmap.
 *	use NULL to denote unused entry
 *
 *	On success, it returns a pointer to hashmap_t (which should be destroyed with destroy_hashmap())
 * 	On failure, it returns NULL
 */
hashmap_t * create_hashmap()
{
	hashmap_t *map = (hashmap_t *)malloc(sizeof(hashmap_t));
	if(map == NULL)
		return NULL;

	map->size = 0;
	map->table_size = INITIAL_SIZE;
	map->table = (bucket_t *)malloc(sizeof(bucket_t) * INITIAL_SIZE);
	if(map->table == NULL)
	{
		free(map);
		return NULL;
	}

	/* initialize the array, marking all entries as unused */
	memset(map->table, 0, sizeof(bucket_t) * INITIAL_SIZE);

	return map;
}

/*
 *	return the index of key in the table
 *
 *	If key exists, its index will be returned
 *
 *	If key does not exist, and there are empty 
 *	entries in the table, the index that the key
 *	should be at will be returned
 *
 *	If the table is full and key does not exist 
 *	in the table, UINT32_MAX will be returned (this should
 *	never happen since table will be resized when it
 *	is nearly full)
 *
 *	If table or key is NULL, return UINT32_MAX.
 */
uint32_t get_index(bucket_t *table, uint32_t table_size, blob_t *key)
{
	uint32_t hashval, index, interval = 0, increment = 0;
	int flag;

	if(table == NULL || key == NULL)
		return UINT32_MAX;

	hashval = SuperFastHash((char *)key->data, key->len);

	/* the same as index = hashval % table_size */
	index = hashval & (table_size - 1);

	while(table[index].key != NULL && (flag = compare_blob(key, table[index].key)) && interval < table_size)
	{
		/* quadratic internal probing */
		interval++;
		increment += interval;
		index = (index + increment) & (table_size - 1);
	}

	if(table[index].key == NULL || !flag)
		return index;
	else
		return UINT32_MAX;
}

/*
 *	replace the entry numbered index in the table with a new entry
 *	the two params key_blob and data_blob cannot be NULL
 *	and the entry to be replaced cannot be NULL  
 *
 *	success: return 0
 * 	failure: return -1
 */
int replace_entry(bucket_t *table, uint32_t table_size, uint32_t index, blob_t *key_blob, blob_t *data_blob)
{
	if(table == NULL || index >= table_size || key_blob == NULL || data_blob == NULL || table[index].key == NULL || table[index].data == NULL)
		return -1;
	
	/* backup orginal data for restoration if it errs */
	blob_t *old_key_blob = table[index].key;
	blob_t *old_data_blob = table[index].key;

	table[index].key = (blob_t *)malloc(sizeof(blob_t) + key_blob->len);
	table[index].data = (blob_t *)malloc(sizeof(blob_t) + data_blob->len);
	if(table[index].key == NULL || table[index].data == NULL)
	{
		/* restore the entry */
		if(table[index].key != NULL)
		{
			free(table[index].key);
			table[index].key = old_key_blob;
		}
		else
			table[index].key = old_key_blob;

		if(table[index].data != NULL)
		{
			free(table[index].data);
			table[index].data = old_data_blob;
		}
		else
			table[index].data = old_data_blob;
		
		return -1;
	}

	memcpy(table[index].key, key_blob, sizeof(blob_t) + key_blob->len);
	memcpy(table[index].data, data_blob, sizeof(blob_t) + data_blob->len);

	free(old_key_blob);
	free(old_data_blob);

	return 0;
}

/*
 *	insert an entry into the table
 *	the two params key_blob and data_blob cannot be NULL
 *	the entry table[index] must be NULL
 *
 *	success: return 0
 *	failure: return -1
 */
int insert_entry(bucket_t *table, uint32_t table_size, uint32_t index, blob_t *key_blob, blob_t *data_blob)
{
	if(table == NULL || index >= table_size || key_blob == NULL || data_blob == NULL || table[index].key == NULL)
		return -1;

	table[index].key = (blob_t *)malloc(sizeof(blob_t) + key_blob->len);
	table[index].data = (blob_t *)malloc(sizeof(blob_t) + data_blob->len);
	if(table[index].key == NULL || table[index].data == NULL)
	{
		/* restore the entry */
		if(table[index].key != NULL)
		{
			free(table[index].key);
			table[index].key = NULL;
		}

		if(table[index].data != NULL)
		{
			free(table[index].data);
			table[index].data = NULL;
		}

		return -1;
	}

	memcpy(table[index].key, key_blob, sizeof(blob_t) + key_blob->len);
	memcpy(table[index].data, data_blob, sizeof(blob_t) + data_blob->len);

	return 0;
}

/*
 *	return the data of entry table[index]. 
 *	table[index] must not be NULL
 *	If delete is 1, the entry will be deleted after retrival
 *	If delete is 0, the entry will not be deleted
 *
 *	the caller is responsible for freeing the returned pointer
 *
 *	success: return a pointer which points to a blob_t
 *	failure: return NULL
 */
blob_t * get_entry(bucket_t *table, uint32_t table_size, uint32_t index, int delete)
{
	if(table == NULL || index >= table_size || table[index].key == NULL)
		return NULL;

	blob_t *data_blob = (blob_t *)malloc(sizeof(blob_t) + table[index].data->len);
	if(data_blob == NULL)
		return NULL;

	memcpy(data_blob, table[index].data, sizeof(blob_t) + table[index].data->len);

	if(delete)
	{
		free(table[index].key);
		free(table[index].data);
		table[index].key = NULL;
		table[index].data = NULL;
	}

	return data_blob;
}

/*
 *	Increase the size of hashmap.
 *	When the load factor is over 0.7, this function will be called
 *
 *	success: return 0
 *	failure: return -1
 */
int resize(hashmap_t *map)
{
	uint32_t i, hashval, newindex, totalsize;

	if(map == NULL || map->table == NULL)
		return -1;

	bucket_t *temp = (bucket_t *)malloc(map->table_size * 2 * sizeof(bucket_t));
	if(temp == NULL)
		return -1;

	/* initialize the array, marking all entries as unused */
	memset(temp, 0, map->table_size * 2 * sizeof(bucket_t));
	
	for(i = 0;i < map->table_size;i++)
	{
		if(map->table[i].key == NULL)
			continue;
		else
		{
			blob_t *key 		= map->table[i].key;
			blob_t *data 		= map->table[i].data;
			uint32_t newindex 	= get_index(temp, map->table_size * 2, key);

			if(newindex == UINT32_MAX)
				break;

			if(temp[newindex].key != NULL)
			{
				/* this will never happen */
				if(replace_entry(temp, map->table_size * 2, newindex, key, data) == -1)
					break;
			}

			if(insert_entry(temp, map->table_size * 2, newindex, key, data) == -1)
				break;
		}
	}

	if(i < map->table_size)
	{
		uint32_t j;
		for(j = 0;j < map->table_size * 2;j++)
		{
			if(temp[j].key != NULL)
				free(temp[j].key);
			if(temp[j].data != NULL)
				free(temp[j].data);
		}

		free(temp);
		return -1;
	}
	else
	{
		uint32_t j;
		for(j = 0;j < map->table_size;j++)
		{
			if(map->table[j].key != NULL)
				free(map->table[j].key);
			if(map->table[j].data != NULL)
				free(map->table[j].data);
		}

		free(map->table);
		map->table = temp;
		map->table_size *= 2;
		return 0
	}
}

/*
 *	Put an entry into the map
 *
 *	success: return 0
 * 	failure: return -1
 */
int hashmap_put(hashmap_t *map, blob_t *key_blob, blob_t *data_blob)
{
	uint32_t index;

	if(map == NULL || key_blob == NULL || data_blob == NULL || map->table == NULL)
		return -1;
	
	bucket_t *table = map->table;
	/* first, find the index */
	index = get_index(table, map->table_size, key_blob);

	if(index = UINT32_MAX)
		return -1;

	if(table[index].key != NULL)	//the key exists
		return replace_entry(table, map->table_size, index, key_blob, data_blob);

	//the key does not exist
	if(insert_entry(table, map->table_size, index, key_blob, data_blob) == -1)
		return -1;

	/* increase the size */
	map->size++;
	if((double)map->size / map->table_size > LOAD_FACTOR)
		return resize(map); 
}

/*
 *  Get data from the map associated with the key. 
 *
 *	the user is responsible for freeing the *data_blob
 * 	and also key_blob
 *
 *	The key exists: 		return 0, and *data_blob_p points to the data
 *	The key does not exist:	return 0, and *data_blob_p is NULL 
 *	error: 					return -1
 */
int hashmap_get(hashmap_t *map, blob_t *key_blob, blob_t **data_blob_p)
{
	uint32_t index;

	if(key_blob == NULL || map == NULL || map->table == NULL)
		return -1;

	bucket_t *table = map->table;
	index = get_index(table, map->table_size, key_blob);

	if(index == UINT32_MAX)		
	{
		/* this will never happen, in fact */
		*data_blob_p = NULL;
		return 0;
	}

	if(table[index].key != NULL)
	{
		blob_t *data = table[index].data;
		*data_blob_p = (blob_t *)malloc(sizeof(blob_t) + data->len);

		if(*data_blob_p == NULL)
			return -1;

		memcpy(*data_blob_p, data, sizeof(blob_t) + data->len);
		return 0;
	}

	*data_blob_p = NULL;
	return 0;
}

/* equal: return 0
 * unequal: return 1
 */
int compare_blob(blob_t *blob1, blob_t *blob2)
{
	uint32_t i;

	if(blob1 == NULL && blob2 == NULL)
		return 0;
	else if(blob1 == NULL || blob2 == NULL)
		return 1;

	if(blob1->len != blob2->len)
		return 1;
	else
	{
		for(i = 0;i < blob1->len;i++)
		{
			if(blob1->data[i] != blob2->data[i])
				return 1;
		}
	}

	return 0;
}

/*
 *	Delete an entry whose key equals key_blob from the map
 *	Basically the same as hashmap_get except the deletion operation
 *
 *	the user is responsible for freeing the *data_blob
 * 	and also key_blob
 *
 *	The key exists: 		return 0, and *data_blob_p points to the data
 *	The key does not exist:	return 0, and *data_blob_p is NULL 
 *	error: 					return -1
 */
int hashmap_delete(hashmap_t *map, blob_t *key_blob, blob_t **data_blob_p)
{
	uint32_t index;

	if(key_blob == NULL || map == NULL || map->table == NULL)
		return -1;

	bucket_t *table = map->table;
	index = get_index(table, map->table_size, key_blob);

	if(index == UINT32_MAX)		
	{
		/* this will never happen, in fact */
		*data_blob_p = NULL;
		return 0;
	}

	if(table[index].key != NULL)
	{
		blob_t *data = table[index].data;
		*data_blob_p = (blob_t *)malloc(sizeof(blob_t) + data->len);

		if(*data_blob_p == NULL)
			return -1;

		memcpy(*data_blob_p, data, sizeof(blob_t) + data->len);

		/* delete the entry */
		free(table[index].key);
		table[index].key = NULL;
		free(table[index].data);
		table[index].data = NULL;

		/* decrease the size */
		map->table_size--;

		return 0;
	}

	*data_blob_p = NULL;
	return 0;
}

/*
 *	Destroy(Free) the whole hashmap
 */
void destroy_hashmap(hashmap_t *map)
{
	bucket_t *table = map->table;
	uint32_t i;

	for(i = 0;i < map->table_size;i++)
	{
		if(table[i].key == NULL)
			continue;
		else
		{
			free(table[i].key);
			free(table[i].data);
		}
	}

	free(table);
	free(map);
}