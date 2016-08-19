/*
 *
 */

#include <stdint.h>
#include <stdlib.h>
#include <simple_hash.h>

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
/*use NULL to denote unused entry*/
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

	memset(map->table, 0, INITIAL_SIZE);

	return map;
}

int resize(hashmap_t *map)
{
	//uint32_t old_table_size = map->table_size;
	//bucket_t *old_table = map->table;
	uint32_t i, hashval, newindex, totalsize;

	bucket_t *temp = (bucket_t *)malloc(old_table_size * 2);
	if(temp == NULL)
		return -1;

	memset(temp, 0, old_table_size * 2);
	//map->table = temp;
	//map->table_size = old_table_size * 2;
	for(i = 0;i < map->table_size;i++)
	{
		if(map->table[i]->key == NULL)
			continue;
		else
		{
			hashval = SuperFastHash((char *)old_table[i]->key->data, old_table[i]->key->len);
			newindex = hashval & (map->table_size * 2 - 1);

			totalsize = sizeof(blob_t) + map->table[i]->key->len;
			temp[newindex].key = (blob_t *)malloc(totalsize);
			if(temp[newindex].key == NULL)
			{
				goto resize_exit;
			}
			memcpy(temp[newindex].key, map->table[i]->key, totalsize);

			totalsize = sizeof(blob_t) + map->table[i]->data->len;
			temp[newindex].data = (blob_t *)malloc(totalsize);
			if(temp[newindex].data == NULL)
			{
				goto resize_exit;
			}
			memcpy(map->table[newindex].data, old_table[i]->data, totalsize);
		}
	}

	for(i = 0;i < old_table_size;i++)
	{
		if(old_table[i]->key == NULL)
			continue;
		else
		{
			free(old_table[i]->key);
			free(old_table[i]->data);
		}
	}
	free(old_table);
	return 0;

resize_exit:
	for(i = 0;i < )
}

int hashmap_put(hashmap_t *map, blob_t *key_blob, blob_t *data_blob)
{
	uint32_t hashval, index, interval = 0, increment = 0;

	if(map == NULL || key_blob == NULL || data_blob == NULL)
		return -1;

	byte *key = key_blob->data;
	uint32_t key_len = key_blob->len;
	
	
	byte *data = data_blob->data;
	uint32_t data_len = data_blob->len;
	
	/* first, find the index */

	hashval = SuperFastHash((char *)key, key_len);
	index = hashval % (map->table_size);
	while(map->table[index].key != NULL && compare_key(key_blob, map->table[index].key) && interval < map->table_size)
	{
		/* quadratic internal probing */
		interval++;
		increment += interval;
		index = (index + increment) % (map->table_size);
	}

	if(interval >= map->table_size)
		return -1;

	if(map->table[index].key == NULL)
	{
		/* resize */
		if((double)(map->size + 1) / map->table_size > LOAD_FACTOR)
		{
			if(resize(map) != 0)
				return -1;
		}

		map->table[index].key = (blob_t *)malloc(sizeof(blob_t) + key_len);
		if(map->table[index].key == NULL)
			return -1;
		map->table[index].key->len = key_len;
		memcpy(map->table[index].key->data, key, key_len);

		map->table[index].data = (blob_t *)malloc(sizeof(blob_t) + data_len);
		if(map->table[index].data == NULL)
		{
			free(map->table[index].key);
			map->table[index].key = NULL;
			return -1;
		}
		map->table[index].data->len = data_len;
		memcpy(map->table[index].data->data, data, data_len);

		map->size++;

		return 0;
	}

	if(!compare_key(key_blob, map->table[index].key))
	{
		free(map->table[index].data);
		map->table[index].data = (blob_t *)malloc(sizeof(blob_t) + data_len);
		memcpy(map->table[index].data, data_blob, sizeof(blob_t) + data_len);

		return 0;
	}

	return -1;
}

/* the user is responsible for freeing data_blob
 * and also key_blob
 */
int hashmap_get(hashmap_t *map, blob_t *key_blob, blob_t *data_blob)
{
	uint32_t hashval, index, interval = 0, increment = 0, totalsize;

	if(key_blob == NULL || map == NULL)
		return -1;

	byte *key = key_blob->data;
	uint32_t key_len = key_blob->len;
	data_blob = NULL;

	hashval = SuperFastHash((char *)key, key_len);
	index = hashval % (map->table_size);

	while(compare_key(key_blob, map->table[index].key) && interval < map->table_size)
	{
		if(map->table[index].key == NULL)
			return -1;
		interval++;
		increment += interval;
		index = (index + increment) % (map->table_size);
	}

	if(interval >= map->table_size)
		return -1;

	totalsize = map->table[index].data->len + sizeof(blob_t);
	data_blob = (blob_t *)malloc(totalsize);
	if(data_blob == NULL)
		return -1;
	memcpy(data_blob, map->table[index].data, totalsize);

	return 0;
}

/* equal: return 0
 * unequal: return 1
 */
int compare_key(blob_t *key1, blob_t *key2)
{
	uint32_t i;

	if(key1 == NULL && key2 == NULL)
		return 0;
	else if(key1 == NULL || key2 == NULL)
		return 1;

	if(key1->len != key2->len)
		return 1;
	else
	{
		for(i = 0;i < key1->len;i++)
		{
			if(key1->data[i] != key2->data[i])
				return 1;
		}
	}

	return 0;
}

int hashmap_delete(hashmap_t *map, blob_t *key_blob)
{
	return hashmap_put(map, key_blob, NULL);
}