/*
 *
 */
#ifndef SIMPLE_HASH_H
#define SIMPLE_HASH_H

#define byte uint8_t;

#define LOAD_FACTOR 0.7
#define INITIAL_SIZE 256

#undef get16bits
#if (defined(__GNUC__) && defined(__i386__)) || defined(__WATCOMC__) \
  || defined(_MSC_VER) || defined (__BORLANDC__) || defined (__TURBOC__)
#define get16bits(d) (*((const uint16_t *) (d)))
#endif

#if !defined (get16bits)
#define get16bits(d) ((((uint32_t)(((const uint8_t *)(d))[1])) << 8)\
                       +(uint32_t)(((const uint8_t *)(d))[0]) )
#endif

struct blob_t 
{
	uint32_t len;
	byte data[0];
} __attribute__ ((packed));
typedef struct blob_t blob_t;

struct bucket_t
{
	struct blob_t *key;
	struct blob_t *data;
} __attribute__ ((packed));
typedef struct bucket_t bucket_t;

struct hashmap_t
{
	uint32_t table_size;
	uint32_t size;
	bucket_t *table;
};
typedef struct hashmap_t hashmap_t;

#endif