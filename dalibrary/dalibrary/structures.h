#ifndef DALIBRARY_STRUCTURES_H_
#define DALIBRARY_STRUCTURES_H_



#include "libmain.h"



typedef enum _NetworkDebugLevel {
	NW_NO_DISPLAY,
	NW_NETWORK_ERRORS,
	NW_ALL_DISPLAY
} NetworkDebugLevel;
NetworkDebugLevel NETWORK_DEBUG_LEVEL = NW_NETWORK_ERRORS;

typedef enum _MutexDebugLevel {
	MX_NO_DISPLAY,
	MX_MUTEX_ERROR,
	MX_ONLY_LOCK_UNLOCK,
	MX_ALL_DISPLAY
} MutexDebugLevel;
MutexDebugLevel MUTEX_DEBUG_LEVEL = MX_ONLY_LOCK_UNLOCK;



typedef enum _MessageType {
	HANDSHAKE_RESPONSE
} MessageType;

typedef struct _MessageHeader {
	MessageType type;
	int data_size;
} MessageHeader;



typedef struct _TIDC {
	int tid;
	char * name;
} TIDC;
t_list * ThreadsList = null;

typedef struct _MIDC {
	pthread_mutex_t * mutex_addr;
	char * name;
} MIDC;
t_list * MutexsList = null;



//Lissandra File System
typedef struct _LFSConfig {
	int port;
	char * mounting_point;
	int delay;
	int value_size;
	int dump_delay;
} LFSConfig;



//Memory
typedef struct _MEMConfig {
	int port;
	char * lfs_ip;
	int lfs_port;
	char ** seeds_ips;
	char ** seeds_ports;
	int access_delay;
	int lfs_delay;
	int memsize;
	int journal_time;
	int gossiping_time;
	int memory_id;
} MEMConfig;



//Kernel
typedef struct _KNLConfig {
	char * a_memory_ip;
	int a_memory_port;
	int quamtum;
	int multiprocessing_grade;
	int metadata_refresh;
	int exec_delay;
} KNLConfig;



#endif /* DALIBRARY_STRUCTURES_H_ */
