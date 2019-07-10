#ifndef DALIBRARY_STRUCTURES_H_
#define DALIBRARY_STRUCTURES_H_
#define IP_LENGTH 20



#include "libmain.h"



typedef enum _NetworkDebugLevel {
	NW_NO_DISPLAY,
	NW_NETWORK_ERRORS,
	NW_ALL_DISPLAY
} NetworkDebugLevel;
NetworkDebugLevel NETWORK_DEBUG_LEVEL = NW_NO_DISPLAY;

typedef enum _MutexDebugLevel {
	MX_NO_DISPLAY,
	MX_MUTEX_ERROR,
	MX_ONLY_LOCK_UNLOCK,
	MX_ALL_DISPLAY
} MutexDebugLevel;
MutexDebugLevel MUTEX_DEBUG_LEVEL = MX_NO_DISPLAY;



typedef enum _MessageType {
	HANDSHAKE_RESPONSE,
	GOSSIPING_REQUEST,

	HANDSHAKE_MEM_LFS,
	HANDSHAKE_MEM_LFS_OK,

	OPERATION_SUCCESS,

	MEM_LFS_CREATE,
	CREATE_FAILED_EXISTENT_TABLE,

	MEM_LFS_SELECT,
	SELECT_FAILED_NO_TABLE_SUCH_FOUND,
	SELECT_FAILED_NO_RESULT,

	MEM_LFS_INSERT,

	MEM_LFS_DESCRIBE,
	MEM_LFS_NO_TABLES,

	MEM_LFS_DROP,

	//KNL_MEM
	KNL_MEM_CREATE,
	KNL_MEM_SELECT,
	KNL_MEM_INSERT,
	KNL_MEM_DESCRIBE,
	KNL_MEM_NO_TABLES,
	KNL_MEM_DROP,
	KNL_MEM_JOURNAL,
	MEM_KNL_JOURNAL_OK,
	KNL_MEM_DESCRIBE_METADATA,
	VALUE_SIZE_ERROR,
	OPERATION_FAILURE,

	GIVE_ME_YOUR_METRICS

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
	int mysocket;
} LFSConfig;

typedef struct _LFSMetadata {
	int block_size;
	int blocks;
	char * magic_number;

	char * metadatapath;
	char * bitarraypath;

	t_bitarray * bitarray;
} LFSMetadata;

typedef enum _ConsistencyTypes {
	STRONG_CONSISTENCY,
	EVENTUAL_CONSISTENCY,
	STRONG_HASH_CONSISTENCY,
	C_UNKNOWN
} ConsistencyTypes;

typedef struct _LFSFileStruct {
	int size;
	t_list * blocks;
} LFSFileStruct;

typedef struct _MemtableKeyReg {
	unsigned long timestamp;
	int key;
	char * value;
} MemtableKeyReg;

typedef struct _MemtableTableReg {
	char * table_name;
	ConsistencyTypes consistency;
	int partitions;
	int compaction_time;
	t_list * records;
	t_list * dumping_queue;
	t_list * temp_c;
} MemtableTableReg;



//Memory
typedef struct _MEMConfig {
	int port;
	char * lfs_ip;
	int lfs_port;
	char ** seeds_ips;
	char ** seeds_ports;
	int seeds_q;
	int access_delay;
	int lfs_delay;
	int memsize;
	int journal_time;
	int gossiping_time;
	int memory_id;
	int mysocket;
	int lfs_socket;
	int value_size;
} MEMConfig;

typedef struct _MemPoolData {
	char * ip;
	int port;
	int memory_id;
} MemPoolData;

typedef enum _InstructionType{
	SELECT,
	INSERT,
	CREATE,
	DESCRIBE,
	DROP,
	JOURNAL,

	UNKNOWN_OP_TYPE
}InstructionType;

typedef struct _Instruction {
	InstructionType i_type;
	char * table_name;
	int key;
	char * value;
	ConsistencyTypes c_type;
	int partitions;
	unsigned long compaction_time;
	unsigned long timestamp;

	char * knl_line;
} Instruction;

//Kernel
typedef struct _KNLConfig {
	char * a_memory_ip;
	int a_memory_port;
	int quantum;
	int multiprocessing_grade;
	int metadata_refresh;
	int exec_delay;

	int current_multiprocessing;
} KNLConfig;

typedef struct {
	char comando[20];
	char parametro[5][20];
} comando_t;

typedef enum {
	NEW,
	READY,
	EXEC,
	EXIT
} LQLState;

typedef struct _lql {
	FILE * file;
	char * line;
	size_t len;
	ssize_t read;
	LQLState state;
	char * lql_name;
	int quantum_counter;
	unsigned int lqlid;
} LQLScript;

typedef struct _consistency_criterion {
	ConsistencyTypes type;
	t_list * memories;
	int rr_next_to_use;
	unsigned long metrics_start_measure;
	int read_acum_times;
	int read_acum_count;
	int write_acum_times;
	int write_acum_count;
} ConsistencyCriterion;


#endif /* DALIBRARY_STRUCTURES_H_ */
