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
	HANDSHAKE_NUMBER_GENERATOR_SERVER,
	HANDSHAKE_RESPONSE,
	NEW_TOTAL_WIDTH,
	NEW_OFFSET_RENDER,
	ASCIIFileTransmission,
	WELCOME_SCREEN,
	RANDOM_NUMBER
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



typedef struct _ASCIIFile {
	int ROWQ;
	int COLQ;
	char * matrix;
} ASCIIFile;



#endif /* DALIBRARY_STRUCTURES_H_ */
