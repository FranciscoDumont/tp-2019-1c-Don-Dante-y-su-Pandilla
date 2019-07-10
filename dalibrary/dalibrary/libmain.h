#ifndef DALIBRARY_MAIN_H_
#define DALIBRARY_MAIN_H_



#define MAX_CONN 40
#define null NULL



#include <commons/collections/list.h>
#include <commons/bitarray.h>
#include <commons/config.h>
#include <commons/log.h>
#include <commons/string.h>



#include <stdarg.h>



#include <stdio.h>
#include <stdlib.h>


#include <sys/inotify.h>
#include <limits.h>

#include <dirent.h>
#include <pthread.h>
#include <unistd.h>
#include <netdb.h>
#include <string.h>
#include <ctype.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/syscall.h>

#include <readline/readline.h>
#include <readline/history.h>

#include "structures.h"



void custom_print(const char* message_template, ...);
void sayhi(char * name);
int create_socket();
int bind_socket(int socket, int port);
int connect_socket(int socket, char * addr, int port);
int close_socket(int socket);



int send_data(int destination, MessageType type, int data_size, void * data_stream);
int recieve_header(int source, MessageHeader * buffer);
int recieve_data(int source, void * buffer, int data_size);



int start_server(int socket,
		void (*new_connection)(int fd, char * ip, int port),
		void (*lost_connection)(int fd, char * ip, int port),
		void (*incoming_message)(int fd, char * ip, int port, MessageHeader * header));



int init_normal_mutex(pthread_mutex_t * mutex, char * name);
int destroy_mutex(pthread_mutex_t * mutex);
int lock_mutex(pthread_mutex_t * mutex);
int unlock_mutex(pthread_mutex_t * mutex);



int inform_thread_id(char * name);
char * get_thread_name(int tid);



unsigned long unix_epoch();
char * consistency_to_char(ConsistencyTypes consistency);
ConsistencyTypes char_to_consistency(char * consistency);

void cargar_comando(comando_t*, char*);
void imprimir_comando(comando_t*);
void vaciar_comando(comando_t* );
void * crear_consola(void (*execute)(comando_t*),char* unString);



void create_lql(LQLScript * script, char * filepath);
Instruction * parse_lql_line(LQLScript * script);
void close_lql(LQLScript * script);

#endif
