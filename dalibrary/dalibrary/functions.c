#include "libmain.h"



void sayhi(char * name) {
	printf("Hola %s", name);
}

int create_socket() {
	int fd;
	if ((fd=socket(AF_INET, SOCK_STREAM, 0)) == -1) {
		if(NETWORK_DEBUG_LEVEL >= NW_NETWORK_ERRORS) {
			printf("[NETWORK_ERROR][ERROR_CREATING_SOCKET]\n");
		}
		return -1;
	} else {
		int option = 1;
		setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &option, sizeof(option));
		if(NETWORK_DEBUG_LEVEL >= NW_ALL_DISPLAY) {
			printf("[NETWORK_INFO][SOCKET_CREATED_%d]\n", fd);
		}
		return fd;
	}
}

int bind_socket(int socket, int port) {
	struct sockaddr_in server;
	int bindres;

	server.sin_family = AF_INET;
	server.sin_port = htons(port);
	server.sin_addr.s_addr = INADDR_ANY;
	bzero(&(server.sin_zero), 8);

	bindres = bind(socket, (struct sockaddr*)&server,
		sizeof(struct sockaddr));
	if(bindres != 0) {
		if(NETWORK_DEBUG_LEVEL >= NW_NETWORK_ERRORS) {
			printf("[NETWORK_ERROR][ERROR_BINDING_SOCKET_TO_%d]\n", port);
		}
	} else {
		if(NETWORK_DEBUG_LEVEL >= NW_ALL_DISPLAY) {
			printf("[NETWORK_INFO][SOCKET_BINDED_%d_TO_%d]\n", socket, port);
		}
	}
	return bindres;
}

int connect_socket(int socket, char * addr, int port) {
	struct hostent * he;
	struct sockaddr_in server;

	if ((he=gethostbyname(addr)) == NULL) {
		if(NETWORK_DEBUG_LEVEL >= NW_NETWORK_ERRORS) {
			printf("[NETWORK_ERROR][ERROR_FINDING_HOST_%s]\n", addr);
		}
		return -1;
	}
	if(NETWORK_DEBUG_LEVEL >= NW_ALL_DISPLAY) {
		printf("[NETWORK_INFO][CONNECTING_SOCKET_HOST_FOUND_%s]\n", addr);
	}

	server.sin_family = AF_INET;
	server.sin_port = htons(port);

	server.sin_addr = *((struct in_addr *)he->h_addr);

	bzero(&(server.sin_zero), 8);

	if (connect(socket, (struct sockaddr *)&server, sizeof(struct sockaddr)) == -1){
		if(NETWORK_DEBUG_LEVEL >= NW_NETWORK_ERRORS) {
			printf("[NETWORK_ERROR][ERROR_CONNECTING_TO_%s:%d]\n", addr, port);
		}
		return -1;
	}
	if(NETWORK_DEBUG_LEVEL >= NW_ALL_DISPLAY) {
		printf("[NETWORK_INFO][SOCKET_CONNECTED_%d_%s:%d]\n", socket, addr, port);
	}
	return 0;
}

int close_socket(int socket) {
	close(socket);
	if(NETWORK_DEBUG_LEVEL >= NW_ALL_DISPLAY) {
		printf("[NETWORK_INFO][SOCKET_CLOSED_%d\n]", socket);
	}
	return 0;
}



int send_data(int destination, MessageType type, int data_size, void * data_stream) {
	MessageHeader * header = malloc(sizeof(MessageHeader));
	int sent, header_sent, data_sent;

	header->type = type;
	header->data_size = data_size;

	sent = send(destination, header, sizeof(MessageHeader), 0);
	if (sent == -1) {
		if(NETWORK_DEBUG_LEVEL >= NW_NETWORK_ERRORS) {
			printf("[NETWORK_ERROR][ERROR_SENDING_HEADER_TO_%d]\n", destination);
		}
		return sent;
	} else {
		if(NETWORK_DEBUG_LEVEL >= NW_ALL_DISPLAY) {
			printf("[NETWORK_INFO][HEADER_SENT_TO_%d_(%d_bytes)]\n", destination, sent);
		}
	}
	header_sent = sent;
	if(data_size > 0) {
		data_sent = send(destination, data_stream, data_size, 0);
		if (sent == -1) {
			if(NETWORK_DEBUG_LEVEL >= NW_NETWORK_ERRORS) {
				printf("[NETWORK_ERROR][ERROR_SENDING_DATA_STREAM_TO_%d]\n", destination);
			}
			return sent;
		} else {
			if(NETWORK_DEBUG_LEVEL >= NW_ALL_DISPLAY) {
				printf("[NETWORK_INFO][DATA_STREAM_SENT_TO_%d_(%d_bytes)]\n", destination, data_sent);
			}
			sent += data_sent;
		}
	} else {
		if(NETWORK_DEBUG_LEVEL >= NW_ALL_DISPLAY) {
			printf("[NETWORK_INFO][NO_STREAM_TO_SEND]\n", destination);
		}
	}

	return sent;
}

int recieve_header(int source, MessageHeader * buffer) {
	int rec;
	rec = recv(source, buffer, sizeof(MessageHeader), 0);
	if (rec > 0) {
		if(NETWORK_DEBUG_LEVEL >= NW_ALL_DISPLAY) {
			printf("[NETWORK_INFO][HEADER_RECIEVED_FROM_%d_(%d_bytes)]\n", source, rec);
		}
	} else {
		if(NETWORK_DEBUG_LEVEL >= NW_NETWORK_ERRORS) {
			printf("[NETWORK_ERROR][ERROR_RECIEVING_HEADER_FROM_%d]\n", source);
		}
	}
	return rec;
}

int recieve_data(int source, void * buffer, int data_size) {
	int rec;
	rec = recv(source, buffer, sizeof(data_size), 0);
	if (rec > 0) {
		if(NETWORK_DEBUG_LEVEL >= NW_ALL_DISPLAY) {
			printf("[NETWORK_INFO][DATA_RECIEVED_FROM_%d_(%d_bytes)]\n", source, rec);
		}
	} else {
		if(NETWORK_DEBUG_LEVEL >= NW_NETWORK_ERRORS) {
			printf("[NETWORK_ERROR][ERROR_RECIEVING_DATA_FROM_%d]\n", source);
		}
	}
	return rec;
}



int start_server(int socket,
		void (*new_connection)(int fd, char * ip, int port),
		void (*lost_connection)(int fd, char * ip, int port),
		void (*incoming_message)(int fd, char * ip, int port, MessageHeader * header)) {

	int addrlen, new_socket ,client_socket_array[MAX_CONN], activity, i, bytesread, sd;
	int max_sd;
	struct sockaddr_in address;
	fd_set readfds;

	MessageHeader * incoming;

	for (i = 0; i < MAX_CONN; i++) {
		client_socket_array[i] = 0;
	}

	if (listen(socket, MAX_CONN) < 0) {
		if(NETWORK_DEBUG_LEVEL >= NW_NETWORK_ERRORS) {
			printf("[NETWORK_ERROR][ERROR_LISTENING_FD_%d]\n", socket);
		}
		return -1;
	}
	if(NETWORK_DEBUG_LEVEL >= NW_ALL_DISPLAY) {
		printf("[NETWORK_INFO][SOCKET_NOW_LISTENING_%d]\n", socket);
	}

	addrlen = sizeof(address);

	while(1) {
		FD_ZERO(&readfds);

		FD_SET(socket, &readfds);
		max_sd = socket;

		for (i = 0 ; i < MAX_CONN ; i++) {
			sd = client_socket_array[i];
			if (sd > 0){
				FD_SET( sd , &readfds);
			}
			if (sd > max_sd){
				max_sd = sd;
			}
		}

		if(NETWORK_DEBUG_LEVEL >= NW_ALL_DISPLAY) {
			printf("[NETWORK_INFO][SOCKET_AWAITING_CONNECTION_%d]\n", socket);
		}
		activity = select(max_sd + 1, &readfds, NULL, NULL, NULL);

		if (activity < 0) {
			if(NETWORK_DEBUG_LEVEL >= NW_NETWORK_ERRORS) {
				printf("[NETWORK_ERROR][ERROR_SELECTING_FD_%d]\n", socket);
			}
		}

		if (FD_ISSET(socket, &readfds)) {
			if ((new_socket = accept(socket,
					(struct sockaddr *)&address, (socklen_t*)&addrlen)) < 0) {
				if(NETWORK_DEBUG_LEVEL >= NW_NETWORK_ERRORS) {
					printf("[NETWORK_ERROR][ERROR_ACCEPTING_SOCKET_%d_%s:%d]\n",
							new_socket, inet_ntoa(address.sin_addr), ntohs(address.sin_port));
				}
			}
				if(NETWORK_DEBUG_LEVEL >= NW_ALL_DISPLAY) {
					printf("[NETWORK_INFO][NEW_CONNECTION_%d_%d_%s:%d]\n", socket, new_socket,
							inet_ntoa(address.sin_addr), ntohs(address.sin_port));
				}
				new_connection(new_socket, inet_ntoa(address.sin_addr), ntohs(address.sin_port));

				for (i = 0; i < MAX_CONN; i++) {
					if (client_socket_array[i] == 0) {
						client_socket_array[i] = new_socket;
						break;
					}
				}
		}

		for (i = 0; i < MAX_CONN; i++) {
			sd = client_socket_array[i];

			if (FD_ISSET(sd, &readfds)) {
				int client_socket = sd;

				incoming = malloc(sizeof(MessageHeader));

				if ((bytesread = read(client_socket, incoming, sizeof(MessageHeader))) <= 0) {
					getpeername(sd , (struct sockaddr*)&address , (socklen_t*)&addrlen);

					if(NETWORK_DEBUG_LEVEL >= NW_ALL_DISPLAY) {
						printf("[NETWORK_INFO][LOST_CONNECTION_%d_%d_%s:%d]\n", socket, client_socket,
								inet_ntoa(address.sin_addr), ntohs(address.sin_port));
					}
					lost_connection(client_socket, inet_ntoa(address.sin_addr) , ntohs(address.sin_port));

					close(sd);
					client_socket_array[i] = 0;
				} else {

					if(NETWORK_DEBUG_LEVEL >= NW_ALL_DISPLAY) {
						printf("[NETWORK_INFO][INCOMING_MESSAGE_%d_%d]\n", socket, client_socket);
					}
					incoming_message(client_socket, inet_ntoa(address.sin_addr) , ntohs(address.sin_port), incoming);

				}

				free(incoming);
			}
		}
	}
}



int init_normal_mutex(pthread_mutex_t * mutex, char * name) {
	int res;

	if(MutexsList == null) { MutexsList = list_create(); }

	MIDC * node = malloc(sizeof(MIDC));
	node->name = name;
	node->mutex_addr = mutex;

	res = pthread_mutex_init(mutex, NULL);
	if (res != 0) {
		if(MUTEX_DEBUG_LEVEL >= MX_MUTEX_ERROR) {
			printf("[MUTEX_ERROR][ERROR_MUTEX_INIT]\n");
		}
	} else {
		if(MUTEX_DEBUG_LEVEL >= MX_ALL_DISPLAY) {
			printf("[MUTEX_INFO][MUTEX_INIT_OK]\n");
		}
		list_add(MutexsList, node);
	}
	return res;
}

int destroy_mutex(pthread_mutex_t * mutex) {
	int res;

	if(MutexsList == null) { MutexsList = list_create(); }

	res = pthread_mutex_destroy(mutex);
	if (res != 0) {
		if(MUTEX_DEBUG_LEVEL >= MX_MUTEX_ERROR) {
			printf("[MUTEX_ERROR][ERROR_MUTEX_DESTROY]\n");
		}
	} else {
		if(MUTEX_DEBUG_LEVEL >= MX_ALL_DISPLAY) {
			printf("[MUTEX_INFO][MUTEX_INIT_DESTROY]\n");
		}
	}
	return res;
}

int lock_mutex(pthread_mutex_t * mutex) {
	int res;

	if(MutexsList == null) { MutexsList = list_create(); }

	MIDC * node;
	int search_f(MIDC * data) { return( data->mutex_addr == mutex ); }
	if ( (node = list_find(MutexsList, search_f)) == null ) {
		printf("[CRITICAL_ERROR][UNREGISTERED_MUTEX]\n");
		node = malloc(sizeof(MIDC));
		node->name = "UnknownMutex";
		node->mutex_addr = mutex;
	}

	if(MUTEX_DEBUG_LEVEL >= MX_ONLY_LOCK_UNLOCK) {
		printf("[MUTEX_INFO][TRYING_TO_LOCK_MUTEX*][%d][%s][%s]\n", pthread_self(), get_thread_name(pthread_self()), node->name);
	}
	res = pthread_mutex_lock(mutex);
	if (res != 0) {
		if(MUTEX_DEBUG_LEVEL >= MX_MUTEX_ERROR) {
			printf("[MUTEX_ERROR][ERROR_AT_MUTEX_LOCK*][%d][%s][%s]\n", pthread_self(), get_thread_name(pthread_self()), node->name);
		}
	} else {
		if(MUTEX_DEBUG_LEVEL >= MX_ONLY_LOCK_UNLOCK) {
			printf("[MUTEX_INFO][MUTEX_HAS_BEEN_LOCKED][%d][%s][%s]\n", pthread_self(), get_thread_name(pthread_self()), node->name);
		}
	}
	return res;
}

int unlock_mutex(pthread_mutex_t * mutex) {
	int res;

	if(MutexsList == null) { MutexsList = list_create(); }

	MIDC * node;
	int search_f(MIDC * data) { return( data->mutex_addr == mutex ); }
	if ( (node = list_find(MutexsList, search_f)) == null ) {
		printf("[CRITICAL_ERROR][UNREGISTERED_MUTEX]\n");
		node = malloc(sizeof(MIDC));
		node->name = "UnknownMutex";
		node->mutex_addr = mutex;
	}

	if(MUTEX_DEBUG_LEVEL >= MX_ONLY_LOCK_UNLOCK) {
		printf("[MUTEX_INFO][TRYING_UNLOCK_MUTEX**][%d][%s][%s]\n", pthread_self(), get_thread_name(pthread_self()), node->name);
	}
	res = pthread_mutex_unlock(mutex);
	if (res != 0) {
		if(MUTEX_DEBUG_LEVEL >= MX_MUTEX_ERROR) {
			printf("[MUTEX_ERROR][ERROR_MUTEX_UNLOCK**][%d][%s][%s]\n", pthread_self(), get_thread_name(pthread_self()), node->name);
		}
	} else {
		if(MUTEX_DEBUG_LEVEL >= MX_ONLY_LOCK_UNLOCK) {
			printf("[MUTEX_INFO][MUTEX_BEEN_UNLOCKED**][%d][%s][%s]\n", pthread_self(), get_thread_name(pthread_self()), node->name);
		}
	}
	return res;
}



int inform_thread_id(char * name) {
	TIDC * node;

	if(ThreadsList == null) { ThreadsList = list_create(); }

	int search_f(TIDC * data) { return( data->tid == pthread_self() ); }

	if ( (node = list_find(ThreadsList, search_f)) == null ) {
		node = malloc(sizeof(TIDC));
		node->tid = pthread_self(); node->name = name;
		list_add(ThreadsList, node);
	}

	printf("[THREAD_DATA][INFORMING_THREAD_TID][%d][%s]\n", node->tid, name);
	return pthread_self();
}

char * get_thread_name(int tid) {
	TIDC * node;

	if(ThreadsList == null) { ThreadsList = list_create(); }

	int search_f(TIDC * data) { return( data->tid == tid ); }

	if ( (node = list_find(ThreadsList, search_f)) == null ) {
		return "UnknownThreadName";
	} else {
		return node->name;
	}
}



unsigned long unix_epoch() {
	return (unsigned)time(NULL);
}

char * consistency_to_char(ConsistencyTypes consistency) {
	switch(consistency) {
	case STRONG_CONSISTENCY:
		return "SC";
	case STRONG_HASH_CONSISTENCY:
		return "SHC";
	case EVENTUAL_CONSISTENCY:
		return "EC";
	}
	return "UNK";
}

ConsistencyTypes char_to_consistency(char * consistency) {
	if(strcmp(consistency, "SC")) {
		return STRONG_CONSISTENCY;
	}
	if(strcmp(consistency, "SHC")) {
		return STRONG_HASH_CONSISTENCY;
	}
	if(strcmp(consistency, "EC")) {
		return EVENTUAL_CONSISTENCY;
	}
}

//Consola
void cargar_comando(comando_t* unComando, char* linea){

	char delim[] = " ";
	int indice = 0;
	char* saveptr;

	if (linea && !linea[0]) {
	  return;
	}

	char *ptr = strtok_r(linea, delim,&saveptr);

	strcpy(unComando->comando, ptr);
	ptr = strtok_r(NULL, delim,&saveptr);

	while(ptr != NULL && indice < 5)
	{
		strcpy(unComando->parametro[indice], ptr);
		ptr = strtok_r(NULL, delim,&saveptr);
		indice++;
	}

}

void vaciar_comando(comando_t* unComando){

	*unComando->comando = '\0';
	*unComando->parametro[0] = '\0';
	*unComando->parametro[1] = '\0';
	*unComando->parametro[2] = '\0';
	*unComando->parametro[3] = '\0';
	*unComando->parametro[4] = '\0';

}

void imprimir_comando(comando_t* unComando){
	printf("unComando->comando: %s\n",unComando->comando );
	printf("unComando->parametro[0]: %s\n",unComando->parametro[0] );
	printf("unComando->parametro[1]: %s\n",unComando->parametro[1] );
	printf("unComando->parametro[2]: %s\n",unComando->parametro[2] );
	printf("unComando->parametro[3]: %s\n",unComando->parametro[3] );
	printf("unComando->parametro[4]: %s\n",unComando->parametro[4] );
}


void * crear_consola(void (*execute)(comando_t*),char* unString) {

	comando_t comando;

	char *linea;
	int quit = 0;

	printf("Bienvenido/a a la consola de %s\n",unString);
	printf("Escribi 'info' para obtener una lista de comandos\n\n");

	while(quit == 0){

		linea = readline("> ");
		if (linea && !linea[0]) {
			quit = 1;
		}else{
			add_history(linea);
			vaciar_comando(&comando);
			cargar_comando(&comando,linea);

			if((strcmp(comando.comando,"exit")==0)){
				quit = 1;
			}else{
				(*execute)(&comando);
			}
		}
		free(linea);
	}
	return EXIT_SUCCESS;
}

