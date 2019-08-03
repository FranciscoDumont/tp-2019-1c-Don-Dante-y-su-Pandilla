#include "libmain.h"



void custom_print(const char* message_template, ...) {
	fflush(stdin);fflush(stdout);

	va_list vargs;
	va_start(vargs, message_template);
	vprintf(message_template, vargs);
	va_end(vargs);

	fflush(stdin);fflush(stdout);
}

void sayhi(char * name) {
	custom_print("Hola %s", name);
}

int create_socket() {
	int fd;
	if ((fd=socket(AF_INET, SOCK_STREAM, 0)) == -1) {
		if(NETWORK_DEBUG_LEVEL >= NW_NETWORK_ERRORS) {
			custom_print("[NETWORK_ERROR][ERROR_CREATING_SOCKET]\n");
		}
		return -1;
	} else {
		int option = 1;
		setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &option, sizeof(option));
		if(NETWORK_DEBUG_LEVEL >= NW_ALL_DISPLAY) {
			custom_print("[NETWORK_INFO][SOCKET_CREATED_%d]\n", fd);
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
			custom_print("[NETWORK_ERROR][ERROR_BINDING_SOCKET_TO_%d]\n", port);
		}
	} else {
		if(NETWORK_DEBUG_LEVEL >= NW_ALL_DISPLAY) {
			custom_print("[NETWORK_INFO][SOCKET_BINDED_%d_TO_%d]\n", socket, port);
		}
	}
	return bindres;
}

int connect_socket(int socket, char * addr, int port) {
	struct hostent * he;
	struct sockaddr_in server;

	if ((he=gethostbyname(addr)) == NULL) {
		if(NETWORK_DEBUG_LEVEL >= NW_NETWORK_ERRORS) {
			custom_print("[NETWORK_ERROR][ERROR_FINDING_HOST_%s]\n", addr);
		}
		return -1;
	}
	if(NETWORK_DEBUG_LEVEL >= NW_ALL_DISPLAY) {
		custom_print("[NETWORK_INFO][CONNECTING_SOCKET_HOST_FOUND_%s]\n", addr);
	}

	server.sin_family = AF_INET;
	server.sin_port = htons(port);

	server.sin_addr = *((struct in_addr *)he->h_addr);

	bzero(&(server.sin_zero), 8);

	if (connect(socket, (struct sockaddr *)&server, sizeof(struct sockaddr)) == -1){
		if(NETWORK_DEBUG_LEVEL >= NW_NETWORK_ERRORS) {
			custom_print("[NETWORK_ERROR][ERROR_CONNECTING_TO_%s:%d]\n", addr, port);
		}
		return -1;
	}
	if(NETWORK_DEBUG_LEVEL >= NW_ALL_DISPLAY) {
		custom_print("[NETWORK_INFO][SOCKET_CONNECTED_%d_%s:%d]\n", socket, addr, port);
	}
	return 0;
}

int close_socket(int socket) {
	close(socket);
	if(NETWORK_DEBUG_LEVEL >= NW_ALL_DISPLAY) {
		custom_print("[NETWORK_INFO][SOCKET_CLOSED_%d\n]", socket);
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
			custom_print("[NETWORK_ERROR][ERROR_SENDING_HEADER_TO_%d]\n", destination);
		}
		return sent;
	} else {
		if(NETWORK_DEBUG_LEVEL >= NW_ALL_DISPLAY) {
			custom_print("[NETWORK_INFO][HEADER_SENT_TO_%d_(%d_bytes)]\n", destination, sent);
		}
	}
	header_sent = sent;
	if(data_size > 0) {
		data_sent = send(destination, data_stream, data_size, 0);
		if (sent == -1) {
			if(NETWORK_DEBUG_LEVEL >= NW_NETWORK_ERRORS) {
				custom_print("[NETWORK_ERROR][ERROR_SENDING_DATA_STREAM_TO_%d]\n", destination);
			}
			return sent;
		} else {
			if(NETWORK_DEBUG_LEVEL >= NW_ALL_DISPLAY) {
				custom_print("[NETWORK_INFO][DATA_STREAM_SENT_TO_%d_(%d_bytes)]\n", destination, data_sent);
			}
			sent += data_sent;
		}
	} else {
		if(NETWORK_DEBUG_LEVEL >= NW_ALL_DISPLAY) {
			custom_print("[NETWORK_INFO][NO_STREAM_TO_SEND]\n", destination);
		}
	}

	return sent;
}

int recieve_header(int source, MessageHeader * buffer) {
	int rec;
	rec = recv(source, buffer, sizeof(MessageHeader), 0);
	if (rec > 0) {
		if(NETWORK_DEBUG_LEVEL >= NW_ALL_DISPLAY) {
			custom_print("[NETWORK_INFO][HEADER_RECIEVED_FROM_%d_(%d_bytes)]\n", source, rec);
		}
	} else {
		if(NETWORK_DEBUG_LEVEL >= NW_NETWORK_ERRORS) {
			custom_print("[NETWORK_ERROR][ERROR_RECIEVING_HEADER_FROM_%d]\n", source);
		}
	}
	return rec;
}

int recieve_data(int source, void * buffer, int data_size) {
	int rec;
	rec = recv(source, buffer, sizeof(data_size), 0);
	if (rec > 0) {
		if(NETWORK_DEBUG_LEVEL >= NW_ALL_DISPLAY) {
			custom_print("[NETWORK_INFO][DATA_RECIEVED_FROM_%d_(%d_bytes)]\n", source, rec);
		}
	} else {
		if(NETWORK_DEBUG_LEVEL >= NW_NETWORK_ERRORS) {
			custom_print("[NETWORK_ERROR][ERROR_RECIEVING_DATA_FROM_%d]\n", source);
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
			custom_print("[NETWORK_ERROR][ERROR_LISTENING_FD_%d]\n", socket);
		}
		return -1;
	}
	if(NETWORK_DEBUG_LEVEL >= NW_ALL_DISPLAY) {
		custom_print("[NETWORK_INFO][SOCKET_NOW_LISTENING_%d]\n", socket);
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
			custom_print("[NETWORK_INFO][SOCKET_AWAITING_CONNECTION_%d]\n", socket);
		}
		activity = select(max_sd + 1, &readfds, NULL, NULL, NULL);

		if (activity < 0) {
			if(NETWORK_DEBUG_LEVEL >= NW_NETWORK_ERRORS) {
				custom_print("[NETWORK_ERROR][ERROR_SELECTING_FD_%d]\n", socket);
			}
		}

		if (FD_ISSET(socket, &readfds)) {
			if ((new_socket = accept(socket,
					(struct sockaddr *)&address, (socklen_t*)&addrlen)) < 0) {
				if(NETWORK_DEBUG_LEVEL >= NW_NETWORK_ERRORS) {
					custom_print("[NETWORK_ERROR][ERROR_ACCEPTING_SOCKET_%d_%s:%d]\n",
							new_socket, inet_ntoa(address.sin_addr), ntohs(address.sin_port));
				}
			}
				if(NETWORK_DEBUG_LEVEL >= NW_ALL_DISPLAY) {
					custom_print("[NETWORK_INFO][NEW_CONNECTION_%d_%d_%s:%d]\n", socket, new_socket,
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
						custom_print("[NETWORK_INFO][LOST_CONNECTION_%d_%d_%s:%d]\n", socket, client_socket,
								inet_ntoa(address.sin_addr), ntohs(address.sin_port));
					}
					lost_connection(client_socket, inet_ntoa(address.sin_addr) , ntohs(address.sin_port));

					close(sd);
					client_socket_array[i] = 0;
				} else {

					if(NETWORK_DEBUG_LEVEL >= NW_ALL_DISPLAY) {
						custom_print("[NETWORK_INFO][INCOMING_MESSAGE_%d_%d]\n", socket, client_socket);
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
			custom_print("[MUTEX_ERROR][ERROR_MUTEX_INIT]\n");
		}
	} else {
		if(MUTEX_DEBUG_LEVEL >= MX_ALL_DISPLAY) {
			custom_print("[MUTEX_INFO][MUTEX_INIT_OK]\n");
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
			custom_print("[MUTEX_ERROR][ERROR_MUTEX_DESTROY]\n");
		}
	} else {
		if(MUTEX_DEBUG_LEVEL >= MX_ALL_DISPLAY) {
			custom_print("[MUTEX_INFO][MUTEX_INIT_DESTROY]\n");
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
		custom_print("[CRITICAL_ERROR][UNREGISTERED_MUTEX]\n");
		node = malloc(sizeof(MIDC));
		node->name = "UnknownMutex";
		node->mutex_addr = mutex;
	}

	if(MUTEX_DEBUG_LEVEL >= MX_ONLY_LOCK_UNLOCK) {
		custom_print("[MUTEX_INFO][TRYING_TO_LOCK_MUTEX*][%d][%s][%s]\n", pthread_self(), get_thread_name(pthread_self()), node->name);
	}
	res = pthread_mutex_lock(mutex);
	if (res != 0) {
		if(MUTEX_DEBUG_LEVEL >= MX_MUTEX_ERROR) {
			custom_print("[MUTEX_ERROR][ERROR_AT_MUTEX_LOCK*][%d][%s][%s]\n", pthread_self(), get_thread_name(pthread_self()), node->name);
		}
	} else {
		if(MUTEX_DEBUG_LEVEL >= MX_ONLY_LOCK_UNLOCK) {
			custom_print("[MUTEX_INFO][MUTEX_HAS_BEEN_LOCKED][%d][%s][%s]\n", pthread_self(), get_thread_name(pthread_self()), node->name);
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
		custom_print("[CRITICAL_ERROR][UNREGISTERED_MUTEX]\n");
		node = malloc(sizeof(MIDC));
		node->name = "UnknownMutex";
		node->mutex_addr = mutex;
	}

	if(MUTEX_DEBUG_LEVEL >= MX_ONLY_LOCK_UNLOCK) {
		custom_print("[MUTEX_INFO][TRYING_UNLOCK_MUTEX**][%d][%s][%s]\n", pthread_self(), get_thread_name(pthread_self()), node->name);
	}
	res = pthread_mutex_unlock(mutex);
	if (res != 0) {
		if(MUTEX_DEBUG_LEVEL >= MX_MUTEX_ERROR) {
			custom_print("[MUTEX_ERROR][ERROR_MUTEX_UNLOCK**][%d][%s][%s]\n", pthread_self(), get_thread_name(pthread_self()), node->name);
		}
	} else {
		if(MUTEX_DEBUG_LEVEL >= MX_ONLY_LOCK_UNLOCK) {
			custom_print("[MUTEX_INFO][MUTEX_BEEN_UNLOCKED**][%d][%s][%s]\n", pthread_self(), get_thread_name(pthread_self()), node->name);
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

	custom_print("[THREAD_DATA][INFORMING_THREAD_TID][%d][%s]\n", node->tid, name);
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

	struct timeval tv;
	time_t seconds;
	gettimeofday(&tv, NULL);
	unsigned long long miliseconds_since_epoch = (unsigned long long)((unsigned long long)(tv.tv_sec) * (unsigned long long)1000 + (unsigned long long)(tv.tv_usec) / (unsigned long long)1000);

	seconds = time(NULL);
	int days_since_epoch = seconds/(60*60*24);
	unsigned long long midnight_miliseconds_since_epoch = (unsigned long long)days_since_epoch*(24*60*60*1000);

	unsigned long miliseconds_since_midnight = (unsigned long)(miliseconds_since_epoch - midnight_miliseconds_since_epoch);
	return miliseconds_since_midnight;
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
	if(strcasecmp(consistency, "SC") == 0) {
		return STRONG_CONSISTENCY;
	}
	if(strcasecmp(consistency, "SHC") == 0) {
		return STRONG_HASH_CONSISTENCY;
	}
	if(strcasecmp(consistency, "EC") == 0) {
		return EVENTUAL_CONSISTENCY;
	}
	return C_UNKNOWN;
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
	custom_print("unComando->comando: %s\n",unComando->comando );
	custom_print("unComando->parametro[0]: %s\n",unComando->parametro[0] );
	custom_print("unComando->parametro[1]: %s\n",unComando->parametro[1] );
	custom_print("unComando->parametro[2]: %s\n",unComando->parametro[2] );
	custom_print("unComando->parametro[3]: %s\n",unComando->parametro[3] );
	custom_print("unComando->parametro[4]: %s\n",unComando->parametro[4] );
}


void * crear_consola(void (*execute)(comando_t*),char* unString) {

	comando_t comando;

	char *linea;
	int quit = 0;

	custom_print("Bienvenido/a a la consola de %s\n",unString);
	custom_print("Escribi 'info' para obtener una lista de comandos\n\n");

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

void create_lql(LQLScript * script, char * filepath) {
	custom_print("CREATING LQL\n\n");
	script->file = fopen(filepath, "r");
	script->line = NULL;
	script->len = 0;
	script->quantum_counter = 0;
}

_Bool string_is_number(char * str) {
	int a;
	for(a=0 ; a<strlen(str) ; a++) {
		if(!isdigit(str[a])) {
			return false;
		}
	}
	return true;
}

Instruction * parse_lql_line(LQLScript * script) {
	Instruction * i = null; int a, t;
	char * auxLine = null;

	if ((script->read = getline(&script->line, &script->len, script->file)) != -1) {

		char * line = script->line;

		if(line == NULL || string_equals_ignore_case(line, "")){
			return null;
		}

		t = strlen(line);
		auxLine = malloc((t + 1) * sizeof(char));
		for(a=0 ; a<t ; a++) {
			auxLine[a] = line[a];
		}
		auxLine[a] = '\0';

		string_trim(&auxLine);

		a=0;
		char ** split = malloc(512);

		char * content = auxLine;
		char * buffer  = malloc(sizeof(char) * strlen(content));
		buffer[0] = '\0';

		char * token;
		for (token = strtok_r(content, " ", &buffer) ;
				token != NULL ;
				token = strtok_r(buffer, " ", &buffer)) {
			split[a++] = token;
		}

		i = malloc(sizeof(Instruction));
		i->i_type = UNKNOWN_OP_TYPE;

		if(strcmp(split[0], "SELECT") == 0) {
			i->i_type = SELECT;
			i->table_name = split[1];
			i->key = atoi(split[2]);

			//custom_print("SEL %s %d\n", i->table_name, i->key);
		}else if(strcmp(split[0], "INSERT") == 0) {
			i->i_type = INSERT;
			i->table_name = split[1];

			if(string_is_number(split[2])) {
				i->key = atoi(split[2]);
			} else {
				i->i_type = UNKNOWN_OP_TYPE;
			}

			i->value = split[3];
			i->value = i->value+1;
			i->value[strlen(i->value)-1] = '\0';

			i->timestamp = unix_epoch();

			//custom_print("INS %s %d %s\n", i->table_name, i->key, i->value);
		}else if(strcmp(split[0], "CREATE") == 0) {
			i->i_type = CREATE;
			i->table_name = split[1];

			i->c_type = char_to_consistency(split[2]);
			i->partitions = atoi(split[3]);
			i->compaction_time = strtoul(split[4], NULL, 0);

			//custom_print("CRE %s %d %d %ul\n", i->table_name, i->c_type, i->partitions, i->compaction_time);
		}else if(strcmp(split[0], "DESCRIBE") == 0) {
			i->i_type = DESCRIBE;
			i->table_name = split[1];

			if(i->table_name == null) {
				i->table_name = strdup("");
			}

			//custom_print("DES %s\n", i->table_name);
		}else if(strcmp(split[0], "DROP") == 0) {
			i->i_type = DROP;
			i->table_name = split[1];

			//custom_print("DRO %s\n", i->table_name);
		} else {
			custom_print("UNSOPORTED %s", auxLine);
		}

		i->knl_line = auxLine;
	} else {
		custom_print("ERROR");
		return null;
	}

	if(i->i_type == UNKNOWN_OP_TYPE) {
		custom_print("Error al Parsear %s", auxLine);
	}

	return i;
}

void close_lql(LQLScript * script) {
	fclose(script->file);
}

