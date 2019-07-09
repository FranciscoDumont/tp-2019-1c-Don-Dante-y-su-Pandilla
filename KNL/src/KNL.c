#include <dalibrary/libmain.h>

t_log * logger;
t_config * config_file = null;
KNLConfig config;
int mysocket; //TODO: esto deberia estar en el config de kernel? 🤔
int port = 6969; //TODO: esto deberia estar en el config de kernel? 🤔

t_list * gossiping_list;

void gossiping_start(pthread_t * thread);
void server_start(pthread_t * thread);

//TODO: Implementar luego
int insert_knl(char * table_name, int key, char * value, unsigned long timestamp);
int create_knl(char * table_name, ConsistencyTypes consistency, int partitions, int compaction_time);
int select_knl(char * table_name, int key);
int describe_knl(char * table_name);
int drop_knl(char * table_name);
void execute_knl(comando_t* unComando);
void info();
void consola_knl();

int main(int argc, char **argv) {
	if (argc != 2) {
		config_file = config_create("knl01.cfg");
	} else {
		config_file = config_create(argv[1]);
	}

	gossiping_list = list_create();

	config.a_memory_ip = config_get_string_value(config_file, "IP_MEMORIA");
	config.a_memory_port = config_get_int_value(config_file, "PUERTO_MEMORIA");
	config.quantum = config_get_int_value(config_file, "QUANTUM");
	config.multiprocessing_grade = config_get_int_value(config_file, "MULTIPROCESAMIENTO");
	config.metadata_refresh = config_get_int_value(config_file, "METADATA_REFRESH");
	config.exec_delay = config_get_int_value(config_file, "SLEEP_EJECUCION");

	logger = log_create("kernel_logger.log", "KNL", true, LOG_LEVEL_TRACE);

	pthread_t thread_g;
	gossiping_start(&thread_g);

	pthread_t thread_server;
	server_start(&thread_server);

	pthread_t knl_console_id;
	pthread_create(&knl_console_id, NULL, consola_knl, NULL);

	pthread_detach(thread_g);
	pthread_detach(thread_server);
	pthread_join(knl_console_id, NULL);

	return EXIT_SUCCESS;
}

//API
int insert_knl(char * table_name, int key, char * value, unsigned long timestamp){
	log_info(logger, "INICIA INSERT: insert_knl(%s, %d, %s, %lu)", table_name, key, value, timestamp);
	int exit_value;
	send_data(config.a_memory_ip, KNL_MEM_INSERT, 0, null);

	int table_name_len = strlen(table_name)+1;
	send(config.a_memory_ip, &table_name_len,  sizeof(int), 0);
	send(config.a_memory_ip, table_name,       table_name_len,0);

	send(config.a_memory_ip, &key, sizeof(int), 0);

	int value_len = strlen(value)+1;
	send(config.a_memory_ip, &value_len,  sizeof(int), 0);
	send(config.a_memory_ip, value,       value_len,0);

	send(config.a_memory_ip, &timestamp, sizeof(long), 0);


	MessageHeader * header = malloc(sizeof(MessageHeader));
	recieve_header(config.a_memory_ip, header);
	if(header->type == OPERATION_SUCCESS) {
		log_info(logger, "MEM ANSWERED SUCCESFULLY");
		exit_value = EXIT_SUCCESS;
	} else {
		log_info(logger, "INSERT ERROR");
		exit_value = EXIT_FAILURE;
	}
	return exit_value;
}


int create_knl(char * table_name, ConsistencyTypes consistency, int partitions, int compaction_time){
	log_info(logger, "INICIA CREATE: create_knl(%s, %d, %d, %d)", table_name, consistency, partitions, compaction_time);
	int exit_value;
	send_data(config.a_memory_ip, KNL_MEM_CREATE, 0, null);

	int table_name_len = strlen(table_name)+1;
	send(config.a_memory_ip, &table_name_len,  sizeof(int), 0);
	send(config.a_memory_ip, table_name,       table_name_len,0);

	send(config.a_memory_ip, &consistency, sizeof(int), 0);
	send(config.a_memory_ip, &partitions, sizeof(int), 0);
	send(config.a_memory_ip, &compaction_time, sizeof(int), 0);

	MessageHeader * header = malloc(sizeof(MessageHeader));
	recieve_header(config.a_memory_ip, header);
	if(header->type == OPERATION_SUCCESS) {
		log_info(logger, "MEM ANSWERED SUCCESFULLY");
		exit_value = EXIT_SUCCESS;
	} else {
		log_info(logger, "CREATE ERROR");
		exit_value = EXIT_FAILURE;
	}
	return exit_value;
}


int select_knl(char * table_name, int key){
	log_info(logger, "INICIA SELECT: select_knl(%s, %d)", table_name, key);
	int exit_value;
	send_data(config.a_memory_ip, KNL_MEM_DESCRIBE, 0, null);

	int table_name_len = strlen(table_name)+1;
	send(config.a_memory_ip, &table_name_len,  sizeof(int), 0);
	send(config.a_memory_ip, table_name,       table_name_len,0);

	send(config.a_memory_ip, &key, sizeof(int), 0);

	MessageHeader * header = malloc(sizeof(MessageHeader));
	recieve_header(config.a_memory_ip, header);
	if(header->type == OPERATION_SUCCESS) {
		log_info(logger, "MEM ANSWERED SUCCESFULLY");
		log_info(logger, "MEM ENVÍA RESULTADO DE SELECT SOLICITADO");
		int result_len;
		recv(config.a_memory_ip, &result_len, sizeof(int), 0);
		char* value = malloc(sizeof(char) * result_len);
		recv(config.a_memory_ip, value, result_len, 0);
		log_info(logger, "  EL VALOR RECIBIDO ES %s", value);
		exit_value = EXIT_SUCCESS;
	} else {
		log_info(logger, "SELECT ERROR");
		exit_value = EXIT_FAILURE;
	}
	return exit_value;
}


int describe_knl(char * table_name){
	log_info(logger, "INICIA DESCRIBE: describe_mem(%s)", table_name);
	int exit_value;
	send_data(config.a_memory_ip, KNL_MEM_DESCRIBE, 0, null);

	if(table_name == null){
		table_name = strdup("");
	}

	int table_name_len = strlen(table_name)+1;

	send(config.a_memory_ip, &table_name_len,  sizeof(int), 0);
	send(config.a_memory_ip, table_name,       table_name_len,0);


	MessageHeader * header = malloc(sizeof(MessageHeader));
	recieve_header(config.a_memory_ip, header);
	if(header->type == OPERATION_SUCCESS) {
		log_info(logger, "LFS ANSWERED SUCCESFULLY");
		log_info(logger, "DESCRIBE EN EL FILESYSTEM");
		exit_value = EXIT_SUCCESS;
	} else {
		log_info(logger, "DESCRIBE ERROR");
		exit_value = EXIT_FAILURE;
	}
	return exit_value;
}


int drop_knl(char * table_name){
	log_info(logger, "INICIA DROP: drop_knl(%s)", table_name);

	int exit_value;
	send_data(config.a_memory_ip, KNL_MEM_DROP, 0, null);

	int table_name_len = strlen(table_name)+1;

	send(config.a_memory_ip, &table_name_len,  sizeof(int), 0);
	send(config.a_memory_ip, table_name,       table_name_len,0);

	MessageHeader * header = malloc(sizeof(MessageHeader));
	recieve_header(config.a_memory_ip, header);
	if(header->type == OPERATION_SUCCESS) {
		log_info(logger, "MEM ANSWERED SUCCESFULLY");
		log_info(logger, "DROP EN EL FILESYSTEM");
		exit_value = EXIT_SUCCESS;
	} else {
		log_info(logger, "DROP ERROR");
		exit_value = EXIT_FAILURE;
	}

	return exit_value;
}


//Gossiping
void inform_gossiping_pool() {
	log_info(logger, "GOSSIPING LIST START");

	int a;
	for(a = 0 ; a < gossiping_list->elements_count ; a++) {
		MemPoolData * this_mem = list_get(gossiping_list, a);
		log_info(logger, "      %d %s:%d", this_mem->memory_id, this_mem->ip, this_mem->port);
	}

	log_info(logger, "GOSSIPING LIST END");
}
void add_to_pool(MemPoolData * mem) {
	int a;
	for(a = 0 ; a < gossiping_list->elements_count ; a++) {
		MemPoolData * this_mem = list_get(gossiping_list, a);
		if(this_mem->memory_id == mem->memory_id) {
			return;
		}
	}
	list_add(gossiping_list, mem);
}
void gossiping_thread() {
	while(1) {
		list_clean(gossiping_list);

		//Contact known seed
		int memsocket;
		MemPoolData * this_seed = malloc(sizeof(MemPoolData));
		this_seed->ip = malloc(sizeof(char) * IP_LENGTH);
		this_seed->port = config.a_memory_port;
		strcpy(this_seed->ip, config.a_memory_ip);

		if ((memsocket = create_socket()) == -1) {
			memsocket = -1;
		}
		if (memsocket != -1 && (connect_socket(memsocket, this_seed->ip, this_seed->port)) == -1) {
			memsocket = -1;
		}

		log_info(logger, "requesting to %s %d", this_seed->ip, this_seed->port);
		if(memsocket != -1) {
			send_data(memsocket, GOSSIPING_REQUEST, 0, NULL);
			recv(memsocket, &(this_seed->memory_id), sizeof(int), 0);
			log_info(logger, "   ITS %d", this_seed->memory_id);

			add_to_pool(this_seed);

			int torecieve, a;
			recv(memsocket, &torecieve, sizeof(int), 0);
			for(a = 0 ; a < torecieve ; a++) {
				MemPoolData * this_mem = malloc(sizeof(MemPoolData));
				this_mem->ip = malloc(sizeof(char) * IP_LENGTH);

				recv(memsocket, &this_mem->port, sizeof(int), 0);
				recv(memsocket, &this_mem->memory_id, sizeof(int), 0);
				recv(memsocket, this_mem->ip, sizeof(char) * IP_LENGTH, 0);

				log_info(logger, "   gave me %d in %s %d", this_mem->memory_id, this_mem->ip, this_mem->port);

				add_to_pool(this_mem);
			}
		}

		close(memsocket);

		inform_gossiping_pool();

		sleep(config.metadata_refresh / 1000);
	}
}
void gossiping_start(pthread_t * thread) {
	pthread_create(thread, NULL, gossiping_thread, NULL);
}

//Server
int server_function() {
	if((mysocket = create_socket()) == -1) {
		return EXIT_FAILURE;
	}
	if((bind_socket(mysocket, port)) == -1) {
		return EXIT_FAILURE;
	}

	void new(int fd, char * ip, int port) {

	}
	void lost(int fd, char * ip, int port) {

	}
	void incoming(int fd, char * ip, int port, MessageHeader * header) {
		switch(header->type) {
			/*
			case GOSSIPING_REQUEST:
				;
				log_info(logger, "gossiping request");
				send(fd, &config.memory_id, sizeof(int), 0);

				int gossiping_count = gossiping_list->elements_count;
				int a;
				send(fd, &gossiping_count, sizeof(int), 0);

				log_error(logger, "GONNA PASS %d", gossiping_count);
				for(a = 0 ; a < gossiping_count ; a++) {
					MemPoolData * this_mem = list_get(gossiping_list, a);

					log_info(logger, "   %d %s %d", this_mem->memory_id, this_mem->ip, this_mem->port);

					send(fd, &this_mem->port, sizeof(int), 0);
					send(fd, &this_mem->memory_id, sizeof(int), 0);
					send(fd, this_mem->ip, sizeof(char) * IP_LENGTH, 0);
				}

				break;
				*/
		}
	}
	start_server(mysocket, &new, &lost, &incoming);
}
void server_start(pthread_t * thread) {
	pthread_create(thread, NULL, server_function, NULL);
}

void consola_knl(){
	crear_consola(execute_knl, "Kernel");
}

//TODO: Completar cuando se tenga la implementacion de las funciones
void execute_knl(comando_t* unComando){
	char comandoPrincipal[20];
	char parametro1[20];
	char parametro2[20];
	char parametro3[20];
	char parametro4[20];
	char parametro5[20];

	imprimir_comando(unComando);

	strcpy(comandoPrincipal,unComando->comando);
	strcpy(parametro1,unComando->parametro[0]);
	strcpy(parametro2,unComando->parametro[1]);
	strcpy(parametro3,unComando->parametro[2]);
	strcpy(parametro4,unComando->parametro[3]);
	strcpy(parametro5,unComando->parametro[4]);

	//SELECT
	if(strcmp(comandoPrincipal,"select")==0){
		if(parametro1[0] == '\0'){
			printf("select no recibio el nombre de la tabla\n");
			return;
		}else if (parametro2[0] == '\0'){
			printf("select no recibio la key\n");
			return;
		}//else select_knl(parametro1,atoi(parametro2));

	//INSERT
	}else if (strcmp(comandoPrincipal,"insert")==0){
		if(parametro1[0] == '\0'){
			printf("insert no recibio el nombre de la tabla\n");
			return;
		}else if (parametro2[0] == '\0'){
			printf("insert no recibio la key\n");
			return;
		}else if (parametro3[0] == '\0'){
			printf("insert no recibio el valor\n");
			return;
		}else if (parametro4[0] == '\0'){
//			insert_knl(parametro1,atoi(parametro2),parametro3,unix_epoch());
		}//else insert_knl(parametro1,atoi(parametro2),parametro3,strtoul(parametro4,NULL,10));

	//CREATE
	}else if (strcmp(comandoPrincipal,"create")==0){
		if(parametro1[0] == '\0'){
			printf("create no recibio el nombre de la tabla\n");
			return;
		}else if (parametro2[0] == '\0'){
			printf("create no recibio el tipo de consistencia\n");
			return;
		}else if (parametro3[0] == '\0'){
			printf("create no recibio la particion\n");
			return;
		}else if (parametro4[0] == '\0'){
			printf("create no recibio el tiempo de compactacion\n");
			return;
		}//else create_knl(parametro1,char_to_consistency(parametro2),atoi(parametro3),atoi(parametro4));
	
	//DESCRIBE
	}else if (strcmp(comandoPrincipal,"describe")==0){
		//chekea si parametro es nulo adentro de describe_knl
//		describe_knl(parametro1);

	//DROP
	}else if (strcmp(comandoPrincipal,"drop")==0){
		if(parametro1[0] == '\0'){
			printf("drop no recibio el nombre de la tabla\n");
		}//else drop_knl(parametro1);

	//INFO
	}else if (strcmp(comandoPrincipal,"info")==0){
//		info();
	}

}
