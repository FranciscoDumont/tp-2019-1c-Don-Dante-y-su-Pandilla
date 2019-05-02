#include <dalibrary/libmain.h>

t_log * logger;
t_config * config_file = null;
MEMConfig config;

t_list * gossiping_list;

void* memoriaPrincipal;

t_list tabla_segmentos;

typedef struct {
	char* nombre;
	char* path;
	t_list paginas;
} segmento_t;

typedef struct {
	int nro_pagina;
	void* puntero_pagina;
	int flag_modificado;
} pagina_t;



void gossiping_start(pthread_t * thread);
void server_start(pthread_t * thread);

//TODO: Implementar luego
int insert_mem(char * table_name, int key, char * value, unsigned long timestamp);
int create_mem(char * table_name, ConsistencyTypes consistency, int partitions, int compaction_time);
char * select_mem(char * table_name, int key);
void describe_mem(char * table_name);
void drop_mem(char * table_name);
void execute_mem(comando_t* unComando);
void info();

int main(int argc, char **argv) {

	if (argc != 2) {
		config_file = config_create("mem01.cfg");
	} else {
		config_file = config_create(argv[1]);
	}

	gossiping_list = list_create();

	config.port = config_get_int_value(config_file, "PUERTO");
	config.lfs_ip = config_get_string_value(config_file, "IP_FS");
	config.lfs_port = config_get_int_value(config_file, "PUERTO_FS");
	config.seeds_ips = config_get_array_value(config_file, "IP_SEEDS");
	config.seeds_ports = config_get_array_value(config_file, "PUERTO_SEEDS");
	config.access_delay = config_get_int_value(config_file, "RETARDO_MEM");
	config.lfs_delay = config_get_int_value(config_file, "RETARDO_FS");
	config.memsize = config_get_int_value(config_file, "TAM_MEM");
	config.journal_time = config_get_int_value(config_file, "RETARDO_JOURNAL");
	config.gossiping_time = config_get_int_value(config_file, "RETARDO_GOSSIPING");
	config.memory_id = config_get_int_value(config_file, "MEMORY_NUMBER");
	config.seeds_q = 0;

	char * temp_seeds_q = config_get_string_value(config_file, "IP_SEEDS");
	if(strcmp("[]", temp_seeds_q) != 0) {
		int a;
		config.seeds_q++;
		for(a = 0 ; a < strlen(temp_seeds_q) ; a++) {
			if(temp_seeds_q[a] == ',') config.seeds_q++;
		}
	}
	free(temp_seeds_q);

	char * memory_logger_path = malloc(sizeof(char) * (25));
	strcpy(memory_logger_path, "memory_logger_");
	strcat(memory_logger_path, config_get_string_value(config_file, "MEMORY_NUMBER"));
	strcat(memory_logger_path, ".log");
	logger = log_create(memory_logger_path, "MEM", true, LOG_LEVEL_TRACE);

	pthread_t thread_g;
	gossiping_start(&thread_g);

	pthread_t thread_server;
	server_start(&thread_server);

	pthread_t mem_console_id;
	pthread_create(&mem_console_id, NULL, crear_consola(execute_mem,"Memoria"), NULL);
	
	pthread_t administrar_memoria_id;
	pthread_create(&administrar_memoria_id, NULL, administrar_memoria(), NULL);

	pthread_join(thread_g, NULL);
	pthread_join(thread_server, NULL);
	pthread_join(mem_console_id, NULL);


	return EXIT_SUCCESS;
}

void administrar_memoria(){

	int limite_paginas = config.memsize/sizeof(MemtableKeyReg);
	void* memoria_principal = malloc(config.memsize);
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
	log_info(logger, "      adding %d %s %d", mem->memory_id, mem->ip, mem->port);
	list_add(gossiping_list, mem);
}
void gossiping_thread() {
	while(1) {
		int a;
		list_clean(gossiping_list);

		//First, contact seeds
		for(a = 0 ; a < config.seeds_q ; a++) {
			char * ip = config.seeds_ips[a];
			int  port = atoi(config.seeds_ports[a]);
			int memsocket;

			MemPoolData * this_seed = malloc(sizeof(MemPoolData));
			this_seed->ip = malloc(sizeof(char) * IP_LENGTH);
			this_seed->port = port;
			strcpy(this_seed->ip, ip);

			if ((memsocket = create_socket()) == -1) {
				memsocket = -1;
			}
			if (memsocket != -1 && (connect_socket(memsocket, ip, port)) == -1) {
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

		}
		inform_gossiping_pool();

		sleep(config.gossiping_time / 1000);
	}
}
void gossiping_start(pthread_t * thread) {
	pthread_create(thread, NULL, gossiping_thread, NULL);
}

//Server
int server_function() {
	if((config.mysocket = create_socket()) == -1) {
		return EXIT_FAILURE;
	}
	if((bind_socket(config.mysocket, config.port)) == -1) {
		return EXIT_FAILURE;
	}

	void new(int fd, char * ip, int port) {

	}
	void lost(int fd, char * ip, int port) {

	}
	void incoming(int fd, char * ip, int port, MessageHeader * header) {
		switch(header->type) {
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
		}
	}
	start_server(config.mysocket, &new, &lost, &incoming);
}
void server_start(pthread_t * thread) {
	pthread_create(thread, NULL, server_function, NULL);
}


//TODO: Completar cuando se tenga la implementacion de las funciones
void execute_mem(comando_t* unComando){
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
		}//else select_mem(parametro1,atoi(parametro2));

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
//			insert_mem(parametro1,atoi(parametro2),parametro3,unix_epoch());
		}//else insert_mem(parametro1,atoi(parametro2),parametro3,strtoul(parametro4,NULL,10));

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
		}//else create_mem(parametro1,char_to_consistency(parametro2),atoi(parametro3),atoi(parametro4));
	
	//DESCRIBE
	}else if (strcmp(comandoPrincipal,"describe")==0){
		//chekea si parametro es nulo adentro de describe_mem
//		describe_mem(parametro1);

	//DROP
	}else if (strcmp(comandoPrincipal,"drop")==0){
		if(parametro1[0] == '\0'){
			printf("drop no recibio el nombre de la tabla\n");
		}//else drop_mem(parametro1);

	//INFO
	}else if (strcmp(comandoPrincipal,"info")==0){
//		info();
	}

}
