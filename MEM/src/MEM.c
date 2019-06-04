#include <dalibrary/libmain.h>

t_log * logger;
t_config * config_file = null;
MEMConfig config;

t_list * gossiping_list;

void* memoria_principal;
int* mapa_memoria;
int cantidad_paginas_actuales;
int limite_paginas;

t_list* tabla_segmentos;

typedef struct {
	char* nombre;
	char* path;
	t_list* paginas;
} segmento_t;

typedef struct {
	int nro_pagina;
	void* puntero_memoria;
	int flag_modificado;
} pagina_t;



void gossiping_start(pthread_t * thread);
void server_start(pthread_t * thread);

void tests_memoria();
void crear_pagina(char* nombre_tabla,int key,char* valor,unsigned long timestamp,int un_flag_modificado);
void crear_segmento(char* nombre_tabla);
segmento_t* find_segmento(char* segmento_buscado);
pagina_t* find_pagina_en_segmento(int key_buscado,segmento_t* segmento_buscado);
void set_pagina_timestamp(pagina_t* una_pagina,unsigned long un_timestamp);
void set_pagina_key(pagina_t* una_pagina,int un_key);
void set_pagina_value(pagina_t* una_pagina,char* un_value);
int obtener_tamanio_pagina();
void actualizar_pagina(char* nombre_tabla,int key,char* valor,unsigned long timestamp);
void sacar_lru();
int hay_paginas_disponibles();
int memoria_esta_full();
unsigned long get_pagina_timestamp(pagina_t* una_pagina);
int get_pagina_key(pagina_t* una_pagina);
char* get_pagina_value(pagina_t* una_pagina);
int insert_mem(char * nombre_tabla, int key, char * valor, unsigned long timestamp);
int existe_segmento(char* );
int existe_pagina_en_segmento(int pagina_buscada,segmento_t* );


//TODO: Implementar luego
int insert_mem(char * table_name, int key, char * value, unsigned long timestamp);
int create_mem(char * table_name, ConsistencyTypes consistency, int partitions, int compaction_time);
char * select_mem(char * table_name, int key);
void describe_mem(char * table_name);
void drop_mem(char * table_name);
void execute_mem(comando_t* unComando);
void info();

void add_instruction(Instruction* i);
void journal(InstructionList list);
void delete_instructions(char * table_name);
int is_drop(Instruction* i);
int empty_list(InstructionList* i);

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

	config.lfs_socket = create_socket();
	if (config.lfs_socket != -1 && (connect_socket(config.lfs_socket, config.lfs_ip, config.lfs_port)) == -1) {
		log_info(logger, "No hay LFS");
		return EXIT_FAILURE;
	}

	send_data(config.lfs_socket, HANDSHAKE_MEM_LFS, 0, null);
	MessageHeader * header = malloc(sizeof(MessageHeader));
	recieve_header(config.lfs_socket, header);
	if(header->type == HANDSHAKE_MEM_LFS_OK) {
		log_info(logger, "LFS ANSWERED SUCCESFULLY");
		recv(config.lfs_socket, &config.value_size, sizeof(int), 0);

		log_info(logger, "VALUE SIZE RECIEVED %d", config.value_size);
	} else {
		log_info(logger, "UNEXPECTED HANDSHAKE RESULT");
		return EXIT_FAILURE;
	}

	pthread_t thread_g;
	gossiping_start(&thread_g);

	pthread_t thread_server;
	server_start(&thread_server);

	//Inicializo las variables globales
	tabla_segmentos = list_create();
	cantidad_paginas_actuales = 0;
	limite_paginas = config.memsize / obtener_tamanio_pagina();
	mapa_memoria = calloc(limite_paginas,sizeof *mapa_memoria);
	memoria_principal = malloc(config.memsize);
	log_info(logger, "Se pueden almacenar %d páginas", limite_paginas);

	//cuidado aca ndeaaa skereeeeeeee
	pthread_t tests_memoria_id;
	pthread_create(&tests_memoria_id, NULL,&tests_memoria, NULL);

	//pthread_t mem_console_id;
	//pthread_create(&mem_console_id, NULL, crear_consola(execute_mem,"Memoria"), NULL);
	
	pthread_join(thread_g, NULL);
	pthread_join(thread_server, NULL);
	//pthread_join(mem_console_id, NULL);
	pthread_join(tests_memoria_id, NULL);

	free(mapa_memoria);
	return EXIT_SUCCESS;
}

int obtener_tamanio_pagina() {
	return sizeof(unsigned long) +
			sizeof(int) +
			config.value_size;
}



//TODO: Manejar el tema de timestamp desde la funcion que llama a esta
int insert_mem(char * nombre_tabla, int key, char * valor, unsigned long timestamp) {
	log_info(logger, "Inicio insert(%s,%d,%s,%lu)",nombre_tabla,key,valor,timestamp);
	//Verifica si existe el segmento de la tabla en la memoria principal
	if(existe_segmento(nombre_tabla)){
		log_info(logger, "Ya existe el segmento %s",nombre_tabla);
	//De existir, busca en sus páginas si contiene la key solicitada y de contenerla actualiza 
	//el valor insertando el Timestamp actual.
		segmento_t* segmento_por_insertar = find_segmento(nombre_tabla);
		if (existe_pagina_en_segmento(key,segmento_por_insertar)){
			log_info(logger, "La pagina ya existia, se va a actualizar");
			actualizar_pagina(nombre_tabla,key,valor,timestamp);
		} else{
			log_info(logger, "La pagina no existia");
		//En caso que no contenga la Key, se solicita una nueva página para almacenar la misma.
		//Se deberá tener en cuenta que si no se disponen de páginas libres aplicar el algoritmo de reemplazo 
		//y en caso de que la memoria se encuentre full iniciar el proceso Journal.
			if (hay_paginas_disponibles()){
				log_info(logger, "Hay paginas disponibles, se crea la pagina");
				crear_pagina(nombre_tabla,key,valor,timestamp,1);
			}else{
				log_info(logger, "No hay paginas disponibles:");
				//todas las paginas estan llenas con o sin modificar
				if (memoria_esta_full()){
					log_info(logger, "\tTodas las paginas estan con flag en 1");
					//hacer el journaling
					//hacer journal e insertar o solo hacer journal ?? 
				}else {
					//algoritmo de reemplazo
					log_info(logger, "\tHay paginas con el flag en 0, se hace lru");
					sacar_lru();
					crear_pagina(nombre_tabla,key,valor,timestamp,1);
				}
			}

		}
	}else {
		//si no existe el segmento crear el segmento y llamar recursivamente a insert_mem
		//deberia verificar con el fileSystem para ver si se puede agregar sin hacer un create antes
		log_info(logger, "No existia el segmento %s",nombre_tabla);
		crear_segmento(nombre_tabla);
		insert_mem(nombre_tabla,key,valor,timestamp);
	}
	//add_instruction(instruction);
}

int create_mem(char * table_name, ConsistencyTypes consistency, int partitions, int compaction_time){
	int exit_value;
	send_data(config.lfs_socket, MEM_LFS_CREATE, 0, null);

	send_data(config.lfs_socket, MEM_LFS_CREATE, sizeof(table_name), table_name);
	send_data(config.lfs_socket, MEM_LFS_CREATE, sizeof(int), &consistency);
	send_data(config.lfs_socket, MEM_LFS_CREATE, sizeof(int), &partitions);
	send_data(config.lfs_socket, MEM_LFS_CREATE, sizeof(int), &compaction_time);

	MessageHeader * header = malloc(sizeof(MessageHeader));
	recieve_header(config.lfs_socket, header);
	if(header->type == OPERATION_SUCCESS) {
		log_info(logger, "LFS ANSWERED SUCCESFULLY");
		log_info(logger, "TABLA CREADA EN EL FILESYSTEM");
		exit_value = EXIT_SUCCESS;
	} else {
		log_info(logger, "UNEXPECTED HANDSHAKE RESULT");
		exit_value = EXIT_FAILURE;
	}

	return exit_value;
}

char * select_mem(char * table_name, int key){

	return "ok";
}

void describe_mem(char * table_name){

}

void drop_mem(char * table_name){

}

void journal(InstructionList list){

}
/*
void add_instruction(Instruction* i){


	/\*
	if(memoriallena){
		journal();
		add_instrucion(i);
	}else{
		seguirdelargo
	}
	*\/

	if(is_drop(i) && first != NULL){
		delete_instructions(i -> table_name);
	}

	InstructionList *new;
	new = malloc(sizeof(Instruction));

	new -> i -> i_type = i -> i_type;
	new -> i -> table_name = i -> table_name;
	new -> i-> key = i -> key;
	new -> i-> value = i -> value;
	new -> i-> c_type = i -> c_type;
	new -> i-> partitions = i -> partitions;
	new -> i-> compaction_time = i -> compaction_time;

	new -> next = NULL;

	if(empty_list(first)){
		first = new;
		last = new;
	}else{
		last -> next = new;
		last = new;
	}
	return;

}

void delete_instructions(char * target_table){
	InstructionList run = first;

	while(run != NULL){
		if(run -> i -> table_name == target_table){
			//unir al anterior con el siguiente
			run = run -> next;
		}else{
			run = run -> next;
		}
	}
	free(run);

}
*/
int is_drop(Instruction* i){
	return (i -> i_type == DROP);
}

int empty_list(InstructionList* i){
	//return (i==NULL);
	return 1;
}

/*

void show_list(InstructionList list){
	list * run = first;
	printf("elements: \n");
	while(run != NULL){
		printf(%i - ", run -> i -> i_type);
		printf(%i - ", run -> i -> table_name);
		printf(%i - ", run -> i -> key);
		printf(%i - ", run -> i -> value);
		printf(%i - ", run -> i -> c_type);
		printf(%i - ", run -> i -> partitions);
		printf(%i - ", run -> i -> compaction_time);

		run = run -> next;
		free(run);
	}

}

*/

//busca la pagina con el nomre y key y actualiza el valor
void actualizar_pagina(char* nombre_tabla,int key,char* valor,unsigned long timestamp){
	segmento_t* segmento_encontrado = find_segmento(nombre_tabla);
	pagina_t* pagina_encontrada = find_pagina_en_segmento(key,segmento_encontrado);

	if(pagina_encontrada){
		set_pagina_timestamp(pagina_encontrada,timestamp);
		set_pagina_key(pagina_encontrada,key);
		set_pagina_value(pagina_encontrada,valor);
	} else {
		log_info(logger, "NO SE ENCONTRO LA PAGINA %d en la tabla %s => NO SE PUDO COMPLETA LA OPERACION",key,nombre_tabla);
	} 
}

void* get_memoria_libre(){
	int i;
	for (i = 0; i<sizeof(mapa_memoria)/sizeof(mapa_memoria[0]) && mapa_memoria[i] != 0; ++i)
	{

	}
	mapa_memoria[i] = 1;
	return memoria_principal+i*obtener_tamanio_pagina();
}

void crear_pagina(char* nombre_tabla,int key,char* valor,unsigned long timestamp,int un_flag_modificado){
	//creo la pagina en mi estructura de paginas
	pagina_t* nueva_pagina = malloc(sizeof(pagina_t));
	nueva_pagina->flag_modificado = un_flag_modificado;
	//luego la pongo en memoria
	nueva_pagina->puntero_memoria = get_memoria_libre();

	//escribo en la memoria
	set_pagina_key(nueva_pagina,key);
	set_pagina_value(nueva_pagina,valor);
	set_pagina_timestamp(nueva_pagina,timestamp);

	//se lo asigno al segmento que corresponde
	segmento_t* segmento_encontrado = find_segmento(nombre_tabla);
	list_add(segmento_encontrado->paginas,nueva_pagina);

	log_info(logger, "Pagina creada con exito");
}

void crear_segmento(char* nombre_tabla){
	segmento_t* nuevo_segmento = malloc(sizeof(segmento_t));
	nuevo_segmento->nombre = strdup(nombre_tabla);
	t_list* nueva_lista = list_create();
	nuevo_segmento->paginas = nueva_lista;
	list_add(tabla_segmentos,nuevo_segmento);
	log_info(logger, "Segmento creado con exito");

}

segmento_t* find_segmento(char* segmento_buscado){
	log_info(logger, "Se busca el segmento %s",segmento_buscado);

	int key_search(segmento_t* un_segmento){
		return string_equals_ignore_case(un_segmento->nombre, segmento_buscado);
	}

	segmento_t* segmento_encontrado = list_find(tabla_segmentos,(void*)key_search);
	return segmento_encontrado;
}

pagina_t* find_pagina_en_segmento(int key_buscado,segmento_t* segmento_buscado){

	int key_search(pagina_t* pagina_buscada){
		return get_pagina_key(pagina_buscada) == key_buscado;
	}

	pagina_t* pagina_encontrada = list_find(segmento_buscado->paginas,(void*)key_search);
	return pagina_encontrada;
}

//dice si existe un segmento con ese nombre
int existe_segmento(char* segmento_buscado){
	return find_segmento(segmento_buscado) != NULL;
}
//dice si existe una pagina x en el registro y 
int existe_pagina_en_segmento(int pagina_key_buscada,segmento_t* segmento_buscado){
	return find_pagina_en_segmento(pagina_key_buscada,segmento_buscado) != NULL;
}

void set_pagina_timestamp(pagina_t* una_pagina,unsigned long un_timestamp){
	memcpy(una_pagina->puntero_memoria,&un_timestamp,sizeof(unsigned long));
}
void set_pagina_key(pagina_t* una_pagina,int un_key){
	memcpy(una_pagina->puntero_memoria+sizeof(unsigned long),&un_key,sizeof(int));
}
void set_pagina_value(pagina_t* una_pagina,char* un_value){
	if (sizeof(un_value)>config.value_size){
		log_info(logger,"Error: El valor recibido es mas grande que el config.value_size");
		return;
	}
	//TODO: Ver eso de rellenar el char con null si es menor a lo qe pide
	memcpy(una_pagina->puntero_memoria+sizeof(unsigned long)+sizeof(int),&un_value,config.value_size);
}

unsigned long get_pagina_timestamp(pagina_t* una_pagina){
	unsigned long un_timestamp;
	memcpy(&un_timestamp,una_pagina->puntero_memoria,sizeof(unsigned long));
	return un_timestamp;
}
int get_pagina_key(pagina_t* una_pagina){
	int un_key;
	memcpy(&un_key,una_pagina->puntero_memoria+sizeof(unsigned long),sizeof(int));
	return un_key;
}
char* get_pagina_value(pagina_t* una_pagina){
	char* un_value;
	//TODO: Ver eso de rellenar el char con null si es menor a lo qe pide
	memcpy(&un_value,(una_pagina->puntero_memoria)+sizeof(unsigned long)+sizeof(int),sizeof(char*));
	return un_value;
}


int memoria_esta_full(){
	//la memoria esta full si tiene todas las paginas con flag en 1
	int no_esta_modificada(pagina_t* pagina_buscada){
		return pagina_buscada->flag_modificado == 0;
	}

	//recorro la lista de segmentos
	int i=0;
	segmento_t* un_segmento = list_get(tabla_segmentos,i);
	while(un_segmento != NULL){

		//si alguna de las paginas del segmento estan sin modificar devuelvo FALSE
		if(list_any_satisfy(un_segmento->paginas,(void*)no_esta_modificada)){
			return 0;
		}
		//si estan todas llenas cambio de segmento
		i++;
		un_segmento = list_get(tabla_segmentos,i);
	}
	return 1;
}


int hay_paginas_disponibles(){
	return cantidad_paginas_actuales <= limite_paginas;
}

void sacar_lru(){
	// Esta funcion deberia buscar la pagina que se uso hace mas tiempo
	// con el flag de modificado en 0
	// y sacar esa pagina de las paginas
	return;
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

void tests_memoria(){
	/* A TESTEAR:
	void crear_pagina(char* nombre_tabla,int key,char* valor,unsigned long timestamp,int un_flag_modificado);
	void crear_segmento(char* nombre_tabla);
	segmento_t* find_segmento(char* segmento_buscado);
	pagina_t* find_pagina_en_segmento(int key_buscado,segmento_t* segmento_buscado);
	int obtener_tamanio_pagina();
	void actualizar_pagina(char* nombre_tabla,int key,char* valor,unsigned long timestamp);
	void sacar_lru();
	int hay_paginas_disponibles();
	int memoria_esta_full();
	int insert_mem(char * nombre_tabla, int key, char * valor, unsigned long timestamp);
	int existe_segmento(char* );
	int existe_pagina_en_segmento(int pagina_buscada,segmento_t* );

	void set_pagina_timestamp(pagina_t* una_pagina,unsigned long un_timestamp);
	void set_pagina_key(pagina_t* una_pagina,int un_key);
	void set_pagina_value(pagina_t* una_pagina,char* un_value);

	unsigned long get_pagina_timestamp(pagina_t* una_pagina);
	int get_pagina_key(pagina_t* una_pagina);
	char* get_pagina_value(pagina_t* una_pagina);
	*/
	insert_mem("A",2,"valor",unix_epoch());
	insert_mem("A",2,"valor",unix_epoch());
	insert_mem("A",2,"valor",unix_epoch());
	insert_mem("A",2,"valor",unix_epoch());
	insert_mem("B",2,"valor",unix_epoch());
}
