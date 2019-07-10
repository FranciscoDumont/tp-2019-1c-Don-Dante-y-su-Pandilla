#include <dalibrary/libmain.h>

t_log * logger;
t_config * config_file = null;
MEMConfig config;

t_list * gossiping_list;

void* memoria_principal;
int* mapa_memoria;
int mapa_memoria_size;
int cantidad_paginas_actuales;
int limite_paginas;

int total_operations;
int read_operations;
int write_operations;

t_list* tabla_segmentos;

typedef struct {
	char* nombre;
	char* path;
	t_list* paginas;
} segmento_t;

typedef struct {
	int nro_pagina; //corresponde con el indice en el mapa de memorias
	void* puntero_memoria;
	int flag_modificado;
	unsigned long timestamp_ultima_modificacion;
} pagina_t;

t_list * instruction_list;

pthread_mutex_t journal_by_time;

void gossiping_start(pthread_t * thread);
void server_start(pthread_t * thread);
void journal_start(pthread_t * journal_thread);
void journal_routine();


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
void liberar_segmento(char* table_name);
void consola_mem();


//TODO: Implementar luego
int insert_mem(char * table_name, int key, char * value, unsigned long timestamp);
int create_mem(char * table_name, ConsistencyTypes consistency, int partitions, int compaction_time);
char * select_mem(char * table_name, int key);
t_list * describe_mem(char * table_name);
int drop_mem(char * table_name);
void execute_mem(comando_t* unComando);
void info();

void add_instruction(Instruction* i);
int journal();
void delete_instructions(char * table_name);
int modified_page(char * table_name, int key);
int is_drop(Instruction* i);
void free_tables();
int insert_into_lfs(char * nombre_tabla, int key, char * valor, unsigned long timestamp);


void set_pagina_timestamp_modificado(pagina_t * p, unsigned long timestamp_modificacion);
void mostrarPaginas();


int main(int argc, char **argv) {
	if (argc != 2) {
		config_file = config_create("mem03.cfg");
	} else {
		config_file = config_create(argv[1]);
	}

	MUTEX_DEBUG_LEVEL = MX_ALL_DISPLAY;

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
	logger = log_create(memory_logger_path, "MEM", false, LOG_LEVEL_TRACE);

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

	int mutex_ok = init_normal_mutex(&journal_by_time, "Journal mutex");

	if(mutex_ok){
		log_info(logger, "Se crea el mutex para el journal por tiempo");
	}else{
		log_info(logger, "No se pudo crear el mutex para el journal por tiempo");
	}

	total_operations = 0;
	write_operations = 0;
	read_operations = 0;

	pthread_t journal_thread;
	journal_start(&journal_thread);

	//Inicializo las variables globales
	tabla_segmentos = list_create();
	cantidad_paginas_actuales = 0;
	limite_paginas = config.memsize / obtener_tamanio_pagina();
	mapa_memoria_size = limite_paginas;
	mapa_memoria = calloc(limite_paginas,sizeof(int));
	memoria_principal = malloc(config.memsize);
	log_info(logger, "Se pueden almacenar %d páginas", limite_paginas);

	instruction_list = list_create();

	pthread_t mem_console_id;
	pthread_create(&mem_console_id, NULL, consola_mem, NULL);
	
	pthread_join(thread_g, NULL);
	pthread_join(thread_server, NULL);
	pthread_join(journal_thread, NULL);

	pthread_join(mem_console_id, NULL);

	free(mapa_memoria);
	return EXIT_SUCCESS;
}

int obtener_tamanio_pagina() {
	return sizeof(unsigned long) +
			sizeof(int) +
			config.value_size;
}

void journal_start(pthread_t * journal_thread){
	pthread_create(journal_thread, NULL, journal_routine, NULL);

}

void journal_routine(){
	while(1) {

		sleep(config.journal_time / 1000);

		log_info(logger, "Se inicia el JOURNAL por tiempo");
		journal();

	}
}


//TODO: Manejar el tema de timestamp desde la funcion que llama a esta
int insert_mem(char * nombre_tabla, int key, char * valor, unsigned long timestamp) {
	custom_print("Inicio insert(%s,%d,%s,%lu)",nombre_tabla,key,valor,timestamp);
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
					log_info(logger, "\tEmpiezo journal desde el insert porque esta llena la memoria");
					journal();
					insert_mem(nombre_tabla,key,valor,timestamp);

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
	total_operations++;
	write_operations++;
	return EXIT_SUCCESS;
}

int create_mem(char * table_name, ConsistencyTypes consistency, int partitions, int compaction_time){
	log_info(logger, "INICIA CREAR TABLA: create_mem(%s, %d, %d, %d)", table_name, consistency, partitions, compaction_time);
	int exit_value;
	send_data(config.lfs_socket, MEM_LFS_CREATE, 0, null);

	int table_name_len = strlen(table_name)+1;

	send(config.lfs_socket, &table_name_len,  sizeof(int), 0);
	send(config.lfs_socket, table_name,       table_name_len,0);
	send(config.lfs_socket, &consistency,     sizeof(int), 0);
	send(config.lfs_socket, &partitions,      sizeof(int), 0);
	send(config.lfs_socket, &compaction_time, sizeof(int), 0);

	MessageHeader * header = malloc(sizeof(MessageHeader));
	recieve_header(config.lfs_socket, header);
	if(header->type == OPERATION_SUCCESS) {
		log_info(logger, "LFS ANSWERED SUCCESFULLY");
		log_info(logger, "TABLA CREADA EN EL FILESYSTEM");
		crear_segmento(table_name);
		exit_value = EXIT_SUCCESS;
	} else {
		log_info(logger, "LFS NO PUDO CREAR LA TABLA");
		exit_value = EXIT_FAILURE;
	}

	total_operations++;
	return exit_value;
}


char * select_mem(char * table_name, int key){
	custom_print("Inicio select %s, %d", table_name, key);
	total_operations++;
	read_operations++;

	//Buscamos el segmento y verificamos su existencia
	segmento_t* segmento_buscado = find_segmento(table_name);

	if(segmento_buscado){

		log_info(logger, "El segmento %s existe, buscando key solicitada en las páginas del mismo", table_name);
		//Si el segmento existe, buscamos la pagina y verificamos que la contenga

		pagina_t* key_buscada = find_pagina_en_segmento(key, segmento_buscado);

		if(key_buscada){
			log_info(logger, "Se encontró la key %d en el segmento %s", key, table_name);
			char* key_value = get_pagina_value(key_buscada);
			log_info(logger,"VALOR = %s", key_value);

			//Si la pagina esta contenida en el segmento, retornamos el valor de la misma.
			return key_value;
		} else {
			//Si no la contiene, envíamos la solicitud a FileSystem para obtener el valor solicitado y almacenarlo.
			log_info(logger, "La key %d no está contenida en el segmento %s", key, table_name);
			log_info(logger, "Enviando petición a LFS...");
			int exit_value;
			 
			send_data(config.lfs_socket, MEM_LFS_SELECT, 0, null);
			int table_name_len = strlen(table_name)+1;

			send(config.lfs_socket, &table_name_len, sizeof(int), 0);
			send(config.lfs_socket, table_name, table_name_len,0);
			send(config.lfs_socket, &key, sizeof(int), 0);

			MessageHeader * header = malloc(sizeof(MessageHeader));

			recieve_header(config.lfs_socket, header);
			log_info(logger, "LFS RESPONDE");
			if(header->type == OPERATION_SUCCESS) {
				log_info(logger, "LFS ENVÍA RESULTADO DE SELECT SOLICITADO");
				int rl;
				recv(config.lfs_socket, &rl, sizeof(int), 0);
				char * value = malloc(sizeof(char) * rl);
				recv(config.lfs_socket, value, sizeof(char) * rl, 0);

				log_info(logger, "  EL VALOR RECIBIDO ES %s", value);

				exit_value = EXIT_SUCCESS;
				//Si el filesystem responde la solicitud con éxito creo una pagina nueva
				if (hay_paginas_disponibles()){
					log_info(logger, "Hay páginas disponibles, la página se creará");
					unsigned long timestamp = unix_epoch();
					crear_pagina(table_name,key,value,timestamp,1);//valorkey?
					pagina_t* key_buscada = find_pagina_en_segmento(key, segmento_buscado);
					log_info(logger, "Se devuelve el valor de la pagina");
					char* value = get_pagina_value(key_buscada);
					log_info(logger, "VALOR= %s", value);
					return value;
				}else{
					log_info(logger, "No hay páginas disponibles");
			
					if (memoria_esta_full()){
						//Hacer el journal
						journal();

					}else {
						//ejecutamos el algoritmo de reemplazo
						log_info(logger, "\tSe ejecutará el algoritmo de reemplazo(LRU)...");
						sacar_lru();
						//		crear_pagina(table_name,key,valor_key,timestamp,1);
					}
				}
			} else {
				log_info(logger, "SELECT NO SE PUDO REALIZAR");

				exit_value = EXIT_FAILURE;
			}

		}
	} else {
		log_info(logger, "El segmento %s no existe", table_name);
		int exit_value;

		send_data(config.lfs_socket, MEM_LFS_SELECT, 0, null);
		int table_name_len = strlen(table_name)+1;

		send(config.lfs_socket, &table_name_len, sizeof(int), 0);
		send(config.lfs_socket, table_name, table_name_len,0);
		send(config.lfs_socket, &key, sizeof(int), 0);

		MessageHeader * header = malloc(sizeof(MessageHeader));

		recieve_header(config.lfs_socket, header);
		log_info(logger, "LFS RESPONDE");
		if(header->type == OPERATION_SUCCESS) {
			log_info(logger, "LFS ENVÍA RESULTADO DE SELECT SOLICITADO");
			int rl;
			recv(config.lfs_socket, &rl, sizeof(int), 0);
			char * value = malloc(sizeof(char) * rl);
			recv(config.lfs_socket, value, sizeof(char) * rl, 0);

			log_info(logger, "  EL VALOR RECIBIDO ES %s", value);

			exit_value = EXIT_SUCCESS;
		
	}
	}

}

t_list * describe_mem(char * table_name){
	t_list * result = list_create();
	log_info(logger, "INICIA DESCRIBE: describe_mem(%s)", table_name);
	int exit_value = EXIT_FAILURE, f, a;
	send_data(config.lfs_socket, MEM_LFS_DESCRIBE, 0, null);

	total_operations++;

	if(strcmp(table_name, "") == 0){
		int z = 0;
		send(config.lfs_socket, &z, sizeof(int), 0);
		recv(config.lfs_socket, &f, sizeof(int), 0);

		for(a=0 ; a<f ; a++) {
			MemtableTableReg * table = malloc(sizeof(MemtableTableReg));

			int table_n_l;
			recv(config.lfs_socket, table, sizeof(MemtableTableReg), 0);
			recv(config.lfs_socket, &table_n_l, sizeof(int), 0);
			table->table_name = malloc(table_n_l * sizeof(char));
			recv(config.lfs_socket, table->table_name, table_n_l * sizeof(char), 0);

			list_add(result, table);

			exit_value = EXIT_SUCCESS;
		}
	} else {
		int table_name_len = strlen(table_name)+1;
		send(config.lfs_socket, &table_name_len,  sizeof(int), 0);
		send(config.lfs_socket, table_name,       table_name_len, 0);

		recv(config.lfs_socket, &f, sizeof(int), 0);
		log_info(logger, "(%d)", f);
		if(f == 0) {
			exit_value = EXIT_FAILURE;
			list_destroy(result);
			result = null;
		} else {
			MemtableTableReg * table = malloc(sizeof(MemtableTableReg));
			recv(config.lfs_socket, table, sizeof(MemtableTableReg), 0);
			table->table_name = table_name;

			list_add(result, table);

			exit_value = EXIT_SUCCESS;
		}
	}

	return result;
}

int drop_mem(char * table_name){
	log_info(logger, "INICIA DROP: drop_mem(%s)", table_name);
	total_operations++;
	//Verifica si existe un segmento de dicha tabla en la
	//memoria principal y de haberlo libera dicho espacio.
	if (existe_segmento(table_name)){
		liberar_segmento(table_name);
	}
	//Informa al FileSystem dicha operación para que este último realice la operación adecuada
	int exit_value;
	send_data(config.lfs_socket, MEM_LFS_DROP, 0, null);

	int table_name_len = strlen(table_name)+1;

	send(config.lfs_socket, &table_name_len,  sizeof(int), 0);
	send(config.lfs_socket, table_name,       table_name_len,0);

	MessageHeader * header = malloc(sizeof(MessageHeader));
	recieve_header(config.lfs_socket, header);
	if(header->type == OPERATION_SUCCESS) {
		log_info(logger, "LFS ANSWERED SUCCESFULLY");
		log_info(logger, "DROP EN EL FILESYSTEM");
		exit_value = EXIT_SUCCESS;
	} else {
		log_info(logger, "DROP ERROR");
		exit_value = EXIT_FAILURE;
	}

	return exit_value;


}

// instruction_list es una variable global

int journal(){
	total_operations++;
	int r;
	r = lock_mutex(&journal_by_time);
	if(r == 0){

		//Solo se journalea el insert

		custom_print("Se inicia el JOURNAL");
		int elements_count = list_size(instruction_list);
		log_info(logger, "J\tEl tamaño de la lista de instrucciones es: %d", elements_count);
		Instruction * i;
		i = list_get(instruction_list, 0);
		int step = 1;
	
		if(0 == elements_count){
			log_info(logger, "No hay instrucciones para journalear");
		}else{
			while(step <= elements_count){
				log_info(logger, "J\tPaso %d de %d", step, elements_count);
				if(modified_page(i -> table_name, i->key)){
					switch(i -> i_type){
						case INSERT:
							log_info(logger, "J\tEs un INSERT");

							char * nombre_tabla = i -> table_name;
							int key = i -> key;
							char * valor = i -> value;
							unsigned long timestamp = i -> timestamp;
							insert_into_lfs(nombre_tabla, key, valor, timestamp);

							}
							break;
							/*
						case CREATE:
							log_info(logger, "J\tEs un CREATE");
							create_mem(i -> table_name, i -> c_type, i -> partitions, i -> compaction_time);
							break;
							 */
					}
				}
				/*
				else {
					switch(i -> i_type){
						case SELECT:
							log_info(logger, "J\tEs un SELECT");
							select_mem(i -> table_name, i -> key);
							break;
						case DESCRIBE:
							log_info(logger, "J\tEs un DESCRIBE");
							describe_mem(i -> table_name);
							break;
					}

				}
				 */
				i = list_get(instruction_list, step);
				step++;
			}

			free_tables(instruction_list);
			list_clean(instruction_list);
	}
	r = unlock_mutex(&journal_by_time);
	return EXIT_SUCCESS;
}


int insert_into_lfs(char * nombre_tabla, int key, char * valor, unsigned long timestamp){
	//El journal usa esta funcion para pasarle a LFS todos los inserts que tiene que hacer

	log_info(logger, "LE MANDO UN INSERT A LFS: insert_into_lfs(%s, %d, %s, %d)", nombre_tabla, key, valor, timestamp);
	int exit_value;
	send_data(config.lfs_socket, MEM_LFS_INSERT, 0, null);

	int table_name_len = strlen(nombre_tabla)+1;
	int valor_len = strlen(valor)+1;

	send(config.lfs_socket, &table_name_len, sizeof(int), 0);
	send(config.lfs_socket, nombre_tabla,    table_name_len,0);
	send(config.lfs_socket, &key,            sizeof(int), 0);
	send(config.lfs_socket, &valor_len,      sizeof(int), 0);
	send(config.lfs_socket, valor,           valor_len,0);
	send(config.lfs_socket, &timestamp,      sizeof(unsigned long), 0);

	MessageHeader * header = malloc(sizeof(MessageHeader));
	recieve_header(config.lfs_socket, header);
	if(header->type == OPERATION_SUCCESS) {
		log_info(logger, "LFS ANSWERED SUCCESFULLY");
		log_info(logger, "INSERT EN EL FILESYSTEM");
		exit_value = EXIT_SUCCESS;
	} else {
		log_info(logger, "LFS NO PUDO INSERTAR EL REGISTRO");
		exit_value = EXIT_FAILURE;
	}
	return exit_value;
}


int modified_page(char * table_name, int key){

	segmento_t * aux_segment = find_segmento(table_name);
	pagina_t * aux_page = find_pagina_en_segmento(key, aux_segment);

	return aux_page -> flag_modificado;
}


void free_tables(){

	int elements_count = list_size(instruction_list);
	Instruction * i;
	i = list_get(instruction_list, 0);
	int step = 1;

	while(step <= elements_count){

		liberar_segmento(i -> table_name);

		i = list_get(instruction_list, step);
		step++;
	}

}


void add_instruction(Instruction* i){

	if(is_drop(i) && !list_is_empty(instruction_list)){
		delete_instructions(i -> table_name);
		drop_mem(i -> table_name);
	}else if(is_drop(i) && list_is_empty(instruction_list)){
		drop_mem(i -> table_name);
	}else{
		list_add(instruction_list, i);
	}

}


void delete_instructions(char * target_table){

	int elements_count = list_size(instruction_list);
	Instruction * i;
	i = list_get(instruction_list, 0);
	int step = 0;

	while(step < elements_count){
		if(strcmp(i -> table_name, target_table) == 0)
			list_remove(instruction_list, step);
		step++;
		i = list_get(instruction_list, step);
	}

	//free(i);

}


int is_drop(Instruction* i){
	return (i -> i_type == DROP);
}


/*

void show_list(InstructionList list){
	list * run = first;
	custom_print("elements: \n");
	while(run != NULL){
		custom_print(%i - ", run -> i -> i_type);
		custom_print(%i - ", run -> i -> table_name);
		custom_print(%i - ", run -> i -> key);
		custom_print(%i - ", run -> i -> value);
		custom_print(%i - ", run -> i -> c_type);
		custom_print(%i - ", run -> i -> partitions);
		custom_print(%i - ", run -> i -> compaction_time);

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

void get_memoria_libre(pagina_t* una_pagina){
	//Asigna puntero_memoria y nro_pagina
	int i;
	for (i = 0; i<mapa_memoria_size && mapa_memoria[i] != 0; ++i)
	{

	}
	mapa_memoria[i] = 1;
	log_info(logger, "Mapa memoria consigue el indice: %d/%d", i, mapa_memoria_size);
	void* nuevo_puntero_a_memoria = memoria_principal+i*obtener_tamanio_pagina();

	una_pagina->puntero_memoria = nuevo_puntero_a_memoria;
	una_pagina->nro_pagina = i;
}

void crear_pagina(char* nombre_tabla,int key,char* valor,unsigned long timestamp,int un_flag_modificado){
	//creo la pagina en mi estructura de paginas
	pagina_t* nueva_pagina = malloc(sizeof(pagina_t));
	nueva_pagina->flag_modificado = un_flag_modificado;
	//luego la pongo en memoria
	get_memoria_libre(nueva_pagina);

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
	memcpy(una_pagina->puntero_memoria+(sizeof(unsigned long)+sizeof(int)),un_value,config.value_size);
}

void set_pagina_timestamp_modificado(pagina_t * una_pagina, unsigned long un_timestamp_modificado){
	memcpy(una_pagina->puntero_memoria+sizeof(unsigned long)+sizeof(int)+config.value_size,&un_timestamp_modificado,sizeof(unsigned long));  
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
	un_value = strdup((una_pagina->puntero_memoria)+(sizeof(unsigned long)+sizeof(int)));
	return un_value;
}

unsigned long get_pagina_timestamp_modificado(pagina_t* una_pagina){
	unsigned long un_timestamp_modificado;
	memcpy(&un_timestamp_modificado,una_pagina->puntero_memoria+sizeof(unsigned long)+sizeof(int)+config.value_size,sizeof(unsigned long));
	return un_timestamp_modificado;
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

void mostrarPaginas(){
	int size = list_size(tabla_segmentos);

	for(int i=0; i<size; i++){
		segmento_t * s = list_get(tabla_segmentos, i);
		int int_cant_pags = list_size(s->paginas);
		log_info(logger, "segmento %s nro paginas = %d \n", s->nombre, int_cant_pags);
		for(int x=0; x<int_cant_pags; x++){
			pagina_t * t = list_get(s->paginas, x);
			unsigned long ts = get_pagina_timestamp(t);
			char * valor = get_pagina_value(t);
			log_info(logger, "pagina %d timestamp %u valor = %s", t->nro_pagina, ts, valor);
										  }
										

							 }

					  }

void sacar_lru(){
	// Esta funcion deberia buscar la pagina que se uso hace mas tiempo
	// con el flag de modificado en 0
	// y sacar esa pagina de las paginas
	return;
}


void liberar_segmento(char* table_name){
	segmento_t* segmento = find_segmento(table_name);

	void liberar_pagina(pagina_t* una_pagina){
		//no borra los datos en memoria, solo deja disponible el indice en el mapa memoria
		int indice = una_pagina->nro_pagina;
		mapa_memoria[indice] = 0;
		free(una_pagina);
	}

	bool es_el_segmento(segmento_t* s){
		return string_equals_ignore_case(s->nombre, table_name);
	}

	list_iterate(segmento->paginas, (void*) liberar_pagina);
	list_destroy(segmento->paginas);
	list_remove_by_condition(tabla_segmentos, (void *) es_el_segmento);

}


//Gossiping
void inform_gossiping_pool() {
	//log_info(logger, "GOSSIPING LIST START");

	int a;
	for(a = 0 ; a < gossiping_list->elements_count ; a++) {
		MemPoolData * this_mem = list_get(gossiping_list, a);
		log_info(logger, "      %d %s:%d", this_mem->memory_id, this_mem->ip, this_mem->port);
	}

	//log_info(logger, "GOSSIPING LIST END");
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
		log_info(logger, "GOS");

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

			//log_info(logger, "requesting to %s %d", this_seed->ip, this_seed->port);
			if(memsocket != -1) {
				send_data(memsocket, GOSSIPING_REQUEST, 0, NULL);
				recv(memsocket, &(this_seed->memory_id), sizeof(int), 0);
				//log_info(logger, "   ITS %d", this_seed->memory_id);

				add_to_pool(this_seed);

				int torecieve, a;
				recv(memsocket, &torecieve, sizeof(int), 0);
				for(a = 0 ; a < torecieve ; a++) {
					MemPoolData * this_mem = malloc(sizeof(MemPoolData));
					this_mem->ip = malloc(sizeof(char) * IP_LENGTH);

					recv(memsocket, &this_mem->port, sizeof(int), 0);
					recv(memsocket, &this_mem->memory_id, sizeof(int), 0);
					recv(memsocket, this_mem->ip, sizeof(char) * IP_LENGTH, 0);

					//log_info(logger, "   gave me %d in %s %d", this_mem->memory_id, this_mem->ip, this_mem->port);

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

					log_info(logger, "   %d %s %d\n", this_mem->memory_id, this_mem->ip, this_mem->port);

					send(fd, &this_mem->port, sizeof(int), 0);
					send(fd, &this_mem->memory_id, sizeof(int), 0);
					send(fd, this_mem->ip, sizeof(char) * IP_LENGTH, 0);
				}
				break;
			case KNL_MEM_CREATE:
				{
					;
					log_info(logger, "NEW TABLE WILL BE CREATED");

					int table_name_size;
					char * table_name;

					recv(fd, &table_name_size, sizeof(int), 0);
					table_name = malloc(sizeof(table_name_size) * sizeof(char));
					recv(fd, table_name, table_name_size * sizeof(char), 0);

					int consistency, partitions, compaction_time;
					recv(fd, &consistency, sizeof(int), 0);
					recv(fd, &partitions, sizeof(int), 0);
					recv(fd, &compaction_time, sizeof(int), 0);

					int creation_result;
					creation_result = create_mem(table_name, consistency, partitions, compaction_time);

					if(creation_result == false) {
						custom_print("Tabla creada %s\n", table_name);
						send_data(fd, OPERATION_SUCCESS, 0, null);
					} else {
						custom_print("ERROR Tabla NO creada %s\n", table_name);
						send_data(fd, CREATE_FAILED_EXISTENT_TABLE, 0, null);
					}
					break;
				}
			case KNL_MEM_SELECT:
				{
					int table_name_size;
					char * table_name;

					recv(fd, &table_name_size, sizeof(int), 0);
					table_name = malloc(sizeof(table_name_size) * sizeof(char));
					recv(fd, table_name, table_name_size * sizeof(char), 0);

					char * select_result;
					int key_select;
					recv(fd, &key_select, sizeof(int), 0);

					select_result = select_mem(table_name, key_select);

					log_info(logger, "%s", select_result);

					if(select_result == null) {
						custom_print("Select Fallo %s %d por null\n", table_name, key_select);
						send_data(fd, SELECT_FAILED_NO_RESULT, 0, null);
					} else if(strcmp(select_result, "UNKNOWN") == 0) {
						custom_print("Select Fallo %s %d por unknown\n", table_name, key_select);
						send_data(fd, SELECT_FAILED_NO_RESULT, 0, null);
					}else{
						custom_print("Select %s %d = %s\n", table_name, key_select, select_result);
						send_data(fd, OPERATION_SUCCESS, 0, null);
						int res_len = strlen(select_result) + 1;
						send(fd, &res_len, sizeof(int), 0);
						send(fd, select_result, res_len * sizeof(char), 0);
					}
					;
					break;
				}
			case KNL_MEM_INSERT:
				{
					int table_name_size;
					char * table_name;

					recv(fd, &table_name_size, sizeof(int), 0);
					table_name = malloc(sizeof(table_name_size) * sizeof(char));
					recv(fd, table_name, table_name_size * sizeof(char), 0);

					int key;
					int value_len;
					char * value;
					unsigned long timestamp;
					recv(fd, &key, sizeof(int), 0);
					recv(fd, &value_len, sizeof(int), 0);
					value = malloc(value_len * sizeof(char));
					recv(fd, value, value_len * sizeof(char), 0);
					recv(fd, &timestamp, sizeof(long), 0);
					if(value_len > config.value_size){
						log_error(logger, "El valor recibido(%d) es mas grande que el config.value_size(%d)", value_len, config.value_size);
						send_data(fd, VALUE_SIZE_ERROR, 0, null);
						break;
					}

					_Bool insert_result;
					insert_result = insert_mem(table_name, key, value, timestamp);

					if(insert_result){
						custom_print("Insert fallo %s %d %s\n", table_name, key, value);
						send_data(fd, OPERATION_FAILURE, 0, null);
					}else{
						custom_print("Insert realizado %s %d %s\n", table_name, key, value);
						send_data(fd, OPERATION_SUCCESS, 0, null);
					}
					;
					break;
				}
			case KNL_MEM_DROP:
				{
					int table_name_size;
					char * table_name;

					recv(fd, &table_name_size, sizeof(int), 0);
					table_name = malloc(sizeof(table_name_size) * sizeof(char));
					recv(fd, table_name, table_name_size * sizeof(char), 0);

					int drop_result = drop_mem(table_name);

					if(!drop_result){
						custom_print("Drop realizado %s\n", table_name);
						send_data(fd, OPERATION_SUCCESS, 0, null);
					}else{
						custom_print("Drop fallido %s\n", table_name);
						send_data(fd, SELECT_FAILED_NO_TABLE_SUCH_FOUND, 0, null);
					}

					break;
				}
			case KNL_MEM_JOURNAL:
				;
				journal();
				send_data(fd, MEM_KNL_JOURNAL_OK, 0, null);
				break;
			case KNL_MEM_DESCRIBE_METADATA:
				;

				custom_print("Recv de describe");
				t_list * describe_result = describe_mem("");
				int q = describe_result->elements_count, rec;
				send(fd, &q, sizeof(int), 0);
				for(rec=0 ; rec<q ; rec++) {
					MemtableTableReg * table = list_get(describe_result, rec);
					int table_n_l = strlen(table->table_name) + 1;
					send(fd, table, sizeof(MemtableTableReg), 0);
					send(fd, &table_n_l, sizeof(int), 0);
					send(fd, table->table_name, table_n_l * sizeof(char), 0);
				}
				break;
			case GIVE_ME_YOUR_METRICS:
				{
					send(fd, &total_operations, sizeof(int), 0);
					send(fd, &read_operations, sizeof(int), 0);
					send(fd, &write_operations, sizeof(int), 0);
				}
				break;

				break;
		}
	}
	start_server(config.mysocket, &new, &lost, &incoming);
}
void server_start(pthread_t * thread) {
	pthread_create(thread, NULL, server_function, NULL);
}

void consola_mem(){
	crear_consola(execute_mem, "Memoria");
}

//TODO: Completar cuando se tenga la implementacion de las funciones
void execute_mem(comando_t* unComando){
	char comandoPrincipal[20];
	char parametro1[20];
	char parametro2[20];
	char parametro3[20];
	char parametro4[20];
	char parametro5[20];

	//imprimir_comando(unComando);

	strcpy(comandoPrincipal,unComando->comando);
	strcpy(parametro1,unComando->parametro[0]);
	strcpy(parametro2,unComando->parametro[1]);
	strcpy(parametro3,unComando->parametro[2]);
	strcpy(parametro4,unComando->parametro[3]);
	strcpy(parametro5,unComando->parametro[4]);

	Instruction * i = malloc(sizeof(Instruction));

	//SELECT
	if(strcmp(comandoPrincipal,"select")==0){
		if(parametro1[0] == '\0'){
			custom_print("select no recibio el nombre de la tabla\n");
			return;
		}else if (parametro2[0] == '\0'){
			custom_print("select no recibio la key\n");
			return;
		}else{

			/*
			i -> i_type = SELECT;

			//destino, origen
			strcpy(i -> table_name, table_name);


			i -> key = key;

			i -> value = NULL;

			i-> c_type = NULL;
			i-> partitions = NULL;
			i-> compaction_time = NULL;

			add_instruction(i);
			*/

			char * v = select_mem(parametro1, atoi(parametro2));
			custom_print("   El valor es %s", v);
		}

	//INSERT
	}else if (strcmp(comandoPrincipal,"insert")==0){
		if(parametro1[0] == '\0'){
			custom_print("insert no recibio el nombre de la tabla\n");
			return;
		}else if (parametro2[0] == '\0'){
			custom_print("insert no recibio la key\n");
			return;
		}else if (parametro3[0] == '\0'){
			custom_print("insert no recibio el valor\n");
			return;
		}else {

			
			i -> i_type = INSERT;
			i -> table_name = malloc(sizeof(char) * (strlen(parametro1)+1));

			//destino, origen
			strcpy(i -> table_name, parametro1);

			i -> key = atoi(parametro2);

			i -> value = malloc(sizeof(char) * (strlen(parametro3)+1));
			strcpy(i -> value, parametro3);

			i-> c_type = 0;
			i-> partitions = 0;
			i-> compaction_time = 0;
					    
			if (parametro4[0] == '\0'){
				i -> timestamp = unix_epoch();
			} else {			    
				i -> timestamp = strtoul(parametro4,NULL,10);
			}
			insert_mem(parametro1,atoi(parametro2),parametro3, i->timestamp);

			add_instruction(i);
		}

	//CREATE
	}else if (strcmp(comandoPrincipal,"create")==0){
		if(parametro1[0] == '\0'){
			custom_print("create no recibio el nombre de la tabla\n");
			return;
		}else if (parametro2[0] == '\0'){
			custom_print("create no recibio el tipo de consistencia\n");
			return;
		}else if (parametro3[0] == '\0'){
			custom_print("create no recibio la particion\n");
			return;
		}else if (parametro4[0] == '\0'){
			custom_print("create no recibio el tiempo de compactacion\n");
			return;
		}else {

			/*
			i -> i_type = CREATE;

			//destino, origen
			i -> table_name = NULL;

			i -> key = NULL;

			i -> value = NULL;

			i-> c_type = consistency;
			i-> partitions = partitions;
			i-> compaction_time = compaction_time;

			add_instruction(i);
			*/

			create_mem(parametro1,char_to_consistency(parametro2),atoi(parametro3),atoi(parametro4));
		}
	
	//DESCRIBE
	}else if (strcmp(comandoPrincipal,"describe")==0){
		//chekea si parametro es nulo adentro de describe_mem

		/*
		i -> i_type = DESCRIBE;

		//destino, origen
		strcpy(i -> table_name, nombre_tabla);

		i -> key = NULL;

		i -> value = NULL;

		i-> c_type = NULL;
		i-> partitions = NULL;
		i-> compaction_time = NULL;

		add_instruction(i);
		*/

		describe_mem(parametro1);

	//DROP
	}else if (strcmp(comandoPrincipal,"drop")==0){
		if(parametro1[0] == '\0'){
			custom_print("drop no recibio el nombre de la tabla\n");
		}else drop_mem(parametro1);

		//ESTE NO SE JOURNALEA

		/*
		i -> i_type = DROP;

		//destino, origen
		strcpy(i -> table_name, nombre_tabla);

		i -> key = NULL;

		i -> value = NULL;

		i-> c_type = NULL;
		i-> partitions = NULL;
		i-> compaction_time = NULL;

		add_instruction(i);
		*/


	//INFO
	}else if (strcmp(comandoPrincipal,"info")==0){
		info();
	}else if (strcmp(comandoPrincipal,"journal")==0){
		journal();
	}
}

void info(){
    custom_print("SELECT\n La operación Select permite la obtención del valor de una key dentro de una tabla. Para esto, se utiliza la siguiente nomenclatura:\n SELECT [NOMBRE_TABLA] [KEY]\n\n");

    custom_print("INSERT\n La operación Insert permite la creación y/o actualización del valor de una key dentro de una tabla. Para esto, se utiliza la siguiente nomenclatura:\n INSERT [NOMBRE_TABLA] [KEY] “[VALUE]” [Timestamp]\n\n");

    custom_print("CREATE\n La operación Create permite la creación de una nueva tabla dentro del file system. Para esto, se utiliza la siguiente nomenclatura:\n CREATE [NOMBRE_TABLA] [TIPO_CONSISTENCIA] [NUMERO_PARTICIONES] [COMPACTION_TIME]\n\n");

    custom_print("DESCRIBE\n La operación Describe permite obtener la Metadata de una tabla en particular o de todas las tablas que el File System tenga. Para esto, se utiliza la siguiente nomenclatura:\n DESCRIBE [NOMBRE_TABLA]\n\n");

    custom_print("DROP\n La operación Drop permite la eliminación de una tabla del file system. Para esto, se utiliza la siguiente nomenclatura:\n DROP [NOMBRE_TABLA]\n\n");
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

	unsigned long get_pagina_timestamp(pagina_t* una_pagina);
	int get_pagina_key(pagina_t* una_pagina);
	char* get_pagina_value(pagina_t* una_pagina);
	*/
	insert_mem("A",1,"valor1",unix_epoch());
	insert_mem("A",2,"valor2",unix_epoch());
	insert_mem("A",3,"valor3",unix_epoch());
	insert_mem("A",4,"valor4",unix_epoch());
	insert_mem("B",5,"valor5",unix_epoch());
	select_mem("A",1); 
	select_mem("A",2);
	select_mem("A",3);
	select_mem("A",4);
	select_mem("B",5);		
	mostrarPaginas();

	//Test paginas
	pagina_t* pagina_test = malloc(sizeof(pagina_t));
	pagina_test->flag_modificado = 0;
	get_memoria_libre(pagina_test);

	set_pagina_timestamp(pagina_test, 6969ul);
	unsigned long ts = get_pagina_timestamp(pagina_test);
	log_info(logger, "Pagina_timestamp: %lu", ts);
	
	set_pagina_key(pagina_test, 808);
	int k = get_pagina_key(pagina_test);
	log_info(logger, "Pagina_key: %d", k);

	set_pagina_value(pagina_test, "juega lanus");
	char* v = get_pagina_value(pagina_test);
	log_info(logger, "Pagina_value: %s", v);

	//Test segmentos
	crear_segmento("un_segmento");
	segmento_t* s = find_segmento("un_segmento");
	char* mensaje_segmento = strcmp(s->nombre,"un_segmento")==0 ? "find_segmento: BIEN" : "find_segmento: MAL";
	log_info(logger, mensaje_segmento);


	journal();

	//describe_mem("C");
	//drop_mem("C");
}
