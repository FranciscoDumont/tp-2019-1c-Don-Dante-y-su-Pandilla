#include <dalibrary/libmain.h>

t_log * logger;
t_log * metrics_logger;
t_config * config_file = null;
KNLConfig config;

t_list * gossiping_list;

t_list * tables_dict;

unsigned int lql_max_id;
pthread_mutex_t ready_queue_mutex;

t_list * new_queue;
t_list * ready_queue;
t_list * exit_queue;
t_list * exec_threads;

pthread_mutex_t criterions_mutex;

ConsistencyCriterion CriterionStrong;
ConsistencyCriterion CriterionEventual;
ConsistencyCriterion CriterionHash;

ConsistencyCriterion * getCriterionPointer(MemtableTableReg * table);

MemPoolData * getMemoryData(int id);
void running();
_Bool exec_lql_line(LQLScript * lql);
void print_op_debug(Instruction * i);
void gossiping_start(pthread_t * thread);
void metadata_refresh_loop();
void refresh_metadata(_Bool print_x_screen);
void metrics_thread();

//TODO: Implementar luego
_Bool insert_knl(char * table_name, int key, char * value, unsigned long timestamp);
_Bool create_knl(char * table_name, ConsistencyTypes consistency, int partitions, int compaction_time);
_Bool select_knl(char * table_name, int key);
_Bool drop_knl(char * table_name);

void execute_knl(comando_t* unComando);
void info();
void consola_knl();

int main(int argc, char **argv) {
	if (argc != 2) {
		config_file = config_create("knl01.cfg");
	} else {
		config_file = config_create(argv[1]);
	}
	int qa;

	gossiping_list = list_create();
	new_queue = list_create();
	ready_queue = list_create();
	exit_queue = list_create();
	exec_threads = list_create();

	CriterionEventual.type = EVENTUAL_CONSISTENCY;
	CriterionEventual.memories = list_create();
	CriterionEventual.metrics_start_measure = unix_epoch();
	CriterionEventual.rr_next_to_use = 0;

	CriterionHash.type = STRONG_HASH_CONSISTENCY;
	CriterionHash.memories = list_create();
	CriterionHash.metrics_start_measure = unix_epoch();

	CriterionStrong.type = STRONG_CONSISTENCY;
	CriterionStrong.memories = list_create();
	CriterionStrong.metrics_start_measure = unix_epoch();

	tables_dict = list_create();

	lql_max_id = 0;

	init_normal_mutex(&ready_queue_mutex, "READY_QUEUE");
	init_normal_mutex(&criterions_mutex, "CRITERIONS");

	config.a_memory_ip = config_get_string_value(config_file, "IP_MEMORIA");
	config.a_memory_port = config_get_int_value(config_file, "PUERTO_MEMORIA");
	config.quantum = config_get_int_value(config_file, "QUANTUM");
	config.multiprocessing_grade = config_get_int_value(config_file, "MULTIPROCESAMIENTO");
	config.metadata_refresh = config_get_int_value(config_file, "METADATA_REFRESH");
	config.exec_delay = config_get_int_value(config_file, "SLEEP_EJECUCION");
	config.current_multiprocessing = 0;

	logger = log_create("kernel_logger.log", "KNL", true, LOG_LEVEL_TRACE);
	metrics_logger = log_create("metrics_logger.log", "KNL", true, LOG_LEVEL_TRACE);

	pthread_t thread_g;
	gossiping_start(&thread_g);

	for(qa = 0 ; qa < config.multiprocessing_grade ; qa++) {
		pthread_t thread_r;
		pthread_create(&thread_r, NULL, running, qa);
		list_add(exec_threads, &thread_r);
	}

	restart_criterion_stats();

	pthread_t knl_console_id;
	pthread_create(&knl_console_id, NULL, consola_knl, NULL);

	pthread_t metrics_id;
	pthread_create(&metrics_id, NULL, metrics_thread, NULL);


	pthread_t refresh_mtdt_id;
	pthread_create(&refresh_mtdt_id, NULL, metadata_refresh_loop, NULL);

	for(qa = 0 ; qa < config.multiprocessing_grade ; qa++) {
		pthread_t * t = list_get(exec_threads, qa);
		pthread_join(*t, NULL);
	}
	pthread_join(refresh_mtdt_id, NULL);
	pthread_join(thread_g, NULL);
	pthread_join(metrics_id, NULL);

	return EXIT_SUCCESS;
}

void running(int n) {
	LQLScript * this_core_lql = null;
	while(1) {
		if(this_core_lql == null) {
			lock_mutex(&ready_queue_mutex);
			if(ready_queue->elements_count != 0) {
				this_core_lql = list_get(ready_queue, 0);
				list_remove(ready_queue, 0);
			}
			unlock_mutex(&ready_queue_mutex);
		}
		if(this_core_lql != null) {
			this_core_lql->state = EXEC;
			log_info(logger, "RUNNING THREAD %d - LQL %d\n", n, this_core_lql->lqlid);
			if(exec_lql_line(this_core_lql)) {
				this_core_lql->quantum_counter++;
				if(feof(this_core_lql->file)) {
					//EXIT X FEOF
					this_core_lql->state = EXIT;
					list_add(exit_queue, this_core_lql);
					this_core_lql = null;
					log_info(logger, "  EXIT X FEOF\n");
				} else {
					if(this_core_lql->quantum_counter == config.quantum) {
						//EXIT X QUANTUM
						lock_mutex(&ready_queue_mutex);
							this_core_lql->state = READY;
							list_add(ready_queue, this_core_lql);
						unlock_mutex(&ready_queue_mutex);
						this_core_lql = null;
						log_info(logger, "  EXIT X QUANTUM\n");
					} else {
						//CONTINUE
						log_info(logger, "  CONTINUES\n");
					}
				}
			} else {
				//ERROR
				this_core_lql->state = EXIT;
				list_add(exit_queue, this_core_lql);
				this_core_lql = null;
				log_info(logger, "  EXIT X ERROR\n");
			}
		} else {
			log_info(logger, "RUNNING THREAD %d - NO LQL\n", n);
		}
		sleep(config.exec_delay/1000);
	}
}

void add_memory_to_criterion(int memory_id, ConsistencyTypes type) {
	log_info(logger, "ADDING %d TO %s", memory_id, consistency_to_char(type));
	switch(type) {
		case EVENTUAL_CONSISTENCY:
			list_add(CriterionEventual.memories, &memory_id);
			break;
		case STRONG_CONSISTENCY:
			list_add(CriterionStrong.memories, &memory_id);
			break;
		case STRONG_HASH_CONSISTENCY:
			list_add(CriterionHash.memories, &memory_id);
			break;
	}
}

MemPoolData * getMemoryData(int id) {
	_Bool find_by_id(int * mid) {
		return (*mid) == id;
	}
	return list_find(gossiping_list, find_by_id);
}

void run_knl(char * filepath) {
	LQLScript * lql = malloc(sizeof(LQLScript));
	lql->state = NEW;
	lql->lqlid = lql_max_id++;
	list_add(new_queue, lql);

	create_lql(lql, filepath);
	_Bool is_same_pointer(void * _lql) {
		return _lql == lql;
	}
	list_remove_by_condition(new_queue, is_same_pointer);
	lock_mutex(&ready_queue_mutex);
	list_add(ready_queue, lql);
	lql->state = READY;
	unlock_mutex(&ready_queue_mutex);
}

void print_op_debug(Instruction * i) {
	switch(i->i_type) {
		case CREATE:
			printf("CREATE %s %s %d %d", i->table_name, consistency_to_char(i->c_type), i->partitions, i->compaction_time);
			break;
		case SELECT:
			printf("SELECT %s %d", i->table_name, i->key);
			break;
		case INSERT:
			printf("INSERT %s %d %s", i->table_name, i->key, i->value);
			break;
		case DESCRIBE:
			printf("DESCRIBE %s", i->table_name);
			break;
		case DROP:
			printf("DROP %s", i->table_name);
			break;
	}
}

_Bool exec_lql_line(LQLScript * lql) {
	Instruction * i = parse_lql_line(lql);

	print_op_debug(i);

	lock_mutex(&criterions_mutex);
	switch(i->i_type) {
		case CREATE:
			break;
		case SELECT:
			break;
		case INSERT:
			break;
		case DESCRIBE:
			break;
		case DROP:
			break;
	}
	unlock_mutex(&criterions_mutex);

	return true;
}

void journal_to_memories_list(t_list * memories) {
	int i, memsocket;
	for(i=0 ; i<memories->elements_count ; i++) {
		MemPoolData * m = list_get(memories, i);
		if ((memsocket = create_socket()) == -1) {
			memsocket = -1;
		}
		if (memsocket != -1 && (connect_socket(memsocket, m->ip, m->port)) == -1) {
			memsocket = -1;
		}
		if(memsocket != -1) {
			send_data(memsocket, KNL_MEM_JOURNAL, 0, null);
			MessageHeader * h = malloc(sizeof(MessageHeader));
			recieve_header(memsocket, h);
			if(h->type == MEM_KNL_JOURNAL_OK) {
				//SUCCESS
			} else {
				//ERROR
			}
			free(h);
			close(memsocket);
		}
	}
}

void journal_to_criterions_memories() {
	lock_mutex(&criterions_mutex);
	journal_to_memories_list(CriterionEventual.memories);
	journal_to_memories_list(CriterionHash.memories);
	journal_to_memories_list(CriterionStrong.memories);
	unlock_mutex(&criterions_mutex);
}

void refresh_metadata(_Bool print_x_screen) {
	MemPoolData * aMemory; int memsocket;
	if(gossiping_list->elements_count == 0) {
		return;
	}
	aMemory = list_get(gossiping_list, 0);
	if ((memsocket = create_socket()) == -1) {
		memsocket = -1;
	}
	if (memsocket != -1 && (connect_socket(memsocket, aMemory->ip, aMemory->port)) == -1) {
		memsocket = -1;
	}
	if(memsocket != -1) {
		list_clean(tables_dict);
		send_data(memsocket, KNL_MEM_DESCRIBE_METADATA, 0, null);
		int f, a;
		recv(memsocket, &f, sizeof(int), 0);
		for(a=0 ; a<f ; a++) {
			MemtableTableReg * table = malloc(sizeof(MemtableTableReg));

			int table_n_l;
			recv(memsocket, table, sizeof(MemtableTableReg), 0);
			recv(memsocket, &table_n_l, sizeof(int), 0);
			table->table_name = malloc(table_n_l * sizeof(char));
			recv(memsocket, table->table_name, table_n_l * sizeof(char), 0);

			if(print_x_screen)
				log_info(logger, "ASDASD %s %d", table->table_name, table->consistency);

			//TABLE
			list_add(tables_dict, table);
		}
	}
}

void metadata_refresh_loop() {
	while(1) {
		refresh_metadata(false);
		sleep(config.metadata_refresh / 1000);
	}
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

			if(tables_dict->elements_count == 0) {
				refresh_metadata(false);
			}
		}

		close(memsocket);

		//inform_gossiping_pool();

		sleep(config.metadata_refresh / 1000);
	}
}
void gossiping_start(pthread_t * thread) {
	pthread_create(thread, NULL, gossiping_thread, NULL);
}

//Server
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

	//imprimir_comando(unComando);

	strcpy(comandoPrincipal,unComando->comando);
	strcpy(parametro1,unComando->parametro[0]);
	strcpy(parametro2,unComando->parametro[1]);
	strcpy(parametro3,unComando->parametro[2]);
	strcpy(parametro4,unComando->parametro[3]);
	strcpy(parametro5,unComando->parametro[4]);

	//SELECT
	if(strcmp(comandoPrincipal,"select")==0){
		if(parametro1[0] == '\0'){
			log_info(logger, "Indique el nombre de la Tabla");
			return;
		}else if (parametro2[0] == '\0'){
			log_info(logger, "Indique la key");
			return;
		}else {
			select_knl(parametro1, atoi(parametro2));
		}
	//INSERT
	}else if (strcmp(comandoPrincipal,"insert")==0){
		if(parametro1[0] == '\0'){
			log_info(logger, "Indique nombre de Tabla");
			return;
		}else if (parametro2[0] == '\0'){
			log_info(logger, "Indique la key");
			return;
		}else if (parametro3[0] == '\0'){
			log_info(logger, "Indique el valor");
			return;
		}else if (parametro4[0] == '\0'){
			insert_knl(parametro1, atoi(parametro2), parametro3, unix_epoch());
		}else {
			insert_knl(parametro1, atoi(parametro2), parametro3, strtoul(parametro4,NULL,10));
		}
	//CREATE
	}else if (strcmp(comandoPrincipal,"create")==0){
		if(parametro1[0] == '\0'){
			log_info(logger, "create no recibio el nombre de la tabla\n");
			return;
		}else if (parametro2[0] == '\0'){
			log_info(logger, "create no recibio el tipo de consistencia\n");
			return;
		}else if (parametro3[0] == '\0'){
			log_info(logger, "create no recibio la particion\n");
			return;
		}else if (parametro4[0] == '\0'){
			log_info(logger, "create no recibio el tiempo de compactacion\n");
			return;
		}//else create_knl(parametro1,char_to_consistency(parametro2),atoi(parametro3),atoi(parametro4));
	//DESCRIBE
	}else if (strcmp(comandoPrincipal,"describe")==0){
		//chekea si parametro es nulo adentro de describe_knl
		if(strcmp(parametro1, "") == 0) {
			refresh_metadata(true);
		} else {
			refresh_metadata(false);
			_Bool find_table(MemtableTableReg * t) {
				return strcmp(t->table_name, parametro1) == 0;
			}
			MemtableTableReg * result = list_find(tables_dict, find_table);
			if(result == null) {
				printf("La tabla %s no existe", parametro1);
			} else {
				printf("Tabla %s Consistencia %d", result->table_name, result->consistency);
			}
		}
	//DROP
	}else if (strcmp(comandoPrincipal,"drop")==0){
		if(parametro1[0] == '\0'){
			log_info(logger, "Indique el nombre de la Tabla");
		}else {
			drop_knl(parametro1);
		}
	}else if (strcmp(comandoPrincipal,"add")==0){
		int memid;
		if(strcmp(parametro1, "memory") != 0) {
			printf("Wrong syntax. ADD MEMORY");
		} else {
			memid = atoi(parametro2);

			if(strcmp(parametro3, "to") != 0) {
				//ERROR
				printf("Wrong syntax. ADD MEMORY # TO");
			} else {
				ConsistencyTypes consistency = char_to_consistency(parametro4);
				if(consistency == C_UNKNOWN) {
					printf("Wrong syntax. ADD MEMORY # TO [EC/SC/SHC]");
				} else {
					add_memory_to_criterion(memid, consistency);
				}
			}
		}
	}else if (strcmp(comandoPrincipal,"journal")==0){
		journal_to_criterions_memories();
		printf("Journal Realizado");
	} else if (strcmp(comandoPrincipal,"run")==0){
		if(parametro1[0] == '\0'){
			log_info(logger, "Indique el LQL a ejecutar");
		}else {
			run_knl(parametro1);
		}
	} else if (strcmp(comandoPrincipal,"metrics")==0){
		perform_metrics();
	} else if (strcmp(comandoPrincipal,"info")==0){
		//info();
	}
}

MemtableTableReg * find_table(char * name) {
	_Bool is_selected(MemtableTableReg * t) {
		return strcmp(t->table_name, name) == 0;
	}
	return list_find(tables_dict, is_selected);
}

MemPoolData * select_memory_by_table(MemtableTableReg * table) {
	//TODO
	switch(table->consistency) {
	case STRONG_CONSISTENCY:
		if(CriterionStrong.memories->elements_count == 0)
			return null;
		return list_get(CriterionStrong.memories, 0);
		break;
	case STRONG_HASH_CONSISTENCY:
		if(CriterionEventual.rr_next_to_use >= CriterionEventual.memories->elements_count) {
			CriterionEventual.rr_next_to_use = 0;
			if(CriterionEventual.memories->elements_count != 0) {
				CriterionEventual.rr_next_to_use++;
				return list_get(CriterionEventual.memories, 0);
			}
		} else {
			CriterionEventual.rr_next_to_use++;
			return list_get(CriterionEventual.memories, (CriterionEventual.rr_next_to_use-1));
		}
		break;
	case EVENTUAL_CONSISTENCY:
		break;
	}
	return null;
}



//API
_Bool insert_knl(char * table_name, int key, char * value, unsigned long timestamp){
	MemtableTableReg * tReg = find_table(table_name);
	if(tReg == null) {
		log_info(logger, "Tabla no existe");
		return false;
	}

	MemPoolData * selected_memory = select_memory_by_table(tReg);
	if(selected_memory == null) {
		log_error(logger, "No available memory for table");
		return false;
	}

	int memsocket;

	if ((memsocket = create_socket()) == -1) {
		memsocket = -1;
	}
	if (memsocket != -1 && (connect_socket(memsocket, selected_memory->ip, selected_memory->port)) == -1) {
		memsocket = -1;
	}
	if(memsocket == -1) {
		log_error(logger, "Memory was unreachable");
		return false;
	} else {
		int exit_value;
		unsigned long op_start = unix_epoch();
		send_data(memsocket, KNL_MEM_INSERT, 0, null);

		int table_name_len = strlen(table_name)+1;
		send(memsocket, &table_name_len,  sizeof(int), 0);
		send(memsocket, table_name,       table_name_len * sizeof(char),0);

		send(memsocket, &key, sizeof(int), 0);

		int value_len = strlen(value)+1;
		send(memsocket, &value_len,  sizeof(int), 0);
		send(memsocket, value,       value_len * sizeof(char),0);

		send(memsocket, &timestamp, sizeof(long), 0);

		MessageHeader * header = malloc(sizeof(MessageHeader));
		recieve_header(memsocket, header);
		if(header->type == OPERATION_SUCCESS) {
			log_info(logger, "MEM ANSWERED SUCCESFULLY");
			return true;
		} else {
			log_info(logger, "INSERT ERROR");
			return false;
		}

		unsigned long op_end = unix_epoch();
		unsigned long op_length = op_end - op_start;
		ConsistencyCriterion * criterion = getCriterionPointer(tReg);
		criterion->write_acum_count++;
		criterion->write_acum_times += op_length;
	}

	return false;
}

void restart_criterion_stats() {
	CriterionHash.read_acum_count = 0;
	CriterionHash.read_acum_times = 0;
	CriterionHash.write_acum_count = 0;
	CriterionHash.write_acum_times = 0;
	CriterionHash.metrics_start_measure = unix_epoch();

	CriterionStrong.read_acum_count = 0;
	CriterionStrong.read_acum_times = 0;
	CriterionStrong.write_acum_count = 0;
	CriterionStrong.write_acum_times = 0;
	CriterionStrong.metrics_start_measure = unix_epoch();

	CriterionEventual.read_acum_count = 0;
	CriterionEventual.read_acum_times = 0;
	CriterionEventual.write_acum_count = 0;
	CriterionEventual.write_acum_times = 0;
	CriterionEventual.metrics_start_measure = unix_epoch();
}

ConsistencyCriterion * getCriterionPointer(MemtableTableReg * table) {
	switch(table->consistency) {
	case STRONG_HASH_CONSISTENCY:
		return &CriterionHash;
		break;
	case STRONG_CONSISTENCY:
		return &CriterionStrong;
		break;
	case EVENTUAL_CONSISTENCY:
		return &CriterionEventual;
		break;
	}
	return null;
}

void perform_metrics() {
	int a;
	log_info(metrics_logger, "INIT_METRICS_LOGGER_%ul", CriterionHash.metrics_start_measure);

	log_info(metrics_logger, "SHC: Read Latency = %f",
			(CriterionHash.read_acum_count == 0) ? 0 : (CriterionHash.read_acum_times / CriterionHash.read_acum_count));
	log_info(metrics_logger, "SHC: Write Latency = %f",
			(CriterionHash.write_acum_count == 0) ? 0 : (CriterionHash.write_acum_times / CriterionHash.write_acum_count));
	log_info(metrics_logger, "SHC: Read Count = %d",
			CriterionHash.read_acum_count);
	log_info(metrics_logger, "SHC: Write Count = %d",
			CriterionHash.write_acum_count);

	log_info(metrics_logger, " SC: Read Latency = %f",
			(CriterionStrong.read_acum_count == 0) ? 0 : (CriterionStrong.read_acum_times / CriterionStrong.read_acum_count));
	log_info(metrics_logger, " SC: Write Latency = %f",
			(CriterionStrong.write_acum_count == 0) ? 0 : (CriterionStrong.write_acum_times / CriterionStrong.write_acum_count));
	log_info(metrics_logger, " SC: Read Count = %d",
			CriterionStrong.read_acum_count);
	log_info(metrics_logger, " SC: Write Count = %d",
			CriterionStrong.write_acum_count);

	log_info(metrics_logger, " EC: Read Latency = %f",
			(CriterionEventual.read_acum_count == 0) ? 0 : (CriterionEventual.read_acum_times / CriterionEventual.read_acum_count));
	log_info(metrics_logger, " EC: Write Latency = %f",
			(CriterionEventual.write_acum_count == 0) ? 0 : (CriterionEventual.write_acum_times / CriterionEventual.write_acum_count));
	log_info(metrics_logger, " EC: Read Count = %d",
			CriterionEventual.read_acum_count);
	log_info(metrics_logger, " EC: Write Count = %d",
			CriterionEventual.write_acum_count);

	for(a=0 ; a<gossiping_list->elements_count ; a++) {
		int memsocket;
		MemPoolData * memory = list_get(gossiping_list, a);
		if ((memsocket = create_socket()) == -1) {
			memsocket = -1;
		}
		if (memsocket != -1 && (connect_socket(memsocket, memory->ip, memory->port)) == -1) {
			memsocket = -1;
		}
		if(memsocket == -1) {
		} else {
			send_data(memsocket, GIVE_ME_YOUR_METRICS, 0, null);
			int to, ro, wo;
			recv(memsocket, &to, sizeof(int), 0);
			recv(memsocket, &ro, sizeof(int), 0);
			recv(memsocket, &wo, sizeof(int), 0);

			log_info(metrics_logger, "MEM[%d] Memory Load = %f",
							memory->memory_id, ((ro+wo) / to));
		}
	}

	log_info(metrics_logger, "END_METRICS_LOGGER_%ul", CriterionHash.metrics_start_measure);
}

void metrics_thread() {
	while(1) {
		perform_metrics();
		restart_criterion_stats();
		sleep(30);
	}
}

_Bool select_knl(char * table_name, int key){
	MemtableTableReg * tReg = find_table(table_name);
	if(tReg == null) {
		log_info(logger, "Tabla no existe");
		return false;
	}

	MemPoolData * selected_memory = select_memory_by_table(tReg);
	if(selected_memory == null) {
		log_error(logger, "No available memory for table");
		return false;
	}

	int memsocket;

	if ((memsocket = create_socket()) == -1) {
		memsocket = -1;
	}
	if (memsocket != -1 && (connect_socket(memsocket, selected_memory->ip, selected_memory->port)) == -1) {
		memsocket = -1;
	}
	if(memsocket == -1) {
		log_error(logger, "Memory was unreachable");
		return false;
	} else {
		int exit_value;
		unsigned long op_start = unix_epoch();
		send_data(memsocket, KNL_MEM_SELECT, 0, null);

		int table_name_len = strlen(table_name)+1;
		send(memsocket, &table_name_len,  sizeof(int), 0);
		send(memsocket, table_name,       table_name_len * sizeof(char),0);

		send(memsocket, &key, sizeof(int), 0);

		MessageHeader * header = malloc(sizeof(MessageHeader));
		recieve_header(memsocket, header);
		if(header->type == OPERATION_SUCCESS) {
			log_info(logger, "MEM ANSWERED SUCCESFULLY");
			log_info(logger, "MEM ENVÍA RESULTADO DE SELECT SOLICITADO");
			int result_len;
			recv(memsocket, &result_len, sizeof(int), 0);
			char* value = malloc(sizeof(char) * result_len);
			recv(memsocket, value, result_len * sizeof(char), 0);
			log_info(logger, "  EL VALOR RECIBIDO ES %s", value);
			exit_value = EXIT_SUCCESS;
			return true;
		} else {
			log_info(logger, "SELECT ERROR");
			return false;
		}

		unsigned long op_end = unix_epoch();
		unsigned long op_length = op_end - op_start;
		ConsistencyCriterion * criterion = getCriterionPointer(tReg);
		criterion->read_acum_count++;
		criterion->read_acum_times += op_length;
	}
	return false;
}

_Bool drop_knl(char * table_name){
	MemtableTableReg * tReg = find_table(table_name);
	if(tReg == null) {
		log_info(logger, "Tabla no existe");
		return false;
	}

	MemPoolData * selected_memory = select_memory_by_table(tReg);
	if(selected_memory == null) {
		log_error(logger, "No available memory for table");
		return false;
	}

	int memsocket;

	if ((memsocket = create_socket()) == -1) {
		memsocket = -1;
	}
	if (memsocket != -1 && (connect_socket(memsocket, selected_memory->ip, selected_memory->port)) == -1) {
		memsocket = -1;
	}
	if(memsocket == -1) {
		log_error(logger, "Memory was unreachable");
		return false;
	} else {
		int exit_value;
		send_data(memsocket, KNL_MEM_DROP, 0, null);

		int table_name_len = strlen(table_name)+1;
		send(memsocket, &table_name_len,  sizeof(int), 0);
		send(memsocket, table_name,       table_name_len * sizeof(char),0);

		MessageHeader * header = malloc(sizeof(MessageHeader));
		recieve_header(memsocket, header);
		if(header->type == OPERATION_SUCCESS) {
			log_info(logger, "MEM ANSWERED SUCCESFULLY");
			log_info(logger, "DROP EN EL FILESYSTEM");
			exit_value = EXIT_SUCCESS;
			return true;
		} else {
			log_info(logger, "DROP ERROR");
			exit_value = EXIT_FAILURE;
			return false;
		}

		return exit_value;
	}
	return false;
}


/*int create_knl(char * table_name, ConsistencyTypes consistency, int partitions, int compaction_time){
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
}*/
