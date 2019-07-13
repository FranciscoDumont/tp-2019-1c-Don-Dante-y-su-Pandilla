#include <dalibrary/libmain.h>

t_log * logger;
t_log * metrics_logger;
t_config * config_file = null;
KNLConfig config;

t_list * gossiping_list;

t_list * tables_dict;
pthread_mutex_t tables_mutex;

int max_multiprocessing_grade;

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

MemPoolData * select_memory_by_consistency(ConsistencyTypes type, int key);

//TODO: Implementar luego
_Bool insert_knl(char * table_name, int key, char * value, unsigned long timestamp);
_Bool create_knl(char * table_name, ConsistencyTypes consistency, int partitions, int compaction_time);
_Bool select_knl(char * table_name, int key);
_Bool drop_knl(char * table_name);

void execute_knl(comando_t* unComando);
void info();
void consola_knl();

char * config_path;
void notify_config_thread();
int main(int argc, char **argv) {
	if (argc != 2) {
		config_path = strdup("knl01.cfg");
	} else {
		config_path = strdup(argv[1]);
	}
	inform_thread_id("main");
	int qa;

	gossiping_list = list_create();
	new_queue = list_create();
	ready_queue = list_create();
	exit_queue = list_create();
	exec_threads = list_create();

	max_multiprocessing_grade = 0;
	_read_config();
	pthread_t config_notify_thread;
	pthread_create(&config_notify_thread, NULL, notify_config_thread, NULL);
	pthread_detach(config_notify_thread);

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
	init_normal_mutex(&tables_mutex, "TABLES_MUTEX");

	logger = log_create("kernel_logger.log", "KNL", false, LOG_LEVEL_TRACE);
	metrics_logger = log_create("metrics_logger.log", "KNL", false, LOG_LEVEL_TRACE);

	pthread_t thread_g;
	gossiping_start(&thread_g);

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

void notify_config_thread() {
	int inotifyFd = inotify_init();
	int wd = inotify_add_watch(inotifyFd, config_path, IN_ALL_EVENTS);
	int BUF_LEN = 1024 * 10;
	char buf[BUF_LEN] __attribute__ ((aligned(8)));
	ssize_t numRead;
	char *p;
	struct inotify_event *event;

	for (;;) {                                  /* Read events forever */
		numRead = read(inotifyFd, buf, BUF_LEN);
		if (numRead == 0)
			printf("read() from inotify fd returned 0!");

		if (numRead == -1)
			printf("read");

		for (p = buf; p < buf + numRead; ) {
			event = (struct inotify_event *) p;
			read_config(event);

			p += sizeof(struct inotify_event) + event->len;
		}
	}
}

void read_config(struct inotify_event *i) {
	if (i->mask & IN_CLOSE_WRITE) {
		_read_config();
	}
}
void _read_config() {
	int nmpp, qa;
	config_file = config_create(config_path);

	config.a_memory_ip = config_get_string_value(config_file, "IP_MEMORIA");
	config.a_memory_port = config_get_int_value(config_file, "PUERTO_MEMORIA");
	config.quantum = config_get_int_value(config_file, "QUANTUM");
	nmpp = config_get_int_value(config_file, "MULTIPROCESAMIENTO");

	if(nmpp > max_multiprocessing_grade) {
		int qa  = max_multiprocessing_grade;

		for(qa; qa < nmpp ; qa++) {
			pthread_t thread_r;
			pthread_create(&thread_r, NULL, running, qa);
			list_add(exec_threads, &thread_r);
		}

		max_multiprocessing_grade = nmpp;
	}

	config.multiprocessing_grade = nmpp;
	config.metadata_refresh = config_get_int_value(config_file, "METADATA_REFRESH");
	config.exec_delay = config_get_int_value(config_file, "SLEEP_EJECUCION");
}

void running(int n) {
	custom_print("Launched rthred %d", n);
	inform_thread_id("running");
	LQLScript * this_core_lql = null;
	while(1) {
		if(n < config.multiprocessing_grade) {
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
				custom_print("RUNNING THREAD %d - LQL %d (x%d) %s\n", n, this_core_lql->lqlid, this_core_lql->instruction_counter, this_core_lql->lql_name);
				if(exec_lql_line(this_core_lql)) {
					this_core_lql->quantum_counter++;
					if(feof(this_core_lql->file)) {
						//EXIT X FEOF
						this_core_lql->state = EXIT;
						list_add(exit_queue, this_core_lql);
						custom_print("  %d EXIT X FEOF\n", this_core_lql->lqlid);
						this_core_lql = null;
					} else {
						if(this_core_lql->quantum_counter == config.quantum) {
							//EXIT X QUANTUM
							lock_mutex(&ready_queue_mutex);
								this_core_lql->state = READY;
								list_add(ready_queue, this_core_lql);
							unlock_mutex(&ready_queue_mutex);
							this_core_lql->quantum_counter = 0;
							custom_print("  EXIT X QUANTUM\n");
							this_core_lql = null;
						} else {
							//CONTINUE
						}
					}
				} else {
					//ERROR
					this_core_lql->state = EXIT;
					list_add(exit_queue, this_core_lql);
					custom_print("  %d EXIT X ERROR\n", this_core_lql->lqlid);
					this_core_lql = null;
				}
			} else {
				//log_info(logger, "RUNNING THREAD %d - NO LQL\n", n);
			}
		} else {
			if (this_core_lql != null) {
				lock_mutex(&ready_queue_mutex);
					this_core_lql->state = READY;
					list_add(ready_queue, this_core_lql);
				unlock_mutex(&ready_queue_mutex);
				this_core_lql->quantum_counter = 0;
				this_core_lql = null;
			}
		}
		usleep(config.exec_delay * 1000);
	}
}

void add_memory_to_criterion(int memory_id, ConsistencyTypes type) {
	int * did = malloc(sizeof(int));
	(*did) = memory_id;
	//log_info(logger, "ADDING %d TO %s", *did, consistency_to_char(type));
	switch(type) {
		case EVENTUAL_CONSISTENCY:
			list_add(CriterionEventual.memories, did);
			break;
		case STRONG_CONSISTENCY:
			list_add(CriterionStrong.memories, did);
			break;
		case STRONG_HASH_CONSISTENCY:
			list_add(CriterionHash.memories, did);
			journal_to_memories_list(CriterionHash.memories);
			break;
	}
}

MemPoolData * getMemoryData(int id) {
	_Bool find_by_id(MemPoolData * this_mem) {
		return this_mem->memory_id == id;
	}
	return list_find(gossiping_list, find_by_id);
}

void run_knl(char * filepath) {
	LQLScript * lql = malloc(sizeof(LQLScript));
	lql->state = NEW;
	lql->lqlid = lql_max_id++;
	lql->lql_name = strdup(filepath);
	lql->instruction_counter = 0;
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
			custom_print("CREATE %s %s %d %d\n", i->table_name, consistency_to_char(i->c_type), i->partitions, i->compaction_time);
			break;
		case SELECT:
			custom_print("SELECT %s %d\n", i->table_name, i->key);
			break;
		case INSERT:
			custom_print("INSERT %s %d %s %ul\n", i->table_name, i->key, i->value, i->timestamp);
			break;
		case DESCRIBE:
			custom_print("DESCRIBE %s\n", i->table_name);
			break;
		case DROP:
			custom_print("DROP %s\n", i->table_name);
			break;
	}
}

_Bool exec_lql_line(LQLScript * lql) {
	Instruction * i = parse_lql_line(lql);
	lql->instruction_counter++;

	_Bool op_return = false;
	if(i->i_type == UNKNOWN_OP_TYPE)
		return false;
	print_op_debug(i);

	lock_mutex(&criterions_mutex);
	switch(i->i_type) {
		case CREATE:
			op_return = create_knl(i->table_name, i->c_type, i->partitions, i->compaction_time);
			break;
		case SELECT:
			op_return = select_knl(i->table_name, i->key);
			break;
		case INSERT:
			op_return = insert_knl(i->table_name, i->key, i->value, i->timestamp);
			break;
		case DESCRIBE:
			//refresh_metadata(true);
			custom_print("mockup de describe\n");
			op_return = true;
			break;
		case DROP:
			op_return = drop_knl(i->table_name);
			break;
	}
	unlock_mutex(&criterions_mutex);

	return op_return;
}

void journal_to_memories_list(t_list * memories) {
	int i, memsocket;
	for(i=0 ; i<memories->elements_count ; i++) {
		int * did = list_get(memories, i);
		MemPoolData * m = getMemoryData(*did);
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
		lock_mutex(&tables_mutex);
		list_clean(tables_dict);
		list_destroy(tables_dict);
		tables_dict = list_create();
		custom_print("Sending describe to memory");
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

			if(print_x_screen) {
				custom_print("   %s %d", table->table_name, table->consistency);
			}

			//TABLE
			list_add(tables_dict, table);
		}
		unlock_mutex(&tables_mutex);
		close(memsocket);
	}
}

void metadata_refresh_loop() {
	inform_thread_id("metadata_refresh");
	while(1) {
		refresh_metadata(false);
		usleep(config.metadata_refresh * 1000);
	}
}

//Gossiping
void inform_gossiping_pool() {
	//log_info(logger, "GOSSIPING LIST START");
	custom_print("GOSSIPING LIST\n");

	int a;
	for(a = 0 ; a < gossiping_list->elements_count ; a++) {
		MemPoolData * this_mem = list_get(gossiping_list, a);
		log_info(logger, "      %d %s:%d", this_mem->memory_id, this_mem->ip, this_mem->port);
		custom_print("\t%d %s:%d\n", this_mem->memory_id, this_mem->ip, this_mem->port);
	}

	custom_print("GOSSIPING END\n");
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

		char * ip = config.a_memory_ip;
		int  port = config.a_memory_port;
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

		inform_gossiping_pool();

		usleep(config.metadata_refresh * 1000);
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
			custom_print("Indique el nombre de la Tabla");
			return;
		}else if (parametro2[0] == '\0'){
			custom_print("Indique la key");
			return;
		}else {
			select_knl(parametro1, atoi(parametro2));
		}
	//INSERT
	}else if (strcmp(comandoPrincipal,"insert")==0){
		if(parametro1[0] == '\0'){
			custom_print("Indique nombre de Tabla");
			return;
		}else if (parametro2[0] == '\0'){
			custom_print("Indique la key");
			return;
		}else if (parametro3[0] == '\0'){
			custom_print("Indique el valor");
			return;
		}else if (parametro4[0] == '\0'){
			insert_knl(parametro1, atoi(parametro2), parametro3, unix_epoch());
		}else {
			insert_knl(parametro1, atoi(parametro2), parametro3, strtoul(parametro4,NULL,10));
		}
	//CREATE
	}else if (strcmp(comandoPrincipal,"create")==0){
		if(parametro1[0] == '\0'){
			custom_print("Indique el nombre de la Tabla");
			return;
		}else if (parametro2[0] == '\0'){
			custom_print("Indique el tipo de consistencia");
			return;
		}else if (parametro3[0] == '\0'){
			custom_print("Indique la cantidad de particiones");
			return;
		}else if (parametro4[0] == '\0'){
			custom_print("Indique el tiempo de compactacion");
			return;
		}else {
			create_knl(parametro1, char_to_consistency(parametro2), atoi(parametro3), atoi(parametro4));
		}
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
				custom_print("La tabla %s no existe", parametro1);
			} else {
				custom_print("Tabla %s Consistencia %d", result->table_name, result->consistency);
			}
		}
	//DROP
	}else if (strcmp(comandoPrincipal,"drop")==0){
		if(parametro1[0] == '\0'){
			custom_print("Indique el nombre de la Tabla");
		}else {
			drop_knl(parametro1);
		}
	}else if (strcmp(comandoPrincipal,"add")==0){
		int memid;
		if(strcmp(parametro1, "memory") != 0) {
			custom_print("Wrong syntax. ADD MEMORY");
		} else {
			memid = atoi(parametro2);

			if(strcmp(parametro3, "to") != 0) {
				//ERROR
				custom_print("Wrong syntax. ADD MEMORY # TO");
			} else {
				ConsistencyTypes consistency = char_to_consistency(parametro4);
				if(consistency == C_UNKNOWN) {
					custom_print("Wrong syntax. ADD MEMORY # TO [EC/SC/SHC]");
				} else {
					add_memory_to_criterion(memid, consistency);
				}
			}
		}
	}else if (strcmp(comandoPrincipal,"journal")==0){
		journal_to_criterions_memories();
		custom_print("Journal Realizado");
	} else if (strcmp(comandoPrincipal,"run")==0){
		if(parametro1[0] == '\0'){
			custom_print("Indique el LQL a ejecutar");
		}else {
			run_knl(parametro1);
		}
	} else if (strcmp(comandoPrincipal,"metrics")==0){
		perform_metrics();
	} else if (strcmp(comandoPrincipal,"info")==0){
		//info();
	}else if (strcmp(comandoPrincipal,"find")==0){
		if(parametro1[0] == '\0'){
			custom_print("Indique el numero de memoria");
		}else {
			getMemoryData(atoi(parametro1));
		}
	}
}

MemtableTableReg * find_table(char * name) {
	lock_mutex(&tables_mutex);
	//custom_print("\n\nBST\n");
	_Bool is_selected(MemtableTableReg * t) {
		//custom_print("  %s vs %s\n", t->table_name, name);
		return strcasecmp(t->table_name, name) == 0;
	}
	MemtableTableReg * r = list_find(tables_dict, is_selected);
	//custom_print("EST\n\n");
	unlock_mutex(&tables_mutex);
	return r;
}

MemPoolData * select_memory_by_table(MemtableTableReg * table, int key) {
	return select_memory_by_consistency(table->consistency, key);
}

MemPoolData * select_memory_by_consistency(ConsistencyTypes type, int key) {
	//TODO
	int * si = null;
	switch(type) {
	case STRONG_CONSISTENCY:
		if(CriterionStrong.memories->elements_count == 0)
			return null;
		si = list_get(CriterionStrong.memories, 0);
		break;
	case EVENTUAL_CONSISTENCY:
		//log_info(logger, "trying %d %d", CriterionEventual.rr_next_to_use, CriterionEventual.memories->elements_count);
		if(CriterionEventual.rr_next_to_use >= CriterionEventual.memories->elements_count) {
			CriterionEventual.rr_next_to_use = 0;
			if(CriterionEventual.memories->elements_count != 0) {
				CriterionEventual.rr_next_to_use++;
				si = list_get(CriterionEventual.memories, 0);
			}
		} else {
			CriterionEventual.rr_next_to_use++;
			si = list_get(CriterionEventual.memories, (CriterionEventual.rr_next_to_use-1));
		}
		break;
	case STRONG_HASH_CONSISTENCY:
		if(CriterionHash.memories->elements_count == 0)
			return null;
		if(key == -1) {
			si = list_get(CriterionHash.memories, 0);
		} else {
			int selected_index = key % CriterionHash.memories->elements_count;
			si = list_get(CriterionHash.memories, selected_index);
		}
		break;
	}
	//log_info(logger, "USING SID %d", (*si));
	return getMemoryData(*si);
	return null;
}



//API
_Bool insert_knl(char * table_name, int key, char * value, unsigned long timestamp){
	int a;
	for(a=0 ; a<strlen(table_name) ; a++) {
		if(table_name[a] == ' ') {
			table_name[a] = '\0';
		}
	}
	MemtableTableReg * tReg = find_table(table_name);

	if(tReg == null) {
		custom_print("\tLa tabla no existe\n");
		//log_info(logger, "Tabla no existe");
		return false;
	}

	MemPoolData * selected_memory = null;
	while(selected_memory == null) {
		selected_memory = select_memory_by_table(tReg, -1);
		//log_error(logger, "No available memory for table");
	}

	int memsocket;

	if ((memsocket = create_socket()) == -1) {
		memsocket = -1;
	}
	if (memsocket != -1 && (connect_socket(memsocket, selected_memory->ip, selected_memory->port)) == -1) {
		memsocket = -1;
	}
	if(memsocket == -1) {
		//log_error(logger, "Memory was unreachable");
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
			//log_info(logger, "MEM ANSWERED SUCCESFULLY");
			custom_print("\tInsert realizado\n");
			exit_value = true;
		} else {
			//log_info(logger, "INSERT ERROR");
			custom_print("\tInsert fallo\n");
			exit_value = false;
		}

		close(memsocket);

		unsigned long op_end = unix_epoch();
		unsigned long op_length = op_end - op_start;
		ConsistencyCriterion * criterion = getCriterionPointer(tReg);
		criterion->write_acum_count++;
		criterion->write_acum_times += op_length;

		return exit_value;
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

			close(memsocket);

			log_info(metrics_logger, "MEM[%d] Memory Load = %f",
							memory->memory_id, ((ro+wo) / to));
		}
	}

	//log_info(metrics_logger, "END_METRICS_LOGGER_%ul", CriterionHash.metrics_start_measure);
}

void metrics_thread() {
	while(1) {
		/*custom_print("\nstart metr\n");
		perform_metrics();
		restart_criterion_stats();
		custom_print("\nend metr\n");*/
		sleep(30);
	}
}

_Bool select_knl(char * table_name, int key){
	MemtableTableReg * tReg = find_table(table_name);
	if(tReg == null) {
		//log_info(logger, "Tabla no existe");
		custom_print("\tLa tabla no existe\n");
		return false;
	}

	MemPoolData * selected_memory = select_memory_by_table(tReg, -1);
	if(selected_memory == null) {
		//log_error(logger, "No available memory for table");
		custom_print("\tNo hay memorias para realizar la operacion\n");
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
		//log_error(logger, "Memory was unreachable");
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
			//log_info(logger, "MEM ANSWERED SUCCESFULLY");
			//log_info(logger, "MEM ENVÍA RESULTADO DE SELECT SOLICITADO");
			int result_len;
			recv(memsocket, &result_len, sizeof(int), 0);
			char* value = malloc(sizeof(char) * result_len);
			recv(memsocket, value, result_len * sizeof(char), 0);

			custom_print("\tEl valor es %s\n", value);
			//log_info(logger, "  EL VALOR RECIBIDO ES %s", value);
			exit_value = true;
		} else {
			//log_info(logger, "SELECT ERROR");
			custom_print("\tValor desconocido\n");
			exit_value = true;
		}

		close(memsocket);

		unsigned long op_end = unix_epoch();
		unsigned long op_length = op_end - op_start;
		ConsistencyCriterion * criterion = getCriterionPointer(tReg);
		criterion->read_acum_count++;
		criterion->read_acum_times += op_length;

		return exit_value;
	}
	return false;
}

_Bool drop_knl(char * table_name){
	MemtableTableReg * tReg = find_table(table_name);
	if(tReg == null) {
		custom_print("Tabla no existe %s", table_name);
		return false;
	}

	MemPoolData * selected_memory = null;
	while(selected_memory == null) {
		selected_memory = select_memory_by_table(tReg, -1);
		//log_error(logger, "No available memory for table");
	}

	int memsocket;

	if ((memsocket = create_socket()) == -1) {
		memsocket = -1;
	}
	if (memsocket != -1 && (connect_socket(memsocket, selected_memory->ip, selected_memory->port)) == -1) {
		memsocket = -1;
	}
	if(memsocket == -1) {
		//log_error(logger, "Memory was unreachable");
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
			custom_print("\tDrop realizado\n");
			//log_info(logger, "MEM ANSWERED SUCCESFULLY");
			//log_info(logger, "DROP EN EL FILESYSTEM");
			exit_value = true;
		} else {
			custom_print("\tDrop fallo\n");
			//log_info(logger, "DROP ERROR");
			exit_value = false;
		}
		close(memsocket);

		return exit_value;
	}
	return false;
}

_Bool create_knl(char * table_name, ConsistencyTypes consistency, int partitions, int compaction_time){
	MemPoolData * selected_memory = select_memory_by_consistency(consistency, -1);
	if(selected_memory == null) {
		//log_error(logger, "No available memory for table");
		custom_print("\tCreate fallo, falta memoria\n");
		return false;
	}

	//log_info(logger, "SELECTED %d %s %d", selected_memory->memory_id, selected_memory->ip, selected_memory->port);

	int memsocket;

	if ((memsocket = create_socket()) == -1) {
		memsocket = -1;
	}
	if (memsocket != -1 && (connect_socket(memsocket, selected_memory->ip, selected_memory->port)) == -1) {
		memsocket = -1;
	}
	if(memsocket == -1) {
		//log_error(logger, "Memory was unreachable");
		return false;
	} else {
		int exit_value;
		send_data(memsocket, KNL_MEM_CREATE, 0, null);

		int table_name_len = strlen(table_name)+1;
		send(memsocket, &table_name_len,  sizeof(int), 0);
		send(memsocket, table_name,       table_name_len * sizeof(char),0);

		send(memsocket, &consistency, sizeof(int), 0);
		send(memsocket, &partitions, sizeof(int), 0);
		send(memsocket, &compaction_time, sizeof(int), 0);

		MemtableTableReg * table = malloc(sizeof(MemtableTableReg));
		table->table_name = table_name;
		table->partitions = partitions;
		table->compaction_time = compaction_time;
		table->consistency = consistency;

		list_add(tables_dict, table);

		MessageHeader * header = malloc(sizeof(MessageHeader));
		recieve_header(memsocket, header);
		if(header->type == OPERATION_SUCCESS) {
			custom_print("\tCreate realizado\n");
			//log_info(logger, "MEM ANSWERED SUCCESFULLY");
			exit_value = true;
		} else {
			//log_info(logger, "CREATE ERROR");
			custom_print("\tCreate fallo\n");
			exit_value = false;
		}
		close(memsocket);
		return exit_value;
	}
	return false;
}
