#include <dalibrary/libmain.h>

t_log * logger;
t_config * config_file = null;
KNLConfig config;

t_list * gossiping_list;

void gossiping_start(pthread_t * thread);
void server_start(pthread_t * thread);

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

	pthread_join(thread_g, NULL);

	return EXIT_SUCCESS;
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
