#include <dalibrary/libmain.h>

t_log * logger;
t_config * config_file = null;
MEMConfig config;

void gossiping_start();

int main(int argc, char **argv) {
	if (argc != 2) {
		config_file = config_create("mem01.cfg");
	} else {
		config_file = config_create(argv[1]);
	}

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

	char * memory_logger_path = malloc(sizeof(char) * (25));
	strcpy(memory_logger_path, "memory_logger_");
	strcat(memory_logger_path, config_get_string_value(config_file, "MEMORY_NUMBER"));
	strcat(memory_logger_path, ".log");
	logger = log_create(memory_logger_path, "MEM", true, LOG_LEVEL_TRACE);

	gossiping_start();

	return EXIT_SUCCESS;
}

//Gossiping
void gossiping_thread() {
	while(1) {
		log_info(logger, "GOSSIPING");
		sleep(config.gossiping_time / 1000);
	}
}
void gossiping_start() {
	pthread_t thread_g;
	pthread_create(&thread_g, NULL, gossiping_thread, NULL);
	pthread_join(thread_g, NULL);
}
