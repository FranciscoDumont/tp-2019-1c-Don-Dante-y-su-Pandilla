#include <dalibrary/libmain.h>

t_log * logger;
t_config * config_file = null;
KNLConfig config;

int main(int argc, char **argv) {
	if (argc != 2) {
		config_file = config_create("knl01.cfg");
	} else {
		config_file = config_create(argv[1]);
	}

	config.a_memory_ip = config_get_string_value(config_file, "IP_MEMORIA");
	config.a_memory_port = config_get_int_value(config_file, "PUERTO_MEMORIA");
	config.quamtum = config_get_int_value(config_file, "QUANTUM");
	config.multiprocessing_grade = config_get_int_value(config_file, "MULTIPROCESAMIENTO");
	config.metadata_refresh = config_get_int_value(config_file, "METADATA_REFRESH");
	config.exec_delay = config_get_int_value(config_file, "SLEEP_EJECUCION");

	logger = log_create("kernel_logger.log", "KNL", true, LOG_LEVEL_TRACE);

	return EXIT_SUCCESS;
}
