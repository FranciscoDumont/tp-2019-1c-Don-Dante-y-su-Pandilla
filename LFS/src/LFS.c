#include <dalibrary/libmain.h>

t_log * logger;
t_config * config_file = null;
LFSConfig config;

int main(int argc, char **argv) {
	if (argc != 2) {
		config_file = config_create("lfs01.cfg");
	} else {
		config_file = config_create(argv[1]);
	}

	config.port = config_get_int_value(config_file, "PUERTO_ESCUCHA");
	config.mounting_point = config_get_string_value(config_file, "PUNTO_MONTAJE");
	config.delay = config_get_int_value(config_file, "RETARDO");
	config.value_size = config_get_int_value(config_file, "TAMAÑO_VALUE");
	config.dump_delay = config_get_int_value(config_file, "TIEMPO_DUMP");

	logger = log_create("filesystem_logger.log", "LFS", true, LOG_LEVEL_TRACE);

	return EXIT_SUCCESS;
}
