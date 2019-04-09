#include <dalibrary/libmain.h>

t_log * logger;
t_config * config_file = null;
LFSConfig config;
LFSMetadata fsconfig;

void up_filesystem();
void free_block_list(int * blocks, int blocks_q);
t_list * reserve_blocks(int blocks_q);
LFSFileStruct * save_file_contents(char * content, char * file_path);

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

	up_filesystem();

	save_file_contents("hola", "archivo.bin");

	return EXIT_SUCCESS;
}

void up_filesystem() {
	fsconfig.metadatapath = malloc(sizeof(char) * (strlen(config.mounting_point) + 22));

	strcpy(fsconfig.metadatapath, config.mounting_point);
	strcat(fsconfig.metadatapath, "Metadata/Metadata.bin");
	t_config * fs_config = config_create(fsconfig.metadatapath);

	fsconfig.block_size = config_get_int_value(fs_config, "BLOCK_SIZE");
	fsconfig.blocks = config_get_int_value(fs_config, "BLOCKS");
	fsconfig.magic_number = config_get_string_value(fs_config, "MAGIC_NUMBER");

	fsconfig.bitarraypath = malloc(sizeof(char) * (strlen(config.mounting_point) + 22));
	strcpy(fsconfig.bitarraypath, config.mounting_point);
	strcat(fsconfig.bitarraypath, "Metadata/Bitmap.bin");

	char * ba = malloc(sizeof(char) * (fsconfig.blocks / 8));
	FILE * b = fopen(fsconfig.bitarraypath, "r");

	int a;
	if(b == null) {
		fsconfig.bitarray = bitarray_create_with_mode(ba, fsconfig.blocks / 8, MSB_FIRST);

		for(a=0 ; a<fsconfig.blocks ; a++) {
			bitarray_clean_bit(fsconfig.bitarray, a);
		}

		b = fopen(fsconfig.bitarraypath, "w");
		fwrite(fsconfig.bitarray->bitarray, 1, fsconfig.blocks / 8, b);
		fclose(b);

		free(ba);
		ba = malloc(sizeof(char) * (fsconfig.blocks / 8));
	}
	b = fopen(fsconfig.bitarraypath, "r");

	fread(ba, 1, fsconfig.blocks / 8, b);
	fclose(b);

	fsconfig.bitarray = bitarray_create_with_mode(ba, fsconfig.blocks / 8, MSB_FIRST);

	dump_bitmap();
}

void free_block_list(int * blocks, int blocks_q) {
	int a;
	for(a=0 ; a<blocks_q ; a++) {
		bitarray_clean_bit(fsconfig.bitarray, blocks[a]);
	}
}

t_list * reserve_blocks(int blocks_q) {
	int availables = 0;
	int a, b;
	for(a=0 ; a<fsconfig.bitarray->size * 8 ; a++) {
		if(bitarray_test_bit(fsconfig.bitarray, a) == 0) {
			availables++;
			if(availables == blocks_q) a = fsconfig.bitarray->size * 8;
		}
	}
	if(availables < blocks_q) {
		return null;
	}
	t_list * blocks_numbers = list_create();
	b = 0;
	for(a=0 ; a<blocks_q ; a++) {
		while(bitarray_test_bit(fsconfig.bitarray, b) == 1) {
			b++;
		}
		list_add(blocks_numbers, b);
		bitarray_set_bit(fsconfig.bitarray, b);
	}
	return blocks_numbers;
}

void save_bitmap_to_fs() {
	FILE * b = fopen(fsconfig.bitarraypath, "w");
	fwrite(fsconfig.bitarray->bitarray, 1, fsconfig.blocks / 8, b);
	fclose(b);
}

void dump_bitmap() {
	int a;
	for(a=0 ; a<fsconfig.blocks ; a++) {
		log_info(logger, "   %d %d", a, bitarray_test_bit(fsconfig.bitarray, a));
	}
}

LFSFileStruct * save_file_contents(char * content, char * file_path) {
	LFSFileStruct file_struct;

	float content_size = strlen(content) + 0.0;
	float blocks_q_f = content_size / fsconfig.block_size;
	int blocks_q = content_size / fsconfig.block_size, a;
	float r = blocks_q_f - blocks_q;
	if(r != 0.0) {
		blocks_q++;
	}

	char * file_config_path = malloc(sizeof(char) * (strlen(config.mounting_point) + 22));
	strcpy(file_config_path, config.mounting_point);
	strcat(file_config_path, file_path);

	t_list * blocks = reserve_blocks(blocks_q);
	if(blocks == null) {
		log_error(logger, "Not enough free space");
		return null;
	}

	for(a=0 ; a<blocks->elements_count ; a++) {
		char * this_file_stream = malloc(fsconfig.block_size + 1);
		memcpy(this_file_stream, (content + (a*fsconfig.block_size)), fsconfig.block_size);
		this_file_stream[fsconfig.block_size] = '\0';

		char blockn[10];
		sprintf(blockn, "%d.bin", list_get(blocks, a));

		char * this_block_path = malloc( strlen(config.mounting_point) + 22 );
		strcpy(this_block_path, config.mounting_point);
		strcat(this_block_path, "Bloques/");
		strcat(this_block_path, blockn);

		FILE * tf = fopen(this_block_path, "w");
		fwrite(this_file_stream, fsconfig.block_size, 1, tf);
		fclose(tf);

		free(this_block_path);
		free(this_file_stream);
	}

	save_bitmap_to_fs();

	file_struct.blocks = blocks;
	file_struct.size = blocks->elements_count * fsconfig.block_size;

	FILE * strfile = fopen(file_config_path, "w");
	fprintf(strfile, "SIZE=%d\n", file_struct.size);
	fprintf(strfile, "BLOCKS=[", file_struct.size);
	for(a=0 ; a<blocks->elements_count-1 ; a++) {
		fprintf(strfile, "%d,", list_get(blocks, a));
	}
	fprintf(strfile, "%d]", list_get(blocks, a));
	fclose(strfile);

	free(file_config_path);

	return &file_struct;
}
