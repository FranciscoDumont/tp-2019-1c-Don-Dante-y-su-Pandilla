#include <dalibrary/libmain.h>

t_log * logger;
t_config * config_file = null;
LFSConfig config;
LFSMetadata fsconfig;

t_list * memtable;

void up_filesystem();
void free_block_list(int * blocks, int blocks_q);
t_list * reserve_blocks(int blocks_q);
LFSFileStruct * save_file_contents(char * content, char * file_path);
char * get_file_contents(char * file_path);

char * generate_tables_basedir();

char * to_upper_string(char * sPtr);

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

	memtable = list_create();

	up_filesystem();

	create("table1", STRONG_CONSISTENCY, 2, 6000);
	create("table2", STRONG_HASH_CONSISTENCY, 2, 5000);
	create("table3", EVENTUAL_CONSISTENCY, 2, 16000);
	create("table2", EVENTUAL_CONSISTENCY, 5, 7500);

	return EXIT_SUCCESS;
}

char * to_upper_string(char * sPtr) {
	int a;
	char * temp = malloc(sizeof(char) * (strlen(sPtr) + 1));
	for(a=0 ; a<strlen(sPtr) ; a++) {
		temp[a] = toupper(sPtr[a]);
	}
	temp[a] = '\0';
	return temp;
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

	DIR *d;
	struct dirent *dir;
	char * path = generate_tables_basedir();
	d = opendir(path);
	char full_path[1000];
	if (d) {
		while ((dir = readdir(d)) != NULL) {
			if(dir->d_type == DT_DIR) {
				if(dir->d_name[0] != '.') {
					full_path[0] = '\0';
					strcat(full_path, path);
					strcat(full_path, dir->d_name);

					MemtableTableReg * reg = malloc(sizeof(MemtableTableReg));
					reg->records = null;
					reg->table_name = malloc(sizeof(char) * strlen(dir->d_name));
					strcpy(reg->table_name, dir->d_name);

					list_add(memtable, reg);
				}
			}
		}
		closedir(d);
	}
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

char * get_file_contents(char * file_path) {
	char * file_config_path = malloc(sizeof(char) * (strlen(config.mounting_point) + 22));
	strcpy(file_config_path, config.mounting_point);
	strcat(file_config_path, file_path);

	FILE * auxfile = fopen(file_config_path, "r");
	if(auxfile == null) {
		log_error(logger, "File not found in filesystem");
		return null;
	}
	fclose(auxfile);

	t_config * file_data = config_create(file_config_path);
	char ** blocks = config_get_array_value(file_data, "BLOCKS");
	char * blocks_str = config_get_string_value(file_data, "BLOCKS");
	int blocks_q = 0, a;

	if(strcmp("[]", blocks_str) != 0) {
		blocks_q++;
		for(a = 0 ; a < strlen(blocks_str) ; a++) {
			if(blocks_str[a] == ',') blocks_q++;
		}
	}
	free(blocks_str);

	char * file_content = malloc(config_get_int_value(file_data, "SIZE"));

	for(a=0 ; a<blocks_q ; a++) {
		char * this_block_path = malloc( strlen(config.mounting_point) + 22 );
		strcpy(this_block_path, config.mounting_point);
		strcat(this_block_path, "Bloques/");
		strcat(this_block_path, blocks[a]);
		strcat(this_block_path, ".bin");

		FILE * block = fopen(this_block_path, "r");
		fscanf(block, "%s", file_content + (a*fsconfig.block_size));
		fclose(block);

		free(this_block_path);
	}

	free(file_config_path);

	return file_content;
}

//API

MemtableTableReg * table_exists(char * table_name) {
	table_name = to_upper_string(table_name);
	int search(MemtableTableReg * table) {
		return strcmp(table->table_name, table_name) == 0;
	}
	return list_find(memtable, search);
}

int insert(char * table_name, int key, char * value, unsigned long timestamp) {
	MemtableTableReg * tablereg = table_exists(table_name);
	if(tablereg == null) {
		log_error(logger, "Non existent table");
		return -1;
	}

	if(tablereg->records == null) {
		tablereg->records = list_create();
	}
	MemtableKeyReg * registry = malloc(sizeof(MemtableKeyReg));
	registry->key = key;
	registry->timestamp = timestamp;

	registry->value = malloc(config.value_size);
	strcpy(registry->value, value);

	list_add(tablereg->records, registry);

	return 0;
}

char * generate_tables_basedir() {
	char * tables_basedir = malloc( strlen(config.mounting_point) + 22 );
	strcpy(tables_basedir, config.mounting_point);
	strcat(tables_basedir, "Tables/");
	return tables_basedir;
}

char * generate_table_basedir(char * table_name) {
	char * tables_base = generate_tables_basedir();
	char * table_basedir = malloc(sizeof(char) * (strlen(tables_base) + 1 + strlen(table_name)));
	strcpy(table_basedir, tables_base);
	strcat(table_basedir, table_name);
	strcat(table_basedir, "/");
	return table_basedir;
}

int create(char * table_name, ConsistencyTypes consistency, int partitions, int compaction_time) {
	table_name = to_upper_string(table_name);
	MemtableTableReg * tablereg = table_exists(table_name);
	if(tablereg != null) {
		log_error(logger, "The table already exists %s", table_name);
		return -1;
	}

	char * tables_basedir = generate_tables_basedir();
	char * commandbuffer = malloc(sizeof(char) * (6 + strlen(tables_basedir) + strlen(table_name)));
	sprintf(commandbuffer, "mkdir %s%s", tables_basedir, table_name);

	system(commandbuffer);
	free(commandbuffer);

	char * table_basedir = generate_table_basedir(table_name);

	char * metadatafilepath = malloc(sizeof(char) * (strlen(table_basedir) + 13));
	strcpy(metadatafilepath, table_basedir);
	strcat(metadatafilepath, "Metadata.bin");

	FILE * metadatafile = fopen(metadatafilepath, "w");

	fprintf(metadatafile, "CONSISTENCY=%s\nPARTITIONS=%d\nCOMPACTION_TIME=%d",
			consistency_to_char(consistency), partitions, compaction_time);

	fclose(metadatafile);
	free(metadatafilepath);

	tablereg = malloc(sizeof(MemtableTableReg));
	tablereg->table_name = table_name;
	tablereg->records = null;

	list_add(memtable, tablereg);

	log_info(logger, "Table created %s", table_name);
}
