#include <dalibrary/libmain.h>

t_log * logger;
t_config * config_file = null;
LFSConfig config;
LFSMetadata fsconfig;

t_list * memtable;

char * to_upper_string(char * string);

char * generate_tables_basedir();
char * generate_table_basedir(char * table_name);
char * generate_table_metadata_path(char * table_name);

void up_filesystem();

void free_block_list(int * blocks, int blocks_q);
t_list * reserve_blocks(int blocks_q);
void save_bitmap_to_fs();
void dump_bitmap();

LFSFileStruct * save_file_contents(char * content, char * file_path);
char * get_file_contents(char * file_path);
void remove_file(char * file_path);

MemtableTableReg * table_exists(char * table_name);
int get_key_partition(char * table_name, int key);

int insert_fs(char * table_name, int key, char * value, unsigned long timestamp);
int create_fs(char * table_name, ConsistencyTypes consistency, int partitions, int compaction_time);
t_list * search_key_in_memtable(char * table_name, int key);
t_list * search_key_in_partitions(char * table_name, int key);
t_list * search_key_in_temp_files(char * table_name, int key);
char * select_fs(char * table_name, int key);
void inform_table_metadata(MemtableTableReg * reg);
void describe_fs(char * table_name);
void drop_fs(char * table_name);
void dump_memtable();
void compact(char * table_name);

int main(int argc, char **argv) {
	if (argc != 2) {
		config_file = config_create("lfs01.cfg");
	} else {
		config_file = config_create(argv[1]);
	}

	config.port				= config_get_int_value(config_file, "PUERTO_ESCUCHA");
	config.mounting_point	= config_get_string_value(config_file, "PUNTO_MONTAJE");
	config.delay			= config_get_int_value(config_file, "RETARDO");
	config.value_size		= config_get_int_value(config_file, "TAMAÑO_VALUE");
	config.dump_delay		= config_get_int_value(config_file, "TIEMPO_DUMP");

	logger = log_create("filesystem_logger.log", "LFS", true, LOG_LEVEL_TRACE);

	memtable = list_create();

	up_filesystem();

	return EXIT_SUCCESS;
}

char * to_upper_string(char * string) {
	int a;
	char * temp = malloc(sizeof(char) * (strlen(string) + 1));
	for(a=0 ; a<strlen(string) ; a++) {
		temp[a] = toupper(string[a]);
	}
	temp[a] = '\0';
	return temp;
}

char * generate_tables_basedir() {
	char * tables_basedir = malloc( (strlen(config.mounting_point) + 8) * sizeof(char) );
	strcpy(tables_basedir, config.mounting_point);
	strcat(tables_basedir, "Tables/");
	return tables_basedir;
}

char * generate_table_basedir(char * table_name) {
	table_name = to_upper_string(table_name);

	char * tables_base = generate_tables_basedir();
	char * table_basedir = malloc(sizeof(char) * (strlen(tables_base) + 2 + strlen(table_name)));
	strcpy(table_basedir, tables_base);
	strcat(table_basedir, table_name);
	strcat(table_basedir, "/");
	return table_basedir;
}

char * generate_table_metadata_path(char * table_name) {
	char * table_basedir = generate_table_basedir(table_name);
	char * metadatafilepath = malloc(sizeof(char) * (strlen(table_basedir) + 13));
	strcpy(metadatafilepath, table_basedir);
	strcat(metadatafilepath, "Metadata.bin");
	return metadatafilepath;
}

void up_filesystem() {
	fsconfig.metadatapath = malloc(sizeof(char) * (strlen(config.mounting_point) + 22));

	strcpy(fsconfig.metadatapath, config.mounting_point);
	strcat(fsconfig.metadatapath, "Metadata/Metadata.bin");
	t_config * fs_config	= config_create(fsconfig.metadatapath);

	fsconfig.block_size		= config_get_int_value(fs_config, "BLOCK_SIZE");
	fsconfig.blocks			= config_get_int_value(fs_config, "BLOCKS");
	fsconfig.magic_number	= config_get_string_value(fs_config, "MAGIC_NUMBER");

	fsconfig.bitarraypath	= malloc(sizeof(char) * (strlen(config.mounting_point) + 20));
	strcpy(fsconfig.bitarraypath, config.mounting_point);
	strcat(fsconfig.bitarraypath, "Metadata/Bitmap.bin");

	char * bitarray_ptr       = malloc(sizeof(char) * (fsconfig.blocks / 8));
	FILE * bitarray_file_ptr  = fopen(fsconfig.bitarraypath, "r");

	if(bitarray_file_ptr == null) {
		fsconfig.bitarray = bitarray_create_with_mode(bitarray_ptr, fsconfig.blocks / 8, MSB_FIRST);

		int block_counter;
		for(block_counter=0 ; block_counter<fsconfig.blocks ; block_counter++) {
			bitarray_clean_bit(fsconfig.bitarray, block_counter);
		}

		bitarray_file_ptr = fopen(fsconfig.bitarraypath, "w");
		fwrite(fsconfig.bitarray->bitarray, 1, fsconfig.blocks / 8, bitarray_file_ptr);
		fclose(bitarray_file_ptr);

		bitarray_ptr = malloc(sizeof(char) * (fsconfig.blocks / 8));
	}
	bitarray_file_ptr = fopen(fsconfig.bitarraypath, "r");

	fread(bitarray_ptr, 1, fsconfig.blocks / 8, bitarray_file_ptr);
	fclose(bitarray_file_ptr);

	fsconfig.bitarray = bitarray_create_with_mode(bitarray_ptr, fsconfig.blocks / 8, MSB_FIRST);

	DIR * tables_directory;
	struct dirent * tables_dirent;
	char * tables_basedir = generate_tables_basedir();
	tables_directory = opendir(tables_basedir);
	char full_folder_path[1024];
	if (tables_directory) {
		while ((tables_dirent = readdir(tables_directory)) != NULL) {
			if(tables_dirent->d_type == DT_DIR) {
				if(tables_dirent->d_name[0] != '.') {
					full_folder_path[0] = '\0';

					strcat(full_folder_path, tables_basedir);
					strcat(full_folder_path, tables_dirent->d_name);
					strcat(full_folder_path, "/Metadata.bin");

					t_config * config = config_create(full_folder_path);

					MemtableTableReg * reg = malloc(sizeof(MemtableTableReg));
					reg->records			= null;
					reg->table_name			= malloc(sizeof(char) * strlen(tables_dirent->d_name));
					strcpy(reg->table_name, tables_dirent->d_name);

					reg->compaction_time	= config_get_int_value(config, "COMPACTION_TIME");
					reg->partitions			= config_get_int_value(config, "PARTITIONS");
					reg->consistency		= char_to_consistency(config_get_string_value(config, "CONSISTENCY"));
					reg->dumping_queue		= list_create();
					reg->temp_c				= list_create();

					inform_table_metadata(reg);
					list_add(memtable, reg);

					config_destroy(config);
				}
			}
		}
		closedir(tables_directory);
	}
	free(tables_dirent);

	log_info(logger, "Filesystem Up");
}

void free_block_list(int * blocks, int blocks_q) {
	int counter;
	for(counter=0 ; counter<blocks_q ; counter++) {
		bitarray_clean_bit(fsconfig.bitarray, blocks[counter]);
	}
}

t_list * reserve_blocks(int blocks_q) {
	int availables = 0;
	int counter_1, counter_2;
	for(counter_1=0 ; counter_1<fsconfig.bitarray->size * 8 ; counter_1++) {
		if(bitarray_test_bit(fsconfig.bitarray, counter_1) == 0) {
			availables++;
			if(availables == blocks_q) counter_1 = fsconfig.bitarray->size * 8;
		}
	}

	if(availables < blocks_q) {
		return null;
	}

	t_list * blocks_numbers = list_create();
	counter_2 = 0;

	for(counter_1=0 ; counter_1<blocks_q ; counter_1++) {
		while(bitarray_test_bit(fsconfig.bitarray, counter_2) == 1) {
			counter_2++;
		}
		int * this_block_index = malloc(sizeof(int));
		(*this_block_index) = counter_2;
		list_add(blocks_numbers, this_block_index);
		bitarray_set_bit(fsconfig.bitarray, counter_2);
	}

	return blocks_numbers;
}

void save_bitmap_to_fs() {
	FILE * bitarray_ptr = fopen(fsconfig.bitarraypath, "w");
	fwrite(fsconfig.bitarray->bitarray, 1, fsconfig.blocks / 8, bitarray_ptr);
	fclose(bitarray_ptr);
}

void dump_bitmap() {
	int counter;
	for(counter=0 ; counter<fsconfig.blocks ; counter++) {
		log_info(logger, "   %d %d", counter, bitarray_test_bit(fsconfig.bitarray, counter));
	}
}

LFSFileStruct * save_file_contents(char * content, char * file_path) {
	LFSFileStruct * file_struct = malloc(sizeof(LFSFileStruct));

	float content_size	= strlen(content) + 0.0;
	float blocks_q_f	= content_size / fsconfig.block_size;
	int blocks_q		= content_size / fsconfig.block_size, block_counter;
	float remaining		= blocks_q_f - blocks_q;

	if(remaining != 0.0) {
		blocks_q++;
	}

	char * file_config_path = malloc(sizeof(char) * (strlen(config.mounting_point) + strlen(file_path) + 1));
	strcpy(file_config_path, config.mounting_point);
	strcat(file_config_path, file_path);

	t_list * blocks = reserve_blocks(blocks_q);
	if(blocks == null) {
		log_error(logger, "Not enough free space");
		return null;
	}

	for(block_counter=0 ; block_counter<blocks->elements_count ; block_counter++) {
		char * this_file_stream = malloc(fsconfig.block_size + sizeof(char));
		memcpy(this_file_stream, (content + (block_counter*fsconfig.block_size)), fsconfig.block_size);
		this_file_stream[fsconfig.block_size] = '\0';

		char block_number_str[10];
		int * block_number_int = list_get(blocks, block_counter);
		sprintf(block_number_str, "%d.bin", (*block_number_int));

		char * this_block_path = malloc( strlen(config.mounting_point) + 8 + 1 + strlen(block_number_str) );
		strcpy(this_block_path, config.mounting_point);
		strcat(this_block_path, "Bloques/");
		strcat(this_block_path, block_number_str);

		FILE * this_block_ptr = fopen(this_block_path, "w");
		fwrite(this_file_stream, fsconfig.block_size, 1, this_block_ptr);
		fclose(this_block_ptr);

		free(this_block_path);
		free(this_file_stream);
	}

	save_bitmap_to_fs();

	file_struct->blocks	= blocks;
	file_struct->size	= blocks->elements_count * fsconfig.block_size;

	FILE * file_ptr = fopen(file_config_path, "w");

	fprintf(file_ptr, "SIZE=%d\n", file_struct->size);
	fprintf(file_ptr, "BLOCKS=[");
	for(block_counter=0 ; block_counter<blocks->elements_count-1 ; block_counter++) {
		int * block_number_int = list_get(blocks, block_counter);
		fprintf(file_ptr, "%d,", (*block_number_int));
	}
	if(blocks_q != 0) {
		int * block_number_int = list_get(blocks, block_counter);
		fprintf(file_ptr, "%d]", (*block_number_int));
	} else {
		fprintf(file_ptr, "]");
	}
	fclose(file_ptr);
	free(file_config_path);

	return file_struct;
}

char * get_file_contents(char * file_path) {
	char * file_config_path = malloc(sizeof(char) * (strlen(config.mounting_point) + strlen(file_path) + 1));
	strcpy(file_config_path, config.mounting_point);
	strcat(file_config_path, file_path);

	FILE * aux_file = fopen(file_config_path, "r");
	if(aux_file == null) {
		log_error(logger, "File not found in filesystem");
		return null;
	}
	fclose(aux_file);

	t_config * file_data	= config_create(file_config_path);
	char ** blocks			= config_get_array_value(file_data, "BLOCKS");
	char * blocks_str		= config_get_string_value(file_data, "BLOCKS");
	int blocks_q = 0, counter;

	if(strcmp("[]", blocks_str) != 0) {
		blocks_q++;
		for(counter = 0 ; counter < strlen(blocks_str) ; counter++) {
			if(blocks_str[counter] == ',') blocks_q++;
		}
	}
	free(blocks_str);

	char * file_content		= malloc(config_get_int_value(file_data, "SIZE"));
	file_content[0]			= '\0';

	for(counter=0 ; counter<blocks_q ; counter++) {
		char * this_block_path = malloc(sizeof(char) * (strlen(config.mounting_point) + 13 + strlen(blocks[counter])));
		strcpy(this_block_path, config.mounting_point);
		strcat(this_block_path, "Bloques/");
		strcat(this_block_path, blocks[counter]);
		strcat(this_block_path, ".bin");

		char * this_block_content = malloc(config.value_size + sizeof(char));

		FILE * block = fopen(this_block_path, "r");
		fread(this_block_content, config.value_size, 1, block);
		this_block_content[fsconfig.block_size] = '\0';

		strcat(file_content, this_block_content);

		fclose(block);
		free(this_block_path);
		free(this_block_content);
		free(blocks[counter]);
	}
	free(blocks);

	free(file_config_path);

	return file_content;
}

void remove_file(char * file_path) {
	char * file_config_path = malloc(sizeof(char) * (strlen(config.mounting_point) + 1 + strlen(file_path)));
	strcpy(file_config_path, config.mounting_point);
	strcat(file_config_path, file_path);

	FILE * auxfile = fopen(file_config_path, "r");
	if(auxfile == null) {
		log_error(logger, "File not found in filesystem");
		return;
	}
	fclose(auxfile);

	t_config * file_data	= config_create(file_config_path);
	char ** blocks			= config_get_array_value(file_data, "BLOCKS");
	char * blocks_str		= config_get_string_value(file_data, "BLOCKS");
	int blocks_q = 0, block_counter;

	if(strcmp("[]", blocks_str) != 0) {
		blocks_q++;
		for(block_counter = 0 ; block_counter < strlen(blocks_str) ; block_counter++) {
			if(blocks_str[block_counter] == ',') blocks_q++;
		}
	}
	free(blocks_str);

	char * file_content		= malloc(config_get_int_value(file_data, "SIZE"));
	file_content[0]			= '\0';

	for(block_counter=0 ; block_counter<blocks_q ; block_counter++) {
		char * this_block_path = malloc(sizeof(char) * (strlen(config.mounting_point) + 13 + strlen(blocks[block_counter])));
		strcpy(this_block_path, config.mounting_point);
		strcat(this_block_path, "Bloques/");
		strcat(this_block_path, blocks[block_counter]);
		strcat(this_block_path, ".bin");

		bitarray_clean_bit(fsconfig.bitarray, atoi(blocks[block_counter]));

		free(this_block_path);
		free(blocks[block_counter]);
	}
	free(blocks);

	char * remove_command = malloc(sizeof(char) * (strlen(file_config_path) + 4));
	strcpy(remove_command, "rm ");
	strcat(remove_command, file_config_path);

	system(remove_command);

	free(remove_command);
	free(file_config_path);
	free(file_data);

	save_bitmap_to_fs();
}

//API

MemtableTableReg * table_exists(char * table_name) {
	table_name = to_upper_string(table_name);
	int search(MemtableTableReg * table) {
		return strcmp(table->table_name, table_name) == 0;
	}
	return list_find(memtable, search);
}

int get_key_partition(char * table_name, int key) {
	MemtableTableReg * table = table_exists(table_name);
	int this_table_partitions;
	if(table == null) {
		char * metadatafilepath = generate_table_metadata_path(table_name);
		FILE * metadata_ptr = fopen(metadatafilepath, "r");
		if(metadata_ptr == null) {
			return -1;
		}
		fclose(metadata_ptr);

		t_config * config = config_create(metadatafilepath);
		free(metadatafilepath);

		int partitions = config_get_int_value(config, "PARTITIONS");
		free(config);
		this_table_partitions = partitions;
	} else {
		this_table_partitions = table->partitions;
	}
	return (key % this_table_partitions) + 1;
}

int insert_fs(char * table_name, int key, char * value, unsigned long timestamp) {
	MemtableTableReg * tablereg = table_exists(table_name);
	if(tablereg == null) {
		log_error(logger, "Non existent table");
		return false;
	}

	if(tablereg->records == null) {
		tablereg->records = list_create();
	}

	MemtableKeyReg * registry	= malloc(sizeof(MemtableKeyReg));
	registry->key				= key;
	registry->timestamp			= timestamp;
	registry->value				= malloc(config.value_size);
	strcpy(registry->value, value);

	list_add(tablereg->records, registry);

	return true;
}

int create_fs(char * table_name, ConsistencyTypes consistency, int partitions, int compaction_time) {
	MemtableTableReg * table_reg = table_exists(table_name);
	if(table_reg != null) {
		log_error(logger, "The table already exists %s", table_reg->table_name);
		return false;
	}
	table_name = to_upper_string(table_name);

	char * tables_basedir	= generate_tables_basedir();
	char * commandbuffer	= malloc(sizeof(char) * (7 + strlen(tables_basedir) + strlen(table_name)));
	sprintf(commandbuffer, "mkdir %s%s", tables_basedir, table_name);

	system(commandbuffer);

	free(commandbuffer);
	free(tables_basedir);

	char * metadatafilepath = generate_table_metadata_path(table_name);
	FILE * metadatafile = fopen(metadatafilepath, "w");

	fprintf(metadatafile, "CONSISTENCY=%s\nPARTITIONS=%d\nCOMPACTION_TIME=%d",
			consistency_to_char(consistency), partitions, compaction_time);

	fclose(metadatafile);
	free(metadatafilepath);

	int partition_counter;
	for(partition_counter=1 ; partition_counter<=partitions ; partition_counter++) {
		char * partition_number_str = malloc(sizeof(char) * 3);
		sprintf(partition_number_str, "%d", partition_counter);

		char * thispath = malloc(sizeof(char) * (strlen(generate_table_basedir(table_name)) + strlen(partition_number_str) + 4));
		strcpy(thispath, generate_table_basedir(table_name));
		strcat(thispath, partition_number_str);
		strcat(thispath, ".bin");

		FILE * partition = fopen(thispath, "w");
		fprintf(partition, "SIZE=0\nBLOCKS=[]");
		fclose(partition);

		free(partition_number_str);
	}

	table_reg					= malloc(sizeof(MemtableTableReg));
	table_reg->table_name		= table_name;
	table_reg->compaction_time	= compaction_time;
	table_reg->consistency		= consistency;
	table_reg->partitions		= partitions;
	table_reg->records			= null;
	table_reg->dumping_queue	= list_create();
	table_reg->temp_c			= list_create();

	list_add(memtable, table_reg);

	log_info(logger, "Table created %s", table_name);

	return true;
}

t_list * search_key_in_memtable(char * table_name, int key) {
	t_list * results		 = list_create();
	MemtableTableReg * table = table_exists(table_name);
	if(table == null) {
		return results;
	}
	if(table->records == null) {
		return results;
	}

	void analyze(MemtableKeyReg * reg) {
		if(reg->key == key) {
			list_add(results, reg);
		}
	}
	list_iterate(table->records, analyze);

	return results;
}

t_list * search_key_in_partitions(char * table_name, int key) {
	t_list * results	= list_create();
	table_name			= to_upper_string(table_name);

	int partition		= get_key_partition(table_name, key);

	char * partition_file		= malloc(sizeof(char) * (strlen(table_name) + 15));
	strcpy(partition_file, "Tables/");
	char * partition_number_str	= malloc(sizeof(char) * 15);
	sprintf(partition_number_str, "%s/%d.bin", table_name, partition);
	strcat(partition_file, partition_number_str);

	char * partition_content = get_file_contents(partition_file);
	char * partition_buffer  = malloc(sizeof(char) * strlen(partition_content));
	partition_buffer[0] = '\0';

	char * token;
	for (token = strtok_r(partition_content, "\n", &partition_buffer) ;
			token != NULL ;
			token = strtok_r(partition_buffer, "\n", &partition_buffer)) {
		int		t_key;
		char *	value = malloc(config.value_size);
		unsigned long timestamp;

		sscanf(token, "%ul;%d;%s", &timestamp, &t_key, value);

		if(key == t_key) {
			MemtableKeyReg * reg	= malloc(sizeof(MemtableKeyReg));
			reg->key				= t_key;
			reg->timestamp			= timestamp;
			reg->value				= value;

			list_add(results, reg);
		} else {
			free(value);
		}
	}

	free(partition_number_str);
	free(partition_file);
	free(partition_content);

	return results;
}

t_list * search_key_in_temp_files(char * table_name, int key) {
	t_list * results = list_create();
	int file_counter;

	table_name					= to_upper_string(table_name);
	MemtableTableReg * table	= table_exists(table_name);

	if(table == null) {
		return results;
	}

	for(file_counter=0 ; file_counter<table->dumping_queue->elements_count ; file_counter++) {
		int * file_number		= list_get(table->dumping_queue, file_counter);

		char * temp_file_path	= malloc(sizeof(char) * (13 + 3 + strlen(table_name)));
		sprintf(temp_file_path, "Tables/%s/%d.tmp", table_name, (*file_number));

		char * content			= get_file_contents(temp_file_path);
		char * buffer			= malloc(sizeof(char) * strlen(content));

		char * token;
		for (token = strtok_r(content, "\n", &buffer) ;
					token != NULL ;
					token = strtok_r(buffer, "\n", &buffer)) {
			int t_key;
			char * value = malloc(config.value_size);
			unsigned long timestamp;

			sscanf(token, "%ul;%d;%s", &timestamp, &t_key, value);

			if(key == t_key) {
				MemtableKeyReg * reg = malloc(sizeof(MemtableKeyReg));
				reg->key			 = t_key;
				reg->timestamp 		 = timestamp;
				reg->value			 = value;

				list_add(results, reg);
			} else {
				free(value);
			}
		}

		free(temp_file_path);
		free(content);
	}

	for(file_counter=0 ; file_counter<table->temp_c->elements_count ; file_counter++) {
		int * file_number 		= list_get(table->temp_c, file_counter);

		char * temp_file_path 	= malloc(sizeof(char) * (13 + 3 + strlen(table_name)));
		sprintf(temp_file_path, "Tables/%s/%d.tmpc", table_name, (*file_number));

		char * content			= get_file_contents(temp_file_path);
		char * buffer			= malloc(sizeof(char) * strlen(content));

		char * token;
		for (token = strtok_r(content, "\n", &buffer) ;
					token != NULL ;
					token = strtok_r(buffer, "\n", &buffer)) {
			int t_key;
			char * value = malloc(config.value_size);
			unsigned long timestamp;

			sscanf(token, "%ul;%d;%s", &timestamp, &t_key, value);

			if(key == t_key) {
				MemtableKeyReg * reg = malloc(sizeof(MemtableKeyReg));
				reg->key			 = t_key;
				reg->timestamp		 = timestamp;
				reg->value			 = value;

				list_add(results, reg);
			} else {
				free(value);
			}
		}

		free(temp_file_path);
		free(content);
	}

	return results;
}

char * select_fs(char * table_name, int key) {
	t_list * hits = list_create();
	list_add_all(hits, search_key_in_memtable  (table_name, key));
	list_add_all(hits, search_key_in_partitions(table_name, key));
	list_add_all(hits, search_key_in_temp_files(table_name, key));

	if(hits->elements_count == 0) {
		//no hay registros con esa key
		return "UNK";
	}

	MemtableKeyReg * final_hit = list_get(hits, 0);
	int reg_iterator;
	for(reg_iterator=0 ; reg_iterator<hits->elements_count ; reg_iterator++) {
		MemtableKeyReg * reg = list_get(hits, reg_iterator);

		//log_info(logger, "   %ul %d %s", reg->timestamp, reg->key, reg->value);

		if(reg->timestamp > final_hit->timestamp) {
			final_hit = reg;
		}
	}

	return final_hit->value;
}

void inform_table_metadata(MemtableTableReg * reg) {
	log_info(logger, "TABLE %s COMPACTION %d PARTITIONS %d CONSISTENCY %s",
		reg->table_name, reg->compaction_time, reg->partitions, consistency_to_char(reg->consistency));
}

void describe_fs(char * table_name) { //table_name puede ser nulo
	if(table_name == null) {
		DIR * tables_directory;
		struct dirent * tables_dirent;
		char * tables_basedir = generate_tables_basedir();
		tables_directory = opendir(tables_basedir);
		char full_path[1000];
		if (tables_directory) {
			while ((tables_dirent = readdir(tables_directory)) != NULL) {
				if(tables_dirent->d_type == DT_DIR) {
					if(tables_dirent->d_name[0] != '.') {
						full_path[0] = '\0';
						strcat(full_path, tables_basedir);
						strcat(full_path, tables_dirent->d_name);
						strcat(full_path, "/Metadata.bin");

						t_config * config = config_create(full_path);

						MemtableTableReg * reg	= malloc(sizeof(MemtableTableReg));
						reg->records			= null;
						reg->table_name			= malloc(sizeof(char) * strlen(tables_dirent->d_name));
						strcpy(reg->table_name, tables_dirent->d_name);
						reg->compaction_time	= config_get_int_value(config, "COMPACTION_TIME");
						reg->partitions			= config_get_int_value(config, "PARTITIONS");
						reg->consistency		= char_to_consistency(config_get_string_value(config, "CONSISTENCY"));

						inform_table_metadata(reg);
					}
				}
			}
			closedir(tables_directory);
		}
	} else {
		table_name = to_upper_string(table_name);
		char * metadatafilepath = generate_table_metadata_path(table_name);
		FILE * metadatafile_ptr = fopen(metadatafilepath, "r");
		if(metadatafile_ptr == null) {
			log_info(logger, "Table doesnt exists");
			return;
		}
		fclose(metadatafile_ptr);

		t_config * config = config_create(metadatafilepath);

		MemtableTableReg * reg	= malloc(sizeof(MemtableTableReg));
		reg->records			= null;
		reg->table_name			= malloc(sizeof(char) * strlen(table_name));
		strcpy(reg->table_name, table_name);
		reg->compaction_time	= config_get_int_value(config, "COMPACTION_TIME");
		reg->partitions			= config_get_int_value(config, "PARTITIONS");
		reg->consistency		= char_to_consistency(config_get_string_value(config, "CONSISTENCY"));

		inform_table_metadata(reg);
	}
}

void drop_fs(char * table_name) {
	table_name = to_upper_string(table_name);
	int index_in_memtable = -1, aux_counter = 0;

	void search(MemtableTableReg * table) {
		if(strcmp(table->table_name, table_name) == 0) {
			index_in_memtable = aux_counter;
		} else {
			aux_counter++;
		}
	}
	list_iterate(memtable, search);

	if(index_in_memtable != -1) {
		MemtableTableReg * reg = list_get(memtable, index_in_memtable);
		list_remove(memtable, index_in_memtable);
		free(reg);
	}
	char * metadatafilepath	= generate_table_metadata_path(table_name);
	FILE * metadata_ptr		= fopen(metadatafilepath, "r");
	if(metadata_ptr == null) {
		return;
	}
	fclose(metadata_ptr);

	t_config * config = config_create(metadatafilepath);
	free(metadatafilepath);

	int partitions = config_get_int_value(config, "PARTITIONS");
	for(aux_counter=1 ; aux_counter<=partitions ; aux_counter++) {
		char * thiscommand = malloc(sizeof(char) * (7 + strlen(table_name) + 5 + 3));
		char * tnum = malloc(sizeof(char) * 3);
		strcpy(thiscommand, "Tables/");
		strcat(thiscommand, table_name);
		sprintf(tnum, "/%d", aux_counter);
		strcat(thiscommand, tnum);
		strcat(thiscommand, ".bin");

		remove_file(thiscommand);

		free(tnum);
		free(thiscommand);
	}

	char * remove_command = malloc(sizeof(char) * (strlen(generate_table_basedir(table_name)) + 6));
	strcpy(remove_command, "rm -r ");
	strcat(remove_command, generate_table_basedir(table_name));

	system(remove_command);

	free(remove_command);
}

void dump_memtable() {
	int aux_iterator, records_iterator, multiplier_iterator, counter;

	for(aux_iterator=0 ; aux_iterator<memtable->elements_count ; aux_iterator++) {
		MemtableTableReg * table	= list_get(memtable, aux_iterator);
		int * dump_multiplier		= malloc(sizeof(int));
		int dump_found				= 0;
		(*dump_multiplier)			= 0;

		while(dump_found == 0) {
			dump_found = 1;
			(*dump_multiplier)++;

			for(multiplier_iterator=0 ;
					multiplier_iterator<table->dumping_queue->elements_count ;
					multiplier_iterator++) {
				int * v = list_get(table->dumping_queue, multiplier_iterator);
				if((*dump_multiplier) == (*v)) {
					dump_found = 0;
				}
			}
		}

		if(table->records == null){
			table->records = list_create();
		}

		char * temp_content;
		if(table->records->elements_count == 0) {
			temp_content = malloc(sizeof(char));
		} else {
			temp_content = malloc(table->records->elements_count * (3 + config.value_size + 10));
		}
		temp_content[0] = '\0';
		counter = 0;

		for(records_iterator=0 ;
				records_iterator<table->records->elements_count ;
				records_iterator++) {
			MemtableKeyReg * reg = list_get(table->records, records_iterator);
			char * thisline		= malloc(3 + config.value_size + 10);
			thisline[0]			= '\0';

			if(counter != 0) {
				strcat(temp_content, "\n");
			}
			sprintf(thisline, "%ul;%d;%s", reg->timestamp, reg->key, reg->value);
			strcat(temp_content, thisline);

			counter++;
		}

		char * pfilepath = malloc(sizeof(char) * (12 + strlen(table->table_name) + 3 + 1));
		sprintf(pfilepath, "Tables/%s/%d.tmp", table->table_name, (*dump_multiplier));
		save_file_contents(temp_content, pfilepath);

		list_add(table->dumping_queue, dump_multiplier);

		free(temp_content);
		free(pfilepath);
	}
}

void compact(char * table_name) {
	table_name = to_upper_string(table_name);
	MemtableTableReg * table = table_exists(table_name);
	int tmp_iterator;

	if(table == null) {
		return;
	}

	if(table->dumping_queue->elements_count == 0) {
		log_info(logger, "Table %s has no temp files to compact", table_name);
		return;
	}

	if(table->temp_c == null) {
		table->temp_c = list_create();
	}

	for(tmp_iterator=0 ; tmp_iterator<table->dumping_queue->elements_count ; tmp_iterator++) {
		int * dumping_multiplier = list_get(table->dumping_queue, tmp_iterator);
		list_add(table->temp_c, dumping_multiplier);

		char * command = malloc(sizeof(char) * (13 + 2 * (strlen(generate_table_basedir(table_name)) + 3)));
		sprintf(command, "mv %s%d.tmp %s%d.tmpc", generate_table_basedir(table_name), (*dumping_multiplier),
				generate_table_basedir(table_name), (*dumping_multiplier));

		system(command);
		free(command);
	}

	list_clean(table->dumping_queue);

	for(tmp_iterator=0 ; tmp_iterator<table->temp_c->elements_count ; tmp_iterator++) {
		int * file_number		= list_get(table->temp_c, tmp_iterator);

		char * temp_file_path	= malloc(sizeof(char) * (13 + 3 + strlen(table_name)));
		sprintf(temp_file_path, "Tables/%s/%d.tmpc", table_name, (*file_number));

		char * file_content		= get_file_contents(temp_file_path);
		char * buffer_temp		= malloc(strlen(file_content) * sizeof(char));

		char * token;
		for (token = strtok_r(file_content, "\n", &buffer_temp) ;
					token != NULL ;
					token = strtok_r(buffer_temp, "\n", &buffer_temp)) {
			int t_key;
			char * value = malloc(config.value_size);
			unsigned long timestamp;

			sscanf(token, "%ul;%d;%s", &timestamp, &t_key, value);

			MemtableKeyReg * reg	= malloc(sizeof(MemtableKeyReg));
			reg->key				= t_key;
			reg->timestamp			= timestamp;
			reg->value				= value;

			t_list * existent_values = search_key_in_partitions(table_name, reg->key);

			char * partition_file	= malloc(sizeof(char) * (strlen(table_name) + 15));
			char * tnum				= malloc(sizeof(char) * 15);
			strcpy(partition_file, "Tables/");
			sprintf(tnum, "%s/%d.bin", table_name, get_key_partition(table_name, reg->key));
			strcat(partition_file, tnum);

			char * partition_content	= get_file_contents(partition_file);
			char * buffer_partition		= malloc(sizeof(char) * strlen(partition_content));

			char * new_partition_content;
			if(strlen(partition_content) == 0) {
				new_partition_content = malloc(3 + config.value_size + 10);
			} else {
				new_partition_content = malloc(sizeof(char) *  2 * strlen(partition_content));
			}
			new_partition_content[0] = '\0';

			int counter_partition	 = 0;

			char * token_partition;
			for (token_partition = strtok_r(partition_content, "\n", &buffer_partition) ;
					token_partition != NULL ;
					token_partition = strtok_r(buffer_partition, "\n", &buffer_partition)) {
				int key_p;
				char * value_p = malloc(config.value_size);
				unsigned long timestamp_p;
				char * this_line;

				sscanf(token_partition, "%ul;%d;%s", &timestamp_p, &key_p, value_p);

				MemtableKeyReg * reg_p	= malloc(sizeof(MemtableKeyReg));
				reg_p->key				= key_p;
				reg_p->timestamp		= timestamp_p;
				reg_p->value			= value_p;

				if(reg->key == reg_p->key) {
					if(reg->timestamp > reg_p->timestamp) {
						if(counter_partition != 0) {
							strcat(new_partition_content, "\n");
						}
						this_line = malloc(3 + config.value_size + 10);
						sprintf(this_line, "%ul;%d;%s", reg->timestamp, reg->key, reg->value);
						strcat(new_partition_content, this_line);
						counter_partition++;
					} else {
						if(counter_partition != 0) {
							strcat(new_partition_content, "\n");
						}
						this_line = malloc(3 + config.value_size + 10);
						sprintf(this_line, "%ul;%d;%s", reg_p->timestamp, reg_p->key, reg_p->value);
						strcat(new_partition_content, this_line);
						counter_partition++;
					}
				} else {
					if(counter_partition != 0) {
						strcat(new_partition_content, "\n");
					}
					this_line = malloc(3 + config.value_size + 10);
					sprintf(this_line, "%ul;%d;%s", reg_p->timestamp, reg_p->key, reg_p->value);
					strcat(new_partition_content, this_line);
					counter_partition++;
				}
			}

			if(counter_partition == 0) {
				//Empty partition file
				char * this_line = malloc(3 + config.value_size + 10);
				sprintf(this_line, "%ul;%d;%s", reg->timestamp, reg->key, reg->value);
				strcat(new_partition_content, this_line);
			}

			remove_file(partition_file);

			save_file_contents(new_partition_content, partition_file);

			free(tnum);
			free(partition_file);
		}
	}

	for(tmp_iterator=0 ; tmp_iterator<table->temp_c->elements_count ; tmp_iterator++) {
		int * file_number = list_get(table->temp_c, tmp_iterator);

		char * temp_file_path = malloc(sizeof(char) * (13 + 3 + strlen(table_name)));
		sprintf(temp_file_path, "Tables/%s/%d.tmpc", table_name, (*file_number));

		remove_file(temp_file_path);
	}
	list_clean(table->temp_c);

	log_info(logger, "Compacted table %s", table_name);
}
