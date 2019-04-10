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
char * generate_table_basedir(char * table_name);

t_list * search_key_in_partitions(char * table_name, int key);

char * select_fs(char * table_name, int key);

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

	//describe_fs("table4");
	//create_fs("mytable", STRONG_HASH_CONSISTENCY, 4, 6540);
	//drop_fs("mytable");

	//log_info(logger, "%s", select_fs("mytable", 4));

	//insert_fs("mytable", 4, "hola", 4568ul);
	//insert_fs("mytable", 8, "chau", 9987ul);
	//insert_fs("FUTBOL", 166, "unvalor", 119873ul);
	insert_fs("mytable", 4, "slu2", 9933ul);
	dump_memtable();

	compact("mytable");

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

char * generate_tables_basedir() {
	char * tables_basedir = malloc( strlen(config.mounting_point) + 22 );
	strcpy(tables_basedir, config.mounting_point);
	strcat(tables_basedir, "Tables/");
	return tables_basedir;
}

char * generate_table_basedir(char * table_name) {
	table_name = to_upper_string(table_name);
	char * tables_base = generate_tables_basedir();
	char * table_basedir = malloc(sizeof(char) * (strlen(tables_base) + 1 + strlen(table_name)));
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
					strcat(full_path, "/Metadata.bin");

					t_config * config = config_create(full_path);

					MemtableTableReg * reg = malloc(sizeof(MemtableTableReg));
					reg->records = null;
					reg->table_name = malloc(sizeof(char) * strlen(dir->d_name));
					strcpy(reg->table_name, dir->d_name);
					reg->compaction_time = config_get_int_value(config, "COMPACTION_TIME");
					reg->partitions = config_get_int_value(config, "PARTITIONS");
					reg->consistency = char_to_consistency(config_get_string_value(config, "CONSISTENCY"));
					reg->dumping_queue = list_create();
					reg->temp_c = list_create();

					free(config);

					inform_table_metadata(reg);
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
	if(blocks_q != 0) {
		fprintf(strfile, "%d]", list_get(blocks, a));
	} else {
		fprintf(strfile, "]");
	}
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
	file_content[0] = '\0';

	for(a=0 ; a<blocks_q ; a++) {
		char * this_block_path = malloc( strlen(config.mounting_point) + 22 );
		strcpy(this_block_path, config.mounting_point);
		strcat(this_block_path, "Bloques/");
		strcat(this_block_path, blocks[a]);
		strcat(this_block_path, ".bin");

		char * this_block_content = malloc(config.value_size + 1);

		FILE * block = fopen(this_block_path, "r");
		fread(this_block_content, config.value_size, 1, block);
		this_block_content[fsconfig.block_size] = '\0';

		strcat(file_content, this_block_content);

		fclose(block);

		free(this_block_path);
	}

	free(file_config_path);

	return file_content;
}

void remove_file(char * file_path) {
	char * file_config_path = malloc(sizeof(char) * (strlen(config.mounting_point) + 22));
	strcpy(file_config_path, config.mounting_point);
	strcat(file_config_path, file_path);

	FILE * auxfile = fopen(file_config_path, "r");
	if(auxfile == null) {
		log_error(logger, "File not found in filesystem");
		return;
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
	file_content[0] = '\0';

	for(a=0 ; a<blocks_q ; a++) {
		char * this_block_path = malloc( strlen(config.mounting_point) + 22 );
		strcpy(this_block_path, config.mounting_point);
		strcat(this_block_path, "Bloques/");
		strcat(this_block_path, blocks[a]);
		strcat(this_block_path, ".bin");

		bitarray_clean_bit(fsconfig.bitarray, atoi(blocks[a]));

		free(this_block_path);
	}

	char * remove_command = malloc(sizeof(char) * (strlen(file_config_path) + 3));
	strcpy(remove_command, "rm ");
	strcat(remove_command, file_config_path);

	system(remove_command);

	free(remove_command);

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
	if(table == null) {
		char * metadatafilepath = generate_table_metadata_path(table_name);
		FILE * t = fopen(metadatafilepath, "r");
		if(t == null) {
			return -1;
		}
		fclose(t);
		t_config * config = config_create(metadatafilepath);
		free(metadatafilepath);

		int partitions = config_get_int_value(config, "PARTITIONS"), a;
		return (key % partitions) + 1;
	}
	return (key % table->partitions) + 1;
}

int insert_fs(char * table_name, int key, char * value, unsigned long timestamp) {
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

int create_fs(char * table_name, ConsistencyTypes consistency, int partitions, int compaction_time) {
	table_name = to_upper_string(table_name);
	MemtableTableReg * tablereg = table_exists(table_name);
	if(tablereg != null) {
		log_error(logger, "The table already exists %s", table_name);
		return -1;
	}

	log_info(logger, "Table %s creation", table_name);

	char * tables_basedir = generate_tables_basedir();
	char * commandbuffer = malloc(sizeof(char) * (6 + strlen(tables_basedir) + strlen(table_name)));
	sprintf(commandbuffer, "mkdir %s%s", tables_basedir, table_name);

	system(commandbuffer);
	free(commandbuffer);

	char * table_basedir = generate_table_basedir(table_name);

	char * metadatafilepath = generate_table_metadata_path(table_name);

	FILE * metadatafile = fopen(metadatafilepath, "w");

	fprintf(metadatafile, "CONSISTENCY=%s\nPARTITIONS=%d\nCOMPACTION_TIME=%d",
			consistency_to_char(consistency), partitions, compaction_time);

	fclose(metadatafile);
	free(metadatafilepath);

	int a;
	for(a=1 ; a<=partitions ; a++) {
		char * thispath = malloc(sizeof(char) * (strlen(generate_table_basedir(table_name)) + 3 + 4));
		char * tnum = malloc(sizeof(char) * 3);
		strcpy(thispath, generate_table_basedir(table_name));
		sprintf(tnum, "%d", a);
		strcat(thispath, tnum);
		strcat(thispath, ".bin");

		FILE * partition = fopen(thispath, "w");
		fprintf(partition, "SIZE=0\nBLOCKS=[]");
		fclose(partition);
		free(thispath);
		free(tnum);
	}

	tablereg = malloc(sizeof(MemtableTableReg));
	tablereg->table_name = table_name;
	tablereg->compaction_time = compaction_time;
	tablereg->consistency = consistency;
	tablereg->partitions = partitions;
	tablereg->records = null;
	tablereg->dumping_queue = list_create();
	tablereg->temp_c = list_create();

	list_add(memtable, tablereg);

	log_info(logger, "Table created %s", table_name);
}

t_list * search_key_in_memtable(char * table_name, int key) {
	t_list * results = list_create();

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
	t_list * results = list_create();
	table_name = to_upper_string(table_name);

	int partition = get_key_partition(table_name, key);

	char * partition_file = malloc(sizeof(char) * (strlen(table_name) + 15));
	char * tnum = malloc(sizeof(char) * 15);
	strcpy(partition_file, "Tables/");
	sprintf(tnum, "%s/%d.bin", table_name, partition);
	strcat(partition_file, tnum);

	char * partition_content = get_file_contents(partition_file);

	char * token = strtok(partition_content, "\n");
	while (token != NULL) {
		int t_key;
		char * value = malloc(config.value_size);
		unsigned long timestamp;

		sscanf(token, "%ul;%d;%s", &timestamp, &t_key, value);

		if(key == t_key) {
			MemtableKeyReg * reg = malloc(sizeof(MemtableKeyReg));
			reg->key = t_key;
			reg->timestamp = timestamp;
			reg->value = value;

			list_add(results, reg);
		} else {
			free(value);
		}

		token = strtok(NULL, "\n");
	}

	free(tnum);
	free(partition_file);

	return results;
}

t_list * search_key_in_temp_files(char * table_name, int key) {
	t_list * results = list_create();

	return results;
}

char * select_fs(char * table_name, int key) {
	t_list * hits = list_create();
	list_add_all(hits, search_key_in_memtable(table_name, key));
	list_add_all(hits, search_key_in_partitions(table_name, key));
	list_add_all(hits, search_key_in_temp_files(table_name, key));

	if(hits->elements_count == 0) {
		//no hay registros con esa key
		return "UNK";
	}

	MemtableKeyReg * final_hit = list_get(hits, 0);
	int a;
	for(a=0 ; a<hits->elements_count ; a++) {
		MemtableKeyReg * reg = list_get(hits, a);

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
						strcat(full_path, "/Metadata.bin");

						t_config * config = config_create(full_path);

						MemtableTableReg * reg = malloc(sizeof(MemtableTableReg));
						reg->records = null;
						reg->table_name = malloc(sizeof(char) * strlen(dir->d_name));
						strcpy(reg->table_name, dir->d_name);
						reg->compaction_time = config_get_int_value(config, "COMPACTION_TIME");
						reg->partitions = config_get_int_value(config, "PARTITIONS");
						reg->consistency = char_to_consistency(config_get_string_value(config, "CONSISTENCY"));

						inform_table_metadata(reg);

						free(config);
						free(reg->table_name);
						free(reg);
					}
				}
			}
			closedir(d);
		}
	} else {
		table_name = to_upper_string(table_name);
		char * metadatafilepath = generate_table_metadata_path(table_name);
		FILE * t = fopen(metadatafilepath, "r");
		if(t == null) {
			log_info(logger, "Table doesnt exists");
			return;
		}
		fclose(t);

		t_config * config = config_create(metadatafilepath);

		MemtableTableReg * reg = malloc(sizeof(MemtableTableReg));
		reg->records = null;
		reg->table_name = malloc(sizeof(char) * strlen(table_name));
		strcpy(reg->table_name, table_name);
		reg->compaction_time = config_get_int_value(config, "COMPACTION_TIME");
		reg->partitions = config_get_int_value(config, "PARTITIONS");
		reg->consistency = char_to_consistency(config_get_string_value(config, "CONSISTENCY"));

		inform_table_metadata(reg);

		free(config);
		free(reg->table_name);
		free(reg);
		free(metadatafilepath);
	}
}

void drop_fs(char * table_name) {
	table_name = to_upper_string(table_name);
	int index = -1, a = 0;

	void search(MemtableTableReg * table) {
		if(strcmp(table->table_name, table_name) == 0) {
			index = a;
		} else {
			a++;
		}
	}
	list_iterate(memtable, search);

	if(index != -1) {
		MemtableTableReg * reg = list_get(memtable, index);
		list_remove(memtable, index);
		free(reg);
	}
	char * metadatafilepath = generate_table_metadata_path(table_name);
	FILE * t = fopen(metadatafilepath, "r");
	if(t == null) {
		return;
	}
	fclose(t);
	t_config * config = config_create(metadatafilepath);
	free(metadatafilepath);

	int partitions = config_get_int_value(config, "PARTITIONS");
	for(a=1 ; a<=partitions ; a++) {
		char * thiscommand = malloc(sizeof(char) * (7 + strlen(table_name) + 5 + 3));
		char * tnum = malloc(sizeof(char) * 3);
		strcpy(thiscommand, "Tables/");
		strcat(thiscommand, table_name);
		sprintf(tnum, "/%d", a);
		strcat(thiscommand, tnum);
		strcat(thiscommand, ".bin");

		log_info(logger, "remove file %s", thiscommand);

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
	int a, b, c, d, counter;

	for(a=0 ; a<memtable->elements_count ; a++) {
		MemtableTableReg * table = list_get(memtable, a);

		int * dump_multiplier = malloc(sizeof(int));
		int dump_found = 0;

		(*dump_multiplier) = 0;

		while(dump_found == 0) {
			dump_found = 1;
			(*dump_multiplier)++;

			for(d=0 ; d<table->dumping_queue->elements_count ; d++) {
				int * v = list_get(table->dumping_queue, d);
				if((*dump_multiplier) == (*v)) {
					dump_found = 0;
				}
			}
		}

		log_info(logger, "TABLE %s", table->table_name);

		if(table->records == null){
			table->records = list_create();
		}

		char * temp_content;
		if(table->records->elements_count == 0) {
			temp_content = malloc(sizeof(char));
		} else {
			temp_content = malloc( table->records->elements_count * (456) );
		}
		temp_content[0] = '\0';
		counter = 0;

		for(c=0 ; c<table->records->elements_count ; c++) {
			MemtableKeyReg * reg = list_get(table->records, c);

			char * thisline = malloc(config.value_size + sizeof(int) + (12 + 2) * sizeof(int));
			thisline[0] = '\0';
			if(counter != 0) {
				strcat(temp_content, "\n");
			}
			sprintf(thisline, "%ul;%d;%s", reg->timestamp, reg->key, reg->value);
			strcat(temp_content, thisline);

			counter++;
		}

		char * pfilepath = malloc(sizeof(char) * (12 + strlen(table->table_name) + 3));
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

	int a, b;
	for(a=0 ; a<table->dumping_queue->elements_count ; a++) {
		int * dumping_multiplier = list_get(table->dumping_queue, a);
		list_add(table->temp_c, dumping_multiplier);

		char * command = malloc(sizeof(char) * (13 + 2 * (strlen(generate_table_basedir(table_name)) + 3)));
		sprintf(command, "mv %s%d.tmp %s%d.tmpc", generate_table_basedir(table_name), (*dumping_multiplier),
				generate_table_basedir(table_name), (*dumping_multiplier));

		system(command);
		free(command);
	}

	list_clean(table->dumping_queue);

	for(a=0 ; a<table->temp_c->elements_count ; a++) {
		int * file_number = list_get(table->temp_c, a);

		char * temp_file_path = malloc(sizeof(char) * (13 + 3 + strlen(table_name)));
		sprintf(temp_file_path, "Tables/%s/%d.tmpc", table_name, (*file_number));

		char * file_content = get_file_contents(temp_file_path);
		char * buffer_temp = malloc(strlen(file_content) * sizeof(char));

		char * token = strtok_r(file_content, "\n", &buffer_temp);
		if(token != null) {
			do {
				int t_key;
				char * value = malloc(config.value_size);
				unsigned long timestamp;

				sscanf(token, "%ul;%d;%s", &timestamp, &t_key, value);

				MemtableKeyReg * reg = malloc(sizeof(MemtableKeyReg));
				reg->key = t_key;
				reg->timestamp = timestamp;
				reg->value = value;

				t_list * existent_values = search_key_in_partitions(table_name, reg->key);

				log_info(logger, "compact %ul %d %s", timestamp, reg->key, value);

				char * partition_file = malloc(sizeof(char) * (strlen(table_name) + 15));
				char * tnum = malloc(sizeof(char) * 15);
				strcpy(partition_file, "Tables/");
				sprintf(tnum, "%s/%d.bin", table_name, get_key_partition(table_name, reg->key));
				strcat(partition_file, tnum);

				log_info(logger, "   partition %s", tnum);

				char * partition_content = get_file_contents(partition_file);
				char * buffer_ptr = malloc(sizeof(char) * strlen(partition_content));
				char * new_partition_content;
				if(strlen(partition_content) == 0) {
					new_partition_content = malloc(sizeof(char) *  255);
				} else {
					new_partition_content = malloc(sizeof(char) *  2 * strlen(partition_content));
				}
				new_partition_content[0] = '\0';
				int counter_partition = 0;

				log_info(logger, "      leyendo particion");

				char * token_partition = strtok_r(partition_content, "\n", &buffer_ptr);
				if(token_partition != null) {
					do {
						int key_p;
						char * value_p = malloc(config.value_size);
						unsigned long timestamp_p;
						char * this_line;

						sscanf(token_partition, "%ul;%d;%s", &timestamp_p, &key_p, value_p);

						MemtableKeyReg * reg_p = malloc(sizeof(MemtableKeyReg));
						reg_p->key = key_p;
						reg_p->timestamp = timestamp_p;
						reg_p->value = value_p;

						log_info(logger, "         %ul %d %s", reg_p->timestamp, reg_p->key, reg_p->value);

						if(reg->key == reg_p->key) {
							if(reg->timestamp > reg_p->timestamp) {
								if(counter_partition != 0) {
									strcat(new_partition_content, "\n");
								}
								this_line = malloc(sizeof(char) * (22 + config.value_size));
								sprintf(this_line, "%ul;%d;%s", reg->timestamp, reg->key, reg->value);
								strcat(new_partition_content, this_line);
								counter_partition++;
							} else {
								if(counter_partition != 0) {
									strcat(new_partition_content, "\n");
								}
								this_line = malloc(sizeof(char) * (22 + config.value_size));
								sprintf(this_line, "%ul;%d;%s", reg_p->timestamp, reg_p->key, reg_p->value);
								strcat(new_partition_content, this_line);
								counter_partition++;
							}
						} else {
							if(counter_partition != 0) {
								strcat(new_partition_content, "\n");
							}
							this_line = malloc(sizeof(char) * (22 + config.value_size));
							sprintf(this_line, "%ul;%d;%s", reg_p->timestamp, reg_p->key, reg_p->value);
							strcat(new_partition_content, this_line);
							counter_partition++;
						}

						log_info(logger, "           compone %s", this_line);

						char * token_partition = strtok_r(partition_content, "\n", &buffer_ptr);
					} while (token_partition != NULL && strcmp(buffer_ptr, "") != 0);
				} else {
					//Empty partition file
					char * this_line = malloc(sizeof(char) * (22 + config.value_size));
					sprintf(this_line, "%ul;%d;%s", reg->timestamp, reg->key, reg->value);
					strcat(new_partition_content, this_line);
				}
				//TODO Save file

				free(tnum);
				free(partition_file);

				token = strtok_r(file_content, "\n", &buffer_temp);
			} while (token != NULL && strcmp(buffer_temp, "") != 0);
		} else {
			//Empty tempfile
		}
	}

	list_clean(table->temp_c);
}
