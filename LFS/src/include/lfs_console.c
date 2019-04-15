#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <readline/readline.h>
#include <readline/history.h>
#include "lfs_console.h"

void * lfs_console_launcher() {
	char *linea, *param1, *param2;
	int command_number, quit = 0;
	const char* command_list[] = {"select", "insert", "create",
			"describe", "drop", "info"};
	int command_list_length = (int) sizeof(command_list) /
			sizeof(command_list[0]);

	printf("Bienvenido/a a la consola del Lisandra File System\n");
	printf("Escribi 'info' para obtener una lista de comandos\n");

	while(quit == 0){
		linea = readline("> ");
		add_history(linea);
		string_tolower(linea);
		if(parse(&linea, &param1, &param2)) printf("Demasiados parámetros!\n");

		else {
			command_number = find_in_array(linea,
						command_list, command_list_length);

			command_number == EXIT ? quit = 1 : execute(command_number,
					param1, param2);
		}

		free(linea);
	}
	return EXIT_SUCCESS;
}

int parse(char **linea, char **param1, char **param2){
	//separo el input en palabras
	strtok(*linea, " ");
	*param1 = strtok(NULL, " ");
	*param2 = strtok(NULL, " ");
	if(strtok(NULL, " ")) return -1; //en este caso hay mas de 2 parametros
	return 0;
}

void execute(int command_number, char* param1, char* param2){
	switch(command_number){
	case -1:
		printf("Comando no reconocido\n");
		break;
	case SELECT:
		if(!param1 && !param2) select_fs(param1,param2);
		else{printf("El comando XXXX no recibe X parametros\n");}
		break;
	case INSERT:
		if(!param1 && !param2) insert_fs(param1,param2,param1,param2);
		else{printf("El comando XXXX no recibe X parametros\n");}
		break;
	case CREATE:
		if(!param1 && !param2) create_fs();
		else{printf("El comando XXXX no recibe X parametros\n");}
		break;
	case DESCRIBE:
		if(!param1 && !param2) describe_fs();
		else{printf("El comando XXXX no recibe X parametros\n");}
		break;
	case DROP:
		if(!param1 && !param2) drop_fs();
		else{printf("El comando XXXX no recibe X parametros\n");}
		break;
	case INFO:
		if(!param1 && !param2) info();
		else{printf("El comando XXXX no recibe X parametros\n");}
		break;
	}
}


int find_in_array(char* linea, const char** command_list, int length){
	for(int i = 0; i < length; i++)
		if(strcmp(linea, command_list[i]) == 0) return i;
	return -1;
}

void string_tolower(char* string){
	for(int i = 0; string[i]; i++)
		string[i] = tolower(string[i]);
}
