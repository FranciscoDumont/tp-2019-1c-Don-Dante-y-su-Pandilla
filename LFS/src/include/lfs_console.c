#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <readline/readline.h>
#include <readline/history.h>
#include "lfs_console.h"


void * lfs_console_launcher() {

	comando_t comando;

	char *linea;
	int quit = 0;

	printf("Bienvenido/a a la consola del Lisandra File System\n");
	printf("Escribi 'info' para obtener una lista de comandos\n");

	while(quit == 0){

		linea = readline("> ");
		string_tolower(linea);
		add_history(linea);
		cargar_comando(&comando,linea);

		if((strcmp(comando.comando,"exit")==0)){
			break;
		}

		execute(&comando);

		free(linea);
	}
	return EXIT_SUCCESS;
}

void execute(comando_t* unComando){
	char comandoPrincipal[20];
	char parametro1[20];
	char parametro2[20];
	char parametro3[20];
	char parametro4[20];
	char parametro5[20];

	strcpy(comandoPrincipal,unComando->comando);
	strcpy(parametro1,unComando->parametro[0]);
	strcpy(parametro2,unComando->parametro[1]);
	strcpy(parametro3,unComando->parametro[2]);
	strcpy(parametro4,unComando->parametro[3]);
	strcpy(parametro5,unComando->parametro[4]);
	

	if(strcmp(comandoPrincipal,"select")==0){
		select_fs(parametro1,parametro2);
	}else if (strcmp(comandoPrincipal,"insert")==0){
		insert_fs(parametro1,parametro2,parametro3,parametro4);
	}else if (strcmp(comandoPrincipal,"create")==0){
		//create_fs(parametro1,parametro2,parametro3,parametro4);
	}else if (strcmp(comandoPrincipal,"describe")==0){
		describe_fs(parametro1);
	}else if (strcmp(comandoPrincipal,"drop")==0){
		drop_fs(parametro1);
	}else if (strcmp(comandoPrincipal,"info")==0){
		info();
	}

}


void string_tolower(char* string){
	for(int i = 0; string[i]; i++)
		string[i] = tolower(string[i]);
}

void cargar_comando(comando_t* unComando, char* linea){

	char delim[] = " ";
	int indice = 0;

	char *ptr = strtok(linea, delim);

	strcpy(unComando->comando, ptr);
	ptr = strtok(NULL, delim);

	while(ptr != NULL && indice < 5)
	{
		strcpy(unComando->parametro[indice], ptr);
		ptr = strtok(NULL, delim);
		indice++;
	}

/*
	printf("Un comando: '%s'\n",unComando->comando);
	printf("Parametro 1'%s'\n",unComando->parametro[0]);
	printf("Parametro 2'%s'\n",unComando->parametro[1]);
	printf("Parametro 3'%s'\n",unComando->parametro[2]);
	printf("Parametro 4'%s'\n",unComando->parametro[3]);
*/

}
