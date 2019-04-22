#ifndef PLAN_CONSOLE_H_
#define PLAN_CONSOLE_H_

#include "commands.h"

// comando_t tiene un string comando y una lista de hasta 5 strings que son parametros
typedef struct {
	char comando[20];
	char parametro[5][20];
} comando_t;

void * lfs_console_launcher();
void execute(comando_t*);
void string_tolower(char*);
void cargar_comando(comando_t*, char*);

#endif
