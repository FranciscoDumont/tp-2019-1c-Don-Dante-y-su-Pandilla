#include "commands.h"

#include <stdio.h>
#include <stdlib.h>

/*Por ahi poner las funciones que estan en LFS.c acá (por ahi no)
void selec(){
	printf("Falta implementacion de select");
};
void insert(){
    printf("Falta implementacion de insert");
}
void create(){
    printf("Falta implementacion de create");
}
void describe(){
    printf("Falta implementacion de describe");
}
void drop(){
    printf("Falta implementacion de drop");
}
*/
void info(){
    printf("SELECT\n La operación Select permite la obtención del valor de una key dentro de una tabla. Para esto, se utiliza la siguiente nomenclatura:\n SELECT [NOMBRE_TABLA] [KEY]\n");

    printf("INSERT\n La operación Insert permite la creación y/o actualización del valor de una key dentro de una tabla. Para esto, se utiliza la siguiente nomenclatura:\n INSERT [NOMBRE_TABLA] [KEY] “[VALUE]” [Timestamp]\n");

    printf("CREATE\n La operación Create permite la creación de una nueva tabla dentro del file system. Para esto, se utiliza la siguiente nomenclatura:\n CREATE [NOMBRE_TABLA] [TIPO_CONSISTENCIA] [NUMERO_PARTICIONES] [COMPACTION_TIME]\n");

    printf("DESCRIBE\n La operación Describe permite obtener la Metadata de una tabla en particular o de todas las tablas que el File System tenga. Para esto, se utiliza la siguiente nomenclatura:\n DESCRIBE [NOMBRE_TABLA]\n");

    printf("DROP\n La operación Drop permite la eliminación de una tabla del file system. Para esto, se utiliza la siguiente nomenclatura:\n DROP [NOMBRE_TABLA]\n");
}
