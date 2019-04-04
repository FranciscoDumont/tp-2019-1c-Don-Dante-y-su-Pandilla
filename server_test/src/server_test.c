#include <dalibrary/libmain.h>

int server_function(int socket);

int main(void) {
	NETWORK_DEBUG_LEVEL = NW_ALL_DISPLAY;
	int fd_socket;
	int port = 8080;

	if((fd_socket = create_socket()) == -1) {
		return EXIT_FAILURE;
	}

	if((bind_socket(fd_socket, port)) == -1) {
		return EXIT_FAILURE;
	}

	pthread_t thread_server;
	pthread_create(&thread_server, NULL, server_function, fd_socket);
	pthread_join(thread_server, NULL);

	close_socket(fd_socket);

	return EXIT_SUCCESS;
}

void new(int fd, char * ip, int port) {
	printf("   alguien se conectó\n");
}

void lost(int fd, char * ip, int port) {
	printf("   alguien se desconectó\n");
}

void incoming(int fd, char * ip, int port, MessageHeader * header) {
	switch(header->type) {
		case HANDSHAKE_NUMBER_GENERATOR_SERVER:
			printf("      Bienvenido, Cliente Generador de Números %d\n", fd);
			send_data(fd, HANDSHAKE_RESPONSE, 0, null);
			break;
		case RANDOM_NUMBER:
			; int n;
			recieve_data(fd, &n, header->data_size);
			printf("      Recibido un número aleatorio: %d\n", n);
			break;
	}
}

int server_function(int socket) {
	start_server(socket, &new, &lost, &incoming);
	return 0;
}
