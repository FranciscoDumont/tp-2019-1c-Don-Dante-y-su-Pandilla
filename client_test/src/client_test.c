#include <dalibrary/libmain.h>
#include <time.h>

int main(void) {
	NETWORK_DEBUG_LEVEL = NW_ALL_DISPLAY;
	int fd_socket;
	int port = 8080;

	if ((fd_socket = create_socket()) == -1) {
		return EXIT_FAILURE;
	}

	if ((connect_socket(fd_socket, "127.0.0.1", port)) == -1) {
		return EXIT_FAILURE;
	}

	srand(time(NULL));
	int * n = malloc(sizeof(int));
	int sent = 0;

	MessageHeader response;

	send_data(fd_socket, HANDSHAKE_NUMBER_GENERATOR_SERVER, 0, NULL);
	recieve_header(fd_socket, &response);

	if(response.type == HANDSHAKE_RESPONSE) {
		printf("      El servidor respondió el saludo\n");

		while (sent < 10) {
			(*n) = rand();

			send_data(fd_socket, RANDOM_NUMBER, sizeof(int), n);

			sent++;
			sleep(1+((rand()%30)/10));
		}
	} else {
		printf("      El servidor respondió inesperadamente\n");
	}

	return EXIT_SUCCESS;
}
