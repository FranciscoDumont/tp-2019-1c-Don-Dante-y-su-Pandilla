#include <dalibrary/libmain.h>

int main(void) {
	ThreadsList = list_create();
	inform_thread_id("KERNEL");
	return EXIT_SUCCESS;
}
