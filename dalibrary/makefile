RM=rm -rf
CC=gcc

C_SRCS=$(shell find . -iname "*.c" | tr '\n' ' ')
H_SRCS=$(shell find . -iname "*.h" | tr '\n' ' ')
OBJS=$(C_SRCS:./%.c=build/%.o)

# Clean and compile .so
all: build/libdalibrary.so

create-dirs:
	mkdir -p build/dalibrary/

build/libdalibrary.so: create-dirs $(OBJS)
	$(CC) -shared -o "build/libdalibrary.so" $(OBJS)

build/dalibrary/%.o: dalibrary/%.c
	$(CC) -c -fmessage-length=0 -fPIC -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"

# Add debug parameters and compile
debug: CC += -DDEBUG -g
debug: all

# Clean build files
clean:
	$(RM) build

install: all
	cp -u build/libdalibrary.so /usr/lib
	cp --parents -u $(H_SRCS) /usr/include

uninstall:
	rm -f /usr/lib/libdalibrary.so
	rm -rf /usr/include/dalibrary

.PHONY: all create-dirs clean install uninstall