# Compiler
CC = gcc
# flags
CFLAGS = -pthread -Wall

# files
all: server client

# compile server (lrt for shared memory)
server: server.c
	$(CC) $(CFLAGS) -o server server.c -lrt

# compile client
client: client.c
	$(CC) $(CFLAGS) -o client client.c

# clean
clean:
	rm -f server client game.log