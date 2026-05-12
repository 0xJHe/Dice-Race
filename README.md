Operating Systems - Project

### 1. How to compile
To compile:
    $ make

To clean up compiled binaries:
    $ make clean

To compile manually:
    # Compile Server
    $ gcc -o server server.c -pthread -lrt

    # Compile Client
    $ gcc -o client client.c

### 2. Example commands
a. Rolling the Dice:
   When it is your turn, type:
   > roll

b. Chatting:
   To send a message to all other players:
   > CHAT: Good game!

c. Quitting:
   To leave the game:
   > QUIT

### 3. Game rules
- The game requires 3 to 5 players. It will not start until at least 3 players are connected.
- Be the first player to reach or exceed Position 30.
- Can only "roll" when the prompt says "YOUR TURN!".

### 4. Supported mode
Supported Mode: Multi-machine Mode (TCP/IP)
The server binds to TCP Port 8888. Clients can connect from the local machine (localhost) 
or remote machines on the same network using the server's IP address.
