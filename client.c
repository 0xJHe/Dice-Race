#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <signal.h>

#define PORT 8888
#define BUFFER_SIZE 1024

int sockfd;
char player_name[20];

// Continuously listens for game state updates from the server
void* receive_thread(void* arg) {
    char buffer[BUFFER_SIZE];
    
    while (1) {
        int bytes = recv(sockfd, buffer, sizeof(buffer) - 1, 0);
        if (bytes <= 0) {
            printf("\nDisconnected from server. Exiting in 3 seconds...\n");
            sleep(3); 
            kill(getpid(), SIGKILL);
            exit(0);
        }
        
        buffer[bytes] = '\0';
        printf("%s", buffer);
        fflush(stdout);
    }
    return NULL;
}

int main(int argc, char* argv[]) {
    struct sockaddr_in server_addr;
    char buffer[BUFFER_SIZE];
    pthread_t recv_thread;
    
    char server_ip[20] = "127.0.0.1";
    if (argc > 1) {
        strcpy(server_ip, argv[1]);
    }
    
    printf("=== Dice Race Client ===\n");
    printf("Enter your name: ");
    fgets(player_name, sizeof(player_name), stdin);
    player_name[strcspn(player_name, "\n")] = 0;
    
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        perror("Socket creation failed");
        exit(1);
    }
    
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(PORT);
    if (inet_pton(AF_INET, server_ip, &server_addr.sin_addr) <= 0) {
        perror("Invalid address");
        exit(1);
    }
    
    if (connect(sockfd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        perror("Connection failed");
        printf("Make sure the server is running!\n");
        exit(1);
    }
    
    // Initial Handshake
    snprintf(buffer, sizeof(buffer), "NAME:%s", player_name);
    send(sockfd, buffer, strlen(buffer), 0);
    
    // Start background listener
    pthread_create(&recv_thread, NULL, receive_thread, NULL);
    
    printf("\nType 'roll' to play, 'quit' to exit\n");
    printf("> ");
    fflush(stdout);
    
    // Main Input Loop
    while (1) {
        if (fgets(buffer, sizeof(buffer), stdin) == NULL) break;
        
        buffer[strcspn(buffer, "\n")] = 0;
        
        if (strlen(buffer) == 0) continue;
        
        if (strcmp(buffer, "quit") == 0) {
            send(sockfd, "QUIT", 4, 0);
            break;
        }
        else if (strcmp(buffer, "roll") == 0) {
            send(sockfd, "ROLL", 4, 0);
        }
        else if (strcmp(buffer, "refresh") == 0) {
            // Useful helper for polling-style updates
            send(sockfd, "REFRESH", 7, 0);
        }
        else {
            printf("Unknown command.\n");
        }
        
    }
    
    close(sockfd);
    return 0;
}