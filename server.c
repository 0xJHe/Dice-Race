#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>

#include <sys/mman.h>   
#include <sys/wait.h>   
#include <fcntl.h>   
#include <errno.h>
#include <signal.h>
#include <time.h>

#include <sys/time.h>   
#include <sys/select.h> 
#include <stdarg.h>

// Config
#define MAX_PLAYERS 5
#define BOARD_SIZE 10 // win condition
#define PORT 8888 // tcp port
#define SHM_NAME "/game_shm" // shared memory
#define LOG_QUEUE_SIZE 50 // max log buffer
#define MAX_SCORES 100 // max persistence
#define SCORE_FILE "scores.txt" // scores file
#define LOG_FILE "game.log" // log file

// --Shared Memory
// Logger
typedef struct {
    char msg[256];
} Log;

typedef struct {
    Log buffer[LOG_QUEUE_SIZE];
    int head;
    int tail;
    int count; // count for buffer
    pthread_mutex_t lock; // safe
} LogQueue;

// score struct for scores.txt
typedef struct {
    char name[20];
    int wins;
} Score;

typedef struct {
    Score entries[MAX_SCORES];
    int count;
    pthread_mutex_t lock; // safe
} ScoreBoard;

// Player
typedef struct {
    int id;
    char name[20];
    int position;
    int is_active;
    int wins;                 
    // Child process sends the messages over tcp
    char outbound_msg[512];   
    int has_msg;              
} Player;

// Game State
typedef struct {
    Player players[MAX_PLAYERS];
    int num_players;
    int current_player;
    int game_active;
    int next_turn; // Signal for RR scheduler thread
    int winner_id;
    
    // Sub-systems in Shared Memory
    LogQueue log_queue;
    ScoreBoard scoreboard;
    // Synchronization
    pthread_mutex_t game_lock; // Global game lock
} GameState;

// Global variable
GameState *game; // pointer to Shared Memory
int server_running = 1; // start server

// --logger functions
// push the message to the queue
void log_queue(const char *format, ...) {
    pthread_mutex_lock(&game->log_queue.lock);
    
    if (game->log_queue.count < LOG_QUEUE_SIZE) {
        char buffer[220];
        va_list args;
        va_start(args, format);
        vsnprintf(buffer, sizeof(buffer), format, args);
        va_end(args);

        // Add timestamp
        time_t now = time(NULL);
        char *time_str = ctime(&now);
        time_str[strlen(time_str) - 1] = '\0';
        
        snprintf(game->log_queue.buffer[game->log_queue.tail].msg, 256, "[%s] %s", time_str, buffer);
        
        game->log_queue.tail = (game->log_queue.tail + 1) % LOG_QUEUE_SIZE;
        game->log_queue.count++;
    }
    
    pthread_mutex_unlock(&game->log_queue.lock);
}

// logger thread in parent
void *logger(void *arg) {
    printf("Logger thread started.\n");
    
    while (server_running) {
        char msg_to_write[256] = {0};
        int has_data = 0;

        // Check queue
        pthread_mutex_lock(&game->log_queue.lock);
        if (game->log_queue.count > 0) {
            strncpy(msg_to_write, game->log_queue.buffer[game->log_queue.head].msg, 256);
            game->log_queue.head = (game->log_queue.head + 1) % LOG_QUEUE_SIZE;
            game->log_queue.count--;
            has_data = 1;
        }
        pthread_mutex_unlock(&game->log_queue.lock);

        // Write file (non-blocking)
        if (has_data) {
            FILE *fp = fopen(LOG_FILE, "a");
            if (fp) {
                fprintf(fp, "%s\n", msg_to_write);
                fclose(fp);
                printf("%s\n", msg_to_write); 
            }
        } else {
            usleep(50000); // Sleep 50ms if empty
        }
    }
    return NULL;
}

// --Scores.txt
// Load scores at startup
void load_scores() {
    pthread_mutex_lock(&game->scoreboard.lock);
    game->scoreboard.count = 0;
    
    FILE *fp = fopen(SCORE_FILE, "r");
    if (fp) {
        char name[20];
        int wins;
        while (fscanf(fp, "%19[^,], %d", name, &wins) == 2) {
            if (game->scoreboard.count < MAX_SCORES) {
                strncpy(game->scoreboard.entries[game->scoreboard.count].name, name, 20);
                game->scoreboard.entries[game->scoreboard.count].wins = wins;
                game->scoreboard.count++;
            }
        }
        fclose(fp);
        printf("Loaded %d scores from %s\n", game->scoreboard.count, SCORE_FILE);
    } else {
        printf("No scores file found. Creating new scores.txt\n");
        fp = fopen(SCORE_FILE, "w");
        if (fp) {
            fprintf(fp, "Name, Win\n"); 
            fclose(fp);
        }
    }
    pthread_mutex_unlock(&game->scoreboard.lock);
}

// Save scores to file
void save_scores() {
    pthread_mutex_lock(&game->scoreboard.lock);
    
    FILE *fp = fopen(SCORE_FILE, "w");
    if (fp) {
        for (int i = 0; i < game->scoreboard.count; i++) {
            fprintf(fp, "%s %d\n", game->scoreboard.entries[i].name, game->scoreboard.entries[i].wins);
        }
        fclose(fp);
        printf("Scores saved to %s\n", SCORE_FILE);
    }
    
    pthread_mutex_unlock(&game->scoreboard.lock);
}

// Update score
void update_winner_score(const char *name) {
    pthread_mutex_lock(&game->scoreboard.lock);
    
    int found = 0;
    for (int i = 0; i < game->scoreboard.count; i++) {
        if (strcmp(game->scoreboard.entries[i].name, name) == 0) {
            game->scoreboard.entries[i].wins++;
            found = 1;
            break;
        }
    }
    
    if (!found && game->scoreboard.count < MAX_SCORES) {
        strncpy(game->scoreboard.entries[game->scoreboard.count].name, name, 20);
        game->scoreboard.entries[game->scoreboard.count].wins = 1;
        game->scoreboard.count++;
    }
    
    pthread_mutex_unlock(&game->scoreboard.lock);
    
    // Save to file
    save_scores();
}

// --- GAME LOGIC ---

void send_to_player(int player_id, const char *msg) {
    if (player_id < 0 || player_id >= MAX_PLAYERS) return;
    if (!game->players[player_id].is_active) return;
    
    if (game->players[player_id].has_msg) {
        size_t current_len = strlen(game->players[player_id].outbound_msg);
        size_t max_len = sizeof(game->players[player_id].outbound_msg) - 1;
        if (current_len < max_len) {
            strncat(game->players[player_id].outbound_msg, msg, max_len - current_len);
        }
    } else {
        strncpy(game->players[player_id].outbound_msg, msg, sizeof(game->players[player_id].outbound_msg) - 1);
        game->players[player_id].outbound_msg[sizeof(game->players[player_id].outbound_msg) - 1] = '\0';
        game->players[player_id].has_msg = 1;
    }
}

// Broadcast to all active players
void broadcast(const char *msg) {
    for (int i = 0; i < MAX_PLAYERS; i++) {
        if (game->players[i].is_active) {
            send_to_player(i, msg);
        }
    }
}


// round robin scheduler thread
void *rr_scheduler(void *arg) {
    printf("RR scheduler thread started.\n");
    while (server_running) {
        // Sleep to avoid cpu spinning
        usleep(100000); // 100ms
        
        pthread_mutex_lock(&game->game_lock);
        
        // Check if the game logic in child process that a turn ended
        if (game->game_active && game->next_turn) {
            // Find next active player
            int attempts = 0;
            do {
                game->current_player = (game->current_player + 1) % MAX_PLAYERS;
                attempts++;
            } while (!game->players[game->current_player].is_active && attempts < MAX_PLAYERS);
            
            if (attempts >= MAX_PLAYERS) {
                // Check if everyone left
                game->game_active = 0;
                broadcast("All players left. Game Over.\n");
                log_queue("Game stopped: not enough players.");
            } else {
                // Notify all players who is in the current turn
                char msg[100];
                snprintf(msg, sizeof(msg), "\n>>> It is %s's turn! <<<\n> ", 
                         game->players[game->current_player].name);
                broadcast(msg);
                
                // Notifiy the current player
                send_to_player(game->current_player, "YOUR TURN! Type 'roll'.\n> ");
                
                log_queue("Turn passed to %s (ID %d)", 
                          game->players[game->current_player].name, game->current_player);
            }
            
            game->next_turn = 0; // Reset signal
        }
        
        pthread_mutex_unlock(&game->game_lock);
    }
    return NULL;
}

// Get the board string to send the current board for all players
void get_board_string(char* buffer, int size) {
    int offset = snprintf(buffer, size, "\n--- CURRENT BOARD ---\n");
    for (int i = 0; i < MAX_PLAYERS; i++) {
        if (game->players[i].is_active) {
            offset += snprintf(buffer + offset, size - offset, 
                            "%s: %d/%d%s\n", 
                            game->players[i].name, 
                            game->players[i].position, 
                            BOARD_SIZE,
                            (game->players[i].position >= BOARD_SIZE) ? " (WINNER)" : "");
        }
    }
    snprintf(buffer + offset, size - offset, "---------------------\n> ");
}

// --Child process (fork from parent to handle client)
// Child process to handle client session
void handle_client(int socket_fd, int player_id) {
    char buffer[256];
    fd_set read_fds;
    struct timeval tv;

    int r = player_id*5.5;
    srand(time(NULL) ^ getpid() ^ r);

    // get the player name
    memset(buffer, 0, sizeof(buffer));
    int bytes = recv(socket_fd, buffer, sizeof(buffer) - 1, 0);
    if (bytes > 0) {
        buffer[bytes] = '\0';
        if (strncmp(buffer, "NAME:", 5) == 0) {
            pthread_mutex_lock(&game->game_lock);
            // Copy the name sent by client and skip the "NAME:"
            snprintf(game->players[player_id].name, 20, "%s", buffer + 5);
            game->players[player_id].name[strcspn(game->players[player_id].name, "\n")] = 0;
            pthread_mutex_unlock(&game->game_lock);
        }
    } else {
        // Fallback if read fails
        snprintf(game->players[player_id].name, 20, "Player%d", player_id+1);
    }
    
    log_queue("Player %d (%s) connected.", player_id, game->players[player_id].name);
    
    while (1) {
        // wait for either socket input or shared memory message
        FD_ZERO(&read_fds);
        FD_SET(socket_fd, &read_fds);
        tv.tv_sec = 0;
        tv.tv_usec = 100000; // Check shared memory every 100ms
        
        int activity = select(socket_fd + 1, &read_fds, NULL, NULL, &tv);
        
        pthread_mutex_lock(&game->game_lock);
        if (!game->players[player_id].is_active) {
            pthread_mutex_unlock(&game->game_lock);
            break; 
        }
        
        // If Scheduler or has a message, send it to the client
        if (game->players[player_id].has_msg) {
            send(socket_fd, game->players[player_id].outbound_msg, 
                strlen(game->players[player_id].outbound_msg), MSG_NOSIGNAL);
            game->players[player_id].has_msg = 0; 
            game->players[player_id].outbound_msg[0] = '\0';
        }
        pthread_mutex_unlock(&game->game_lock);
        
        // Check socket input
        if (activity > 0 && FD_ISSET(socket_fd, &read_fds)) {
            int bytes = recv(socket_fd, buffer, sizeof(buffer) - 1, 0);
            if (bytes <= 0) {
                log_queue("Player %d (%s) disconnected.", player_id, game->players[player_id].name);
                break;
            }
            
            buffer[bytes] = '\0';
            buffer[strcspn(buffer, "\n")] = 0; // Remove newline
            
            // LOGIC
            pthread_mutex_lock(&game->game_lock);
            
            if (strncmp(buffer, "ROLL", 4) == 0 || strncmp(buffer, "roll", 4) == 0) {
                // Only roll if game is active and is in the turn
                if (game->game_active && game->current_player == player_id) {
                    
                    // Random
                    int roll = (rand() % 6) + 1;
                    game->players[player_id].position += roll;
                    
                    log_queue("%s rolled %d (Pos: %d)", game->players[player_id].name, roll, game->players[player_id].position);
                    
                    // broadcast message
                    char board_msg[1024];
                    snprintf(board_msg, sizeof(board_msg), 
                            "\n%s rolled %d (Pos: %d)", 
                            game->players[player_id].name, roll, game->players[player_id].position);
                    
                    char temp_board[512];
                    get_board_string(temp_board, sizeof(temp_board));
                    strncat(board_msg, temp_board, sizeof(board_msg) - strlen(board_msg) - 1);

                    broadcast(board_msg); 
                    
                    // Check Win
                    if (game->players[player_id].position >= BOARD_SIZE) {
                        game->players[player_id].wins++; 
                        game->game_active = 0; // stop game
                        
                        log_queue("GAME OVER: %s won!", game->players[player_id].name);
                        update_winner_score(game->players[player_id].name);

                        // Notify Winner
                        char win_msg[] = "\n=== CONGRATULATIONS, YOU WIN! ===\n";
                        send(socket_fd, win_msg, strlen(win_msg), MSG_NOSIGNAL);
                        // Notify Losers
                        for (int i = 0; i < MAX_PLAYERS; i++) {
                            if (game->players[i].is_active && i != player_id) {
                                    char loose_msg[200];
                                snprintf(loose_msg, sizeof(loose_msg), "# %s WINS!\n", game->players[player_id].name);
                                    send_to_player(i, loose_msg);
                            }
                        }

                        
                        // Broadcast Reset Timer
                        broadcast("\nNEW GAME STARTING IN 10 SECONDS");
                        
                        if (game->players[player_id].has_msg) {
                            send(socket_fd, game->players[player_id].outbound_msg, 
                                 strlen(game->players[player_id].outbound_msg), MSG_NOSIGNAL);
                            // Clear buffer
                            game->players[player_id].has_msg = 0;
                            game->players[player_id].outbound_msg[0] = '\0';
                        }

                        // Unlock to sleep (let other threads/processes breathe)
                        pthread_mutex_unlock(&game->game_lock);
                        
                        printf("Game won by %s. Resetting in 10s...\n", game->players[player_id].name);
                        sleep(10); 
                        
                        // Game reset for next new game
                        pthread_mutex_lock(&game->game_lock);
                        
                        for(int i=0; i<MAX_PLAYERS; i++) {
                            game->players[i].position = 0; // Reset positions
                            game->players[i].has_msg = 0;
                            game->players[i].outbound_msg[0] = '\0';
                        }
                        
                        game->game_active = 1; // Restart game
                        game->current_player = player_id; // Winner starts next game
                        game->next_turn = 1; // Trigger scheduler for first turn
                        
                        broadcast("\nNEW GAME STARTED\n> ");
                        log_queue("New game started automatically.");
                        
                    } else {
                        // Else: game continues, RR scheduler to rotate turn
                        game->next_turn = 1;
                    }
                } else {
                    send_to_player(player_id, "Not your turn!\n> ");
                }
            } else if (strncmp(buffer, "CHAT", 4) == 0) {
                 char chat[300];
                 snprintf(chat, sizeof(chat), "\n[Chat] %s: %s\n> ", game->players[player_id].name, buffer + 5);
                 broadcast(chat);
                 printf("[Chat] Msg from %s\n", game->players[player_id].name);
            } else if (strncmp(buffer, "QUIT", 4) == 0) {
                log_queue("Player %s quit voluntarily.", game->players[player_id].name);
                pthread_mutex_unlock(&game->game_lock);
                break;
            }
            
            pthread_mutex_unlock(&game->game_lock);
        }
    }
    
    // Cleanup
    pthread_mutex_lock(&game->game_lock);
    game->players[player_id].is_active = 0;
    game->num_players--;
    
    // Stop the game if the game is active and players less than 3
    if (game->game_active && game->num_players < 3) {
        game->game_active = 0; // Stop the game
        
        char msg[256];
        snprintf(msg, sizeof(msg), "\nCurrent player disconnected. Not enough players to continue (Need 3+).\nGame Stopped.\n");
        broadcast(msg);
        log_queue("Game stopped: current player disconnected, not enough players.");
    }
    // skip turn if a player disconnected
    else if (game->game_active && game->current_player == player_id) {
        game->next_turn = 1;
        log_queue("Current player disconnected. Skipping turn...");
    }
    
    pthread_mutex_unlock(&game->game_lock);
    
    close(socket_fd);
    exit(0); // Exit child
}

void handle_signal(int s) {
    if (s == SIGINT) {
        printf("\n[Server] Shutting down...\n");
        server_running = 0;
        // Save scores on exit
        save_scores();
        kill(0, SIGKILL);
    } else if (s == SIGCHLD) {
        while (waitpid(-1, NULL, WNOHANG) > 0);
    }
}

int main() {
    int server_fd, client_fd;
    struct sockaddr_in server_addr, client_addr;
    socklen_t client_len = sizeof(client_addr);
    
    signal(SIGINT, handle_signal);
    signal(SIGCHLD, handle_signal);
    
    // Setup Shared Memory
    int shm_fd = shm_open(SHM_NAME, O_CREAT | O_RDWR, 0666);
    ftruncate(shm_fd, sizeof(GameState)); // set size
    game = mmap(0, sizeof(GameState), PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
    
    // Clear Shared Memory
    memset(game, 0, sizeof(GameState)); 
    
    // Initialize Locks
    pthread_mutexattr_t att;
    pthread_mutexattr_init(&att);
    pthread_mutexattr_setpshared(&att, PTHREAD_PROCESS_SHARED);
    pthread_mutex_init(&game->game_lock, &att);
    pthread_mutex_init(&game->log_queue.lock, &att);
    pthread_mutex_init(&game->scoreboard.lock, &att);
    
    // Init Game State
    game->num_players = 0;
    game->game_active = 0;
    srand(time(NULL));
    
    // Load existing scores
    load_scores();
    
    // Start threads
    pthread_t sched_tid, log_tid;
    pthread_create(&sched_tid, NULL, rr_scheduler, NULL);
    pthread_create(&log_tid, NULL, logger, NULL);
    
    // Create socket
    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    
    // Configure server address
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(PORT);

    // Bind socket
    if (bind(server_fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        perror("Bind failed");
        exit(1);
    }
    
    // Listen for connections
    if (listen(server_fd, 5) < 0) {
        perror("Listen failed");
        exit(1);
    }
    
    printf("=== Dice Race Server ===\n");
    printf("Listening on port %d\n", PORT);
    log_queue("Server started on port %d", PORT);
    printf("Waiting for 3-5 players to connect...\n");
    printf("Waiting for players...\n");
    
    // Accept connections (loop)
    while (1) {
        client_fd = accept(server_fd, (struct sockaddr*)&client_addr, &client_len);
        if (client_fd < 0) continue;
        
        printf("New connection from %s:%d\n", 
               inet_ntoa(client_addr.sin_addr), ntohs(client_addr.sin_port));
        
        pthread_mutex_lock(&game->game_lock);
        int player_id = -1;
        if (game->num_players < MAX_PLAYERS) {
            for(int i=0; i<MAX_PLAYERS; i++) {
                if(!game->players[i].is_active) {
                    player_id = i;
                    break;
                }
            }
        }
        
        if (player_id != -1) {
            // Assign new player
            game->players[player_id].is_active = 1;
            game->players[player_id].id = player_id;
            game->players[player_id].position = 0;
            game->players[player_id].has_msg = 0;
            game->players[player_id].outbound_msg[0] = '\0';
            game->num_players++;
            
            // If 3 players, start game automatically if not started
            if (game->num_players >= 3 && !game->game_active) {
                game->game_active = 1;
                game->current_player = player_id; 
                game->next_turn = 1; 
                printf("Game Started!\n");
                log_queue("Game started with %d players.", game->num_players);
            } 
            // send wait more player message to client
            else if (!game->game_active) {
                 char wait_msg[128];
                 snprintf(wait_msg, sizeof(wait_msg), "Waiting for more players to start... (%d/3 connected)\n", game->num_players);
                 strncpy(game->players[player_id].outbound_msg, wait_msg, sizeof(game->players[player_id].outbound_msg) - 1);
                 game->players[player_id].has_msg = 1;
            }
        }
        pthread_mutex_unlock(&game->game_lock);
        
        if (player_id == -1) {
            close(client_fd); 
            continue;
        }
        
        // Fork handling
        pid_t pid = fork();
        
        if (pid == 0) {
            // Child process
            close(server_fd); 
            snprintf(game->players[player_id].name, 20, "Player%d", player_id+1);
            // Handle the child process
            handle_client(client_fd, player_id); 
        } else {
            // Parent process
            close(client_fd); 
        }
    }
    
    return 0;
}