#include <bits/stdc++.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <netdb.h>

using namespace std;

#define MAX 100	
#define BASE_PORT 8000


void grep(char *read_msg, char **args) {
    /*
    Grep a document (through execvp) which is passed in args and stores the result in read_msg
    */

    int pipefd[2];
    pipe(pipefd);
    pid_t grep_pid, wpid;

    // Fork and run the grep command via execvp system call. 
    // Redirect the output from the standard output file (terminal) through pipe
    if((grep_pid=fork()) == 0) {
        
        close(pipefd[0]);
        dup2(pipefd[1], 1);
        close(pipefd[1]);
        execvp(args[0], args);
        exit(0);
    }
    else {
        close(pipefd[1]);
        wait(&grep_pid);
    }

    int byte_read_count = read(pipefd[0], read_msg, sizeof(read_msg));
    close(pipefd[0]);

    int last_index = byte_read_count/sizeof(read_msg[0]);
    read_msg[last_index] = '\0';
}


int main(int argc, char *argv[]) {

    int serv_sock_fd, serv_accptsock_fd;
    struct sockaddr_in serv_addr, cli_addr;

    // get the machine number from the command line argument and assign port at which the server will run
    char *MACHINE_NUM = argv[1];
    int PORT = BASE_PORT + stoi(MACHINE_NUM);

    // Initiate the log filename for this machine
    string filename = string("MP1 Demo Data FA22/vm") + MACHINE_NUM + ".log";

    char mssg[MAX];
    char cmnd[MAX];
    socklen_t clilen;
    fstream logfile;

    // Creating socket
    if((serv_sock_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0){ // IPv4, TCP
        cerr << "Socket creation failed" << endl;
        exit(0);
    }

    // Initializing the socket address to 0
    memset(&serv_addr, 0, sizeof(serv_addr));
    memset(&cli_addr, 0, sizeof(cli_addr));

    // Specifying the control server address
    serv_addr.sin_family = AF_INET; // IPv4
    serv_addr.sin_addr.s_addr = INADDR_ANY; // any address
    serv_addr.sin_port = htons(PORT);

    // Setting Socket options to reuse socket address
    int var = 1;
    if (setsockopt(serv_sock_fd, SOL_SOCKET, SO_REUSEADDR, &var, sizeof(int)) < 0){
        cerr << "setsockopt failed" << endl;
        exit(0);
    }

    // Binding the server
    if(bind(serv_sock_fd,(const struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0){
        cerr << "Binding failed" << endl;
        exit(0);
    }

    // Machine accepting connections
    listen(serv_sock_fd, 10);
    cout << "Server Running..." << endl;

    // Server is always running
    while(1) {

        clilen = sizeof(cli_addr);
        // Accept control connection from client
        serv_accptsock_fd = accept(serv_sock_fd, (struct sockaddr *)&cli_addr, &clilen);

        // Receive the grep command from the client
        recv(serv_accptsock_fd, cmnd, MAX, 0);

        // Tokenize the command, making it suitable for execvp command
        char *args[MAX];
        char *word = strtok (cmnd," ");
        int i = 0;
        while (word != NULL){
            args[i++] = word;
            word = strtok (NULL, " ");
        }
        // Append the filename and NULL in the end
        args[i++] = (char*)filename.c_str();
        args[i++] = (char*)"-c";
        args[i] = NULL;

        // Open the log file
        logfile.open(filename, ios::in);


        // If file is open, run the grep command with the pattern sent by the client and send the number of matching patterns
        if (logfile.is_open()) {
            char send_msg[MAX];
            grep(send_msg, args);

            send(serv_accptsock_fd, send_msg, strlen(send_msg)+1, 0);
            cout << "Sent: " << send_msg << endl;
            logfile.close();
        }
        close(serv_accptsock_fd);
    }

    return 0;
}