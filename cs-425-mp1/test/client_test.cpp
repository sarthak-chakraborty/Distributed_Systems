#include <sys/types.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <chrono>
#include <bits/stdc++.h>


using namespace std;
using namespace std::chrono;


#define MAX 100			// Max length of commands
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

    // Adding null character to the end
    int last_index = byte_read_count/sizeof(read_msg[0]);
    read_msg[last_index] = '\0';
}


char* get_ip_from_domain(string domain) {
    /*
    Given a domain name, finds the ip address for the same
    */

	struct hostent *ip;
	struct in_addr **adr;

    char domain_name[domain.length()+1];
    strcpy(domain_name, domain.c_str());

	// DNS query for IP address of the domain
	ip = gethostbyname(domain_name);
	if(ip == NULL){
		cerr << "Domain name is wrong" << endl;
		exit(0);
	}
	adr = (struct in_addr **)ip->h_addr_list;
    
	return inet_ntoa(*adr[0]);
}




int main(int argc, char *argv[]) {

    vector<string> domains = { "fa23-cs425-3701.cs.illinois.edu", 
                                "fa23-cs425-3702.cs.illinois.edu",
                                "fa23-cs425-3703.cs.illinois.edu", 
                                // "fa23-cs425-3704.cs.illinois.edu",
                                // "fa23-cs425-3705.cs.illinois.edu",
                                // "fa23-cs425-3706.cs.illinois.edu" ,
                                // "fa23-cs425-3707.cs.illinois.edu" ,
                                // "fa23-cs425-3708.cs.illinois.edu" ,
                                // "fa23-cs425-3709.cs.illinois.edu" ,
                                // "fa23-cs425-3710.cs.illinois.edu" ,
                             };



    string pattern = "Mozilla";

    string cmnd = "grep -c " + pattern;
    char mssg[MAX];
    strcpy(mssg, cmnd.c_str());

    int total_matches = 0;


    // For every machine, connect to its specific socket and then send the grep command and get its result
    for (int k = 1; k <= domains.size(); k++) {

        char return_msg[MAX];
        try
        {
            int client_sock_fd;
            struct sockaddr_in cli_addr, serv_addr;

            
            if((client_sock_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
                cerr << "Socket creation failed" << endl;
            }

            memset(&serv_addr, 0, sizeof(serv_addr));
            memset(&cli_addr, 0, sizeof(cli_addr));

            int PORT = BASE_PORT + k;

            // Specifying the address of the control server at server
            serv_addr.sin_family = AF_INET;
            // ctrlserv_addr.sin_addr.s_addr = INADDR_ANY;
            serv_addr.sin_addr.s_addr = inet_addr(get_ip_from_domain(domains[k-1]));
            serv_addr.sin_port = htons(PORT);

            // Connecting to the control server
            int connect_status = connect(client_sock_fd, (struct sockaddr *)&serv_addr, sizeof(serv_addr));
            if (connect_status < 0)
                throw new exception;

            
            // Send grep command
            int sz = send(client_sock_fd, mssg, strlen(mssg)+1, 0);
            if (sz < 1 && sizeof(mssg) < 1)
                throw new exception;

            // Receive input from other machines on the grep command
            int sz2 = recv(client_sock_fd, return_msg, MAX, 0);

            total_matches += stoi(return_msg);
            
            string print_msg = "VM" + to_string(k) + ": " + return_msg;

            close(client_sock_fd);
        }
        catch (...) {
        }

    }


    cout << "Total Matches: " << total_matches << endl;


    ///////////////////////////////////////////////////////

    // Ground Truth Query for unit test
    
    // int n_logs_to_check = 3;
    string ground_truth_grep_mssg = "grep " + string(pattern) + " MP1\\ Demo\\ Data\\ FA22/* -c | awk -F: '{ sum=sum+$2 } END { print sum }'";
    string final_query = ground_truth_grep_mssg + " > gt_output.txt";

    // Run the grep command to get the ground truth result
    system(final_query.c_str());

    ifstream gt_out_file;
    gt_out_file.open("gt_output.txt");
    char line[MAX];
    if (gt_out_file.is_open()) {
        gt_out_file.getline(line, MAX);
        gt_out_file.close();
    }

    int gt_total_match = stoi(line);
    cout << "Ground Truth Matches: " << gt_total_match << endl << endl;
    
    if (gt_total_match == total_matches)
        cout << "UNIT TEST PASSED :) !!" << endl << endl;
    else
        cout << "UNIT TEST FAILED :( !!" << endl << endl;


    return 0;
}