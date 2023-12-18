#include <bits/stdc++.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <chrono>

using namespace std;
using namespace std::chrono;

#define MAX 100			// Max length of commands
#define BASE_PORT 8000


char* get_ip_from_domain(string domain) {
    /*
    Given a domain name, finds the ip address for the same
    */

	struct hostent *ip_addr;
	struct in_addr **addr;

    char domain_name[domain.length()+1];
    strcpy(domain_name, domain.c_str());

	// DNS query for IP address of the domain
	ip_addr = gethostbyname(domain_name);

    if(ip_addr != NULL)
	    addr = (struct in_addr **)ip_addr->h_addr_list;
    else {
        cerr << "Domain name is wrong" << endl;
        exit(0);
    }
    
	return inet_ntoa(*addr[0]);
}


int main(int argc, char *argv[]) {

    // List of all the servers
    vector<string> domains = { "fa23-cs425-3701.cs.illinois.edu", 
                                "fa23-cs425-3702.cs.illinois.edu",
                                "fa23-cs425-3703.cs.illinois.edu", 
                                "fa23-cs425-3704.cs.illinois.edu",
                                "fa23-cs425-3705.cs.illinois.edu",
                                "fa23-cs425-3706.cs.illinois.edu",
                                "fa23-cs425-3707.cs.illinois.edu",
                                "fa23-cs425-3708.cs.illinois.edu",
                                "fa23-cs425-3709.cs.illinois.edu",
                                "fa23-cs425-3710.cs.illinois.edu",
                             };

    // get the machine number from the command line argument and assign port at which the server will run    
    char *MACHINE_NUM = argv[1];

    cout << "[MACHINE " << MACHINE_NUM << "] Client Terminal Starting... \n";

    while(1) {
        string cmnd;

        // Terminal prompt and reading user input
        cout << "> ";
        getline(cin, cmnd);

        // If the command is not grep, dont proceed ahead
        if (cmnd.rfind("grep ", 0) != 0) { 
            cout << "Not a grep command!! Try again!" << endl << endl;
            continue;
        }


        char mssg[MAX];
        strcpy(mssg, cmnd.c_str());

        // To compute the total matches
        int total_matches = 0;

        auto start = high_resolution_clock::now();

        // For every machine, connect to its specific socket and then send the grep command and get its result
        for (int k = 1; k <= domains.size(); k++) {

            char return_msg[MAX];
            try
            {
                int client_sock_fd;
                struct sockaddr_in cli_addr, serv_addr;

                // Initialize client-side socket
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
                cout << print_msg << endl;

                close(client_sock_fd);
            }
            catch (...) {
            }
        }

        auto stop = high_resolution_clock::now();
        auto time = duration_cast<microseconds>(stop - start);

        // Print the total matches and the time taken
        cout << endl << "Total Matches: " << total_matches << endl;
        cout << "Time Taken: " << time.count() << " microseconds" << endl << endl;
    }

    return 0;
}