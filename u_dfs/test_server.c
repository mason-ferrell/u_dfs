#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <dirent.h>
#include <errno.h>
#include <sys/time.h>

int main(int argc, char **argv) {
	int sockfd, client_sock;
	struct sockaddr_in proxy, client;
	int clientlen;
	char buf[2000];
	
	//create/open proxy socket
	sockfd = socket(AF_INET, SOCK_STREAM, 0);
	if(sockfd < 0)
		perror("opening socket");
		
	int optval = 1;
	if(setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, (const void *) &optval, sizeof(int)) < 0)
		perror("setting reuseaddr");
	
	//populate proxy info
	proxy.sin_family = AF_INET;
	proxy.sin_addr.s_addr = INADDR_ANY;
	proxy.sin_port = htons(atoi(argv[1]));
	
	//bind proxy, set listen queue to 3
	if(bind(sockfd, (struct sockaddr *)&proxy, sizeof(proxy)) < 0)
		perror("binding socket");

	listen(sockfd, 3);
	
	clientlen = sizeof(client);
	
	client_sock = accept(sockfd, (struct sockaddr *)&client,(socklen_t *)&clientlen);
	if(client_sock < 0) 
		perror("accepting connection");
	
	recv(client_sock, buf, 2000, 0);
	
	write(client_sock, "received", 8);
}
