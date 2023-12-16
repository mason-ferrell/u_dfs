#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <dirent.h>
#include <sys/time.h>
#include <errno.h>
#include <pthread.h>

#define BUFSIZE 4096

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

//helper function to make sure everything is written
int socket_write(int sock, char *message, int stream_size) {
	int bytes_written, unsent_bytes, new_bytes_written;
	
	bytes_written = write(sock, message, stream_size);
	if(bytes_written < 0) return -1;
	unsent_bytes = stream_size - bytes_written;
	
	while(unsent_bytes > 0) {
		new_bytes_written = write(sock, message+bytes_written, unsent_bytes);
		if(new_bytes_written < 0) return -1;
		
		bytes_written += new_bytes_written;
		unsent_bytes = stream_size - bytes_written;
	}
	return 0;
}

//used to pass arguments to threads
typedef struct {
	int argc;
	char **argv;
	int sock;
} thread_args;

void *server_thread(void*);
void parse_command(int, char*, char*);
int recv_cmd(int, char*);

void list(int, char*);
void put(int, char*, char*);
void get(int, char*, char*);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

int main(int argc, char *argv[]) {
	int sockfd, client_sock;
	struct sockaddr_in server, client;
	int clientlen;
	
	if(argc < 3) {
		printf("Usage %s <dir> <port #>\n", argv[0]);
		exit(-1);
	}
	
	//create/open server socket
	sockfd = socket(AF_INET, SOCK_STREAM, 0);
	if(sockfd < 0)
		perror("opening socket");
		
	int optval = 1;
	if(setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, (const void *) &optval, sizeof(int)) < 0)
		perror("setting reuseaddr");
		
	//set up pthread attributes
	pthread_attr_t attr;
    	pthread_attr_init(&attr);
	
	//populate server info
	server.sin_family = AF_INET;
	server.sin_addr.s_addr = INADDR_ANY;
	if(atoi(argv[2])!=0)
		server.sin_port = htons(atoi(argv[2]));
	else {
		printf("Must use a valid port!\n");
		exit(-1);
	}
	
	//bind server, set listen queue to 3
	if(bind(sockfd, (struct sockaddr *)&server, sizeof(server)) < 0)
		perror("binding socket");

	listen(sockfd, 3);
	
	clientlen = sizeof(client);
	
	//in case directory doesn't exist, make the directory
	mkdir(argv[1], 0700);
	
	while(1) {
		//accept connection and pass to thread
		thread_args pa;
		thread_args *arg_ptr;
		
		client_sock = accept(sockfd, (struct sockaddr *)&client,(socklen_t *)&clientlen);
		if(client_sock < 0) {
			perror("accepting connection");
			continue;
		}
		
		pa.sock = client_sock;
		pa.argc = argc;
		pa.argv = argv;
		
		pthread_t runner;
	
		arg_ptr = malloc(sizeof(thread_args));
		if(arg_ptr==NULL) perror("malloc before proxy thread creation");
		*arg_ptr = pa;
		
		pthread_create(&runner, &attr, server_thread, (void *) arg_ptr);
	}
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

void *server_thread(void *args) {
	thread_args *a = (thread_args *)args;
	
	//constantly waits for command until client tells server to exit
	while(1) {
			
		char buffer[BUFSIZE];
		bzero(buffer, BUFSIZE);
		if(recv_cmd(a->sock, buffer)!=0) break;
		if(strcmp(buffer, "exit\r\n\r\n")==0) break;
		
		parse_command(a->sock, buffer, a->argv[1]);
			
	}
	close(a->sock);
	return NULL;
}

void parse_command(int sock, char *buffer, char *dfs) {
	char *command, *file;
	command = strtok(buffer, " \r\n");
	if(command==NULL) {
		perror("Malformed command");
		return;
	}
	else if(strcasecmp(command, "list")==0)
		list(sock, dfs);
	else if(strcasecmp(command, "put")==0) {
		file = strtok(NULL, "\r\n");
		put(sock, dfs, file);
	}
	else if(strcasecmp(command, "get")==0) {
		file = strtok(NULL, "\r\n");
		get(sock, file, dfs);
	}
}

//this function receives one character at a time from a socket
//and terminates at double carriage return
int recv_cmd(int client_sock, char *buffer) {
	char *ch_buf = buffer;
	int rcv_ch;
	int counter = 0;
	
	while((rcv_ch = recv(client_sock, ch_buf, 1, 0))) {
		if(rcv_ch < 0) {
			perror("receiving command");
			return -1;
		}
		
		counter++;
		if(counter >= BUFSIZE) {
			perror("command too big");
			return -1;
		}
		
		if(counter >= 4 && strcmp(ch_buf - 3, "\r\n\r\n")==0) {
			return 0;
		}
		else
			ch_buf++;
	}
	return 1;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

void list(int sock, char *dfs) {
	struct dirent *d, *c;
	DIR *dh, *ch;
	char buf[BUFSIZE];
	char subdir[BUFSIZE];
	int lines = 0;
	
	bzero(buf, BUFSIZE);
	
	//buffer will have one line per file with all the chunk numbers stored
	//i.e. each line looks like "filename chunk # ... chunk #\r\n"
	
	dh = opendir(dfs);
	if(!dh) perror("opening directory");
	
	//for each element in current directory, add to buffer in a line
	//skip '.' and '..'
	while((d = readdir(dh)) != NULL) {
		if(d->d_name[0] == '.') continue;
		strncat(buf, d->d_name, BUFSIZE - strlen(buf));
		
		//subdir refers to each subdirectory, as files are represented by subdirectories in the DFS
		bzero(subdir, BUFSIZE);
		strncpy(subdir, dfs, BUFSIZE - strlen(subdir));
		strncat(subdir, "/", BUFSIZE - strlen(subdir));
		strncat(subdir, d->d_name, BUFSIZE - strlen(subdir));
		
		ch = opendir(subdir);
		if(!ch) perror("opening subdirectory");
		
		while((c = readdir(ch)) != NULL) {
			if(c->d_name[0]=='.') continue;
			
			strcat(buf, " ");
			strcat(buf, c->d_name);
		}
		
		strcat(buf, "\r\n\r\n");
		lines++;
	}
	
	//first we send the length of the buffer with list info, then the buffer
	socket_write(sock, (char *)&lines, sizeof(int));
	socket_write(sock, buf, strlen(buf));
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

//put stores files in directory given by dir_dfs
//file is stored as  a directory with same name
//containing files associated with chunks, named the chunk number
void put(int sock, char *dir_dfs, char *dir_filename) {
	FILE *fp;
	int chunk, chunk_size;
	int bytes_recvd = 0;
	
	//send chunk number and chunk size
	if(recv(sock, &chunk, sizeof(int), 0) < (ssize_t) sizeof(int)) perror("receving chunk num");
	if(recv(sock, &chunk_size, sizeof(int), 0) < (ssize_t) sizeof(int)) perror("receiving chunk size");
	
	char contents[chunk_size];
	char file_path[BUFSIZE];
	bzero(file_path, BUFSIZE);
	
	strcpy(file_path, dir_dfs);
	strcat(file_path, "/");
	strcat(file_path, dir_filename);
	
	mkdir(file_path, 0700);
	
	char chunk_toa[10];
	bzero(chunk_toa, 10);
	sprintf(chunk_toa, "%d", chunk);
	
	strcat(file_path, "/");
	strcat(file_path, chunk_toa);
	
	//hacky way to make sure full amount of content is received
	while((bytes_recvd += recv(sock, contents + bytes_recvd, chunk_size - bytes_recvd, 0)) < chunk_size);
	
	fp = fopen(file_path, "w");
	
	if(fwrite(contents, chunk_size, 1, fp) < 1) perror("writing file");
	
	fclose(fp);
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

void get(int sock, char *filename, char *dfs) {
	struct dirent *d;
	DIR *dh;
	FILE *fp;
	int max_path_l = strlen(filename) + strlen(dfs) + 20;
	char dir_path[max_path_l];
	char chunk_path[max_path_l];
	int chunk, chunk_size;
	
	strcpy(dir_path, dfs);
	strcat(dir_path, "/");
	strcat(dir_path, filename);
	
	dh = opendir(dir_path);
	if(!dh) {
		//if server doesn't have file, send chunk num of -1 as sentinel value to let client know
		chunk = -1;
		socket_write(sock, (char *)&chunk, sizeof(int));
		return;
	};
	
	//for each element in current directory, add to buffer in a line
	//skip '.' and '..', as well as the server binary listing
	while((d = readdir(dh)) != NULL) {
		if(d->d_name[0] == '.') continue;
		strcpy(chunk_path, dir_path);
		strcat(chunk_path, "/");
		strcat(chunk_path, d->d_name);
		
		chunk = atoi(d->d_name);
		
		fp = fopen(chunk_path, "r");
		
		fseek(fp, 0, SEEK_END);
		chunk_size = ftell(fp);
		fseek(fp, 0, SEEK_SET);
		
		socket_write(sock, (char *)&chunk, sizeof(int));
		socket_write(sock, (char *)&chunk_size, sizeof(int));
		
		char contents[chunk_size];
		if(fread(contents, chunk_size, 1, fp) < 1) perror("reading file");
		socket_write(sock, contents, chunk_size);
		
		fclose(fp);
	}
}
