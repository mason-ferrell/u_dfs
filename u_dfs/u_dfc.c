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

#define BUFSIZE 4096

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

//helper function for writing to socket
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

//hash function (used this in proxy and didn't wanna add any more dependencies)
unsigned long fileHash(char *str) {
    unsigned long hash = 5381;
    int c;

    while ((c = *str++))
        hash = ((hash << 5) + hash) + c;

    return hash;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

//functionality functions
void list(int[], int);
void put(int[], char*);
void put_chunk(int, char*, int, int, char*);
void get(int[], char*);

//helper functions
int read_conf_file(int[]);
int connect_to_host(int*, char*);
int recv_line(int, char*);
void rmdir_rec(char*);
int get_file_size(FILE*);

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

int main(int argc, char **argv) {
	if(argc < 2) {
		printf("Usage: %s <command> [filename] ... [filename]\n", argv[0]);
		exit(-1);
	}
	int dfs[4];
	
	if(read_conf_file(dfs)==-1) {
		printf("Bad configuration file\n");
		exit(-1);	
	}
	
	//for put and get, must let server know we're done when we finish our commands, hence the second loop
	if(strcmp(argv[1], "list")==0) list(dfs, 4);
	if(strcmp(argv[1], "put")==0) {
		for(int i=2; i < argc; i++)
			put(dfs, argv[i]);
		for(int i=0; i < 4; i++)
			if(dfs[i]!=-1)
				socket_write(dfs[i], "exit\r\n\r\n", 8);
	}
	if(strcmp(argv[1], "get")==0) {
		for(int i=2; i < argc; i++)
			get(dfs, argv[i]);
		for(int i=0; i < 4; i++)
			if(dfs[i]!=-1)
				socket_write(dfs[i], "exit\r\n\r\n", 8);
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

//assuming a maximum of 512 files
//currently not enforced
void list(int dfs[], int num_serv) {
	//files is a hashmap, file[hash] contains the filename, where hash is the hash value of the filename
	//if two files hash to the same name, we just move the file to the next available index and check
	//we have correct filename every time we reference the files array
	
	//chunks uses the file hash to determine which chunks it has
	int chunks[512][num_serv];
	char files[512][512];
	
	memset(files, 0, 512*512);
	memset(chunks, 0, 512*num_serv);

	//for each server, get a list of filenames and associated chunks
	for(int i=0; i<num_serv; i++) {
		//if server was never connected, ignore
		if(dfs[i] == -1) continue;
		
		int lines;
		
		socket_write(dfs[i], "list\r\n\r\n", 8);
		if(recv(dfs[i], &lines, sizeof(int), 0) < (ssize_t) sizeof(int))
			continue;
		
		char buffer[BUFSIZE];
		
		for(int j=0; j<lines; j++) {
			bzero(buffer, BUFSIZE);
			
			if(recv_line(dfs[i], buffer)!=0) break;
			
			char *filename;
			int chunk1, chunk2;
			
			filename = strtok(buffer, " ");
			chunk1 = atoi(strtok(NULL, " "));
			chunk2 = atoi(strtok(NULL, " "));
			
			//add file information to file and chunk hashmaps
			int fh = fileHash(filename) % 512;
			int f_in = fh;
			while(files[f_in][0]!=0 && strcmp(files[f_in], filename)!=0)
				f_in++;
			if(files[f_in][0]==0) {
				strcpy(files[f_in], filename);
				chunks[f_in][chunk1] = 1;
				chunks[f_in][chunk2] = 1;
			} else {
				chunks[f_in][chunk1] = 1;
				chunks[f_in][chunk2] = 1;
			}
		}
		
		socket_write(dfs[i], "exit\r\n\r\n", 8);
	}
	
	//list files and determine whether or not they are constructible
	for(int i = 0; i < 512; i++) {
		if(files[i][0]!=0) {
			int all_chunks = 1;
			for(int j=0; j<num_serv; j++) {
				if(chunks[i][j]==0)
					all_chunks = 0;
			}
			
			if(all_chunks)
				printf("%s\n", files[i]);
			else
				printf("%s [incomplete]\n", files[i]);
		}
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

void get(int dfs[], char *filename) {
	char file_dir[strlen(filename)+10];
	char file_path[strlen(filename)+20];
	int chunk, chunk_size, max_chunk_size = 0;
	int chunks_recvd[4] = {0,0,0,0};
	int construct = 1;
	char chunk_toa[3];
	FILE *fp, *chp;
	int bytes_recvd = 0;
	
	//store file chunks in directory sharing name of file
	strcpy(file_dir, "./");
	strcat(file_dir, filename);
	strcat(file_dir, ".dir");
	
	//if file directory doesn't exist, make it
	mkdir(file_dir, 0700);
	
	//loop through each server
	for(int i=0; i<4; i++) {
		if(dfs[i]==-1) continue;
		
		construct = 1;
		
		socket_write(dfs[i], "get ", 4);
		socket_write(dfs[i], filename, strlen(filename));
		socket_write(dfs[i], "\r\n\r\n", 4);
		
		for(int j=0; j<2; j++){
			bytes_recvd = 0;
			
			if(recv(dfs[i], &chunk, sizeof(int), 0) < (ssize_t) sizeof(int))
				perror("receiving chunk filename");
			if(chunk==-1) {
				//server might have sent sentinel to let us know it doesn't have the file
				printf("%s is incomplete\n", filename);
				rmdir_rec(file_dir);
				return;
			}
			
			if(recv(dfs[i], &chunk_size, sizeof(int), 0) < (ssize_t) sizeof(int))
				perror("receiving filesize");
			if(chunk_size > max_chunk_size) 
				max_chunk_size = chunk_size;
		
			char contents[chunk_size];
			bzero(chunk_toa, 3);
			sprintf(chunk_toa, "%d", chunk);
	
			bzero(file_path, strlen(filename) + 5);
			strcpy(file_path, file_dir);
			strcat(file_path, "/");
			strcat(file_path, chunk_toa);
			
			//hacky way to make sure all bytes of file are received
			while((bytes_recvd += recv(dfs[i], contents + bytes_recvd, chunk_size - bytes_recvd, 0)) < chunk_size);
	
			//create temp files to hold chunk info
			fp = fopen(file_path, "w");
			
			fwrite(contents, chunk_size, 1, fp);
	
			fclose(fp);
			
			chunks_recvd[chunk] = 1;
		}
		
		for(int j=0; j<4; j++)
			if(chunks_recvd[j]==0)
				construct = 0;
				
		if(construct==1) break;
	}
	
	if(construct==0) {
		printf("%s is incomplete\n", filename);
		rmdir_rec(file_dir);
		
		return;
	}
	
	//fp now contains the main file
	fp = fopen(filename, "w");
	
	for(int i=0; i<4; i++) {
		char chunk_toa[3];
		bzero(chunk_toa, 3);
		sprintf(chunk_toa, "%d", i);
		
		//file_path will get the path for each chunk
		bzero(file_path, strlen(filename) + 20);
		strcpy(file_path, file_dir);
		strcat(file_path, "/");
		strcat(file_path, chunk_toa);
		
		chp = fopen(file_path, "r");
		chunk_size = get_file_size(chp);
		
		char contents[chunk_size];
		if(fread(contents, chunk_size, 1, chp) < 1) 
			perror("reading local chunk file");
		if(fwrite(contents, chunk_size, 1, fp) < 1)
			perror("writing to reconstructed file");
		fclose(chp);
	}
	
	fclose(fp);
	
	rmdir_rec(file_dir);
}

//removes temporary directory for storing file chunk data
void rmdir_rec(char *file_dir) {
	struct dirent *d;
	DIR *dh;	
	dh = opendir(file_dir);
	if(!dh) perror("opening directory");
	
	//for each element in current directory, add to buffer in a line
	//skip '.' and '..', as well as the server binary listing
	while((d = readdir(dh)) != NULL) {
		if(d->d_name[0] == '.') continue;
		char file_name[strlen(file_dir) + 10];
		strcpy(file_name, file_dir);
		strcat(file_name, "/");
		strcat(file_name, d->d_name);
		
		if(remove(file_name) < 0)
			perror("removing chunk");
	}
	
	if(rmdir(file_dir) < 0)
		perror("removing temp directory");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

void put(int dfs[], char *filename) {
	FILE *fp = fopen(filename, "r");
	int invalid_flag = 0;
	int hash_bucket, index;
	
	//make sure file exists and we're connected to all 4 servers
	if(fp==NULL)
		invalid_flag = 1;

	for(int i = 0; i<4; i++) {
		if(dfs[i] == -1)
			invalid_flag = 1;
	}
	
	if(invalid_flag==1) {
		printf("%s put failed\n", filename);
		if(fp!=NULL)
			fclose(fp);
		return;
	}

	int file_size, chunk_size, offset_chunks;
	
	file_size = get_file_size(fp);
	chunk_size = file_size/4 + 1;
	offset_chunks = file_size % 4;
	
	char chunks[4][chunk_size + 1];
	
	//make sure we read proper number of bytes if not perfectly divisible by 4
	for(int i = 0; i < 4; i++) {
		if(i < offset_chunks) {
			if(fread(chunks[i], chunk_size, 1, fp) < 1)
				perror("reading into chunk");
		}
		else
			if(fread(chunks[i], chunk_size - 1, 1, fp) < 1)
				perror("reading into chunk");
	}
	
	fclose(fp);
	
	//hash_bucket will tell us which servers get which chunks
	hash_bucket = fileHash(filename) % 4;
	for(int i = 0; i < 4; i++) {
		index = (i + hash_bucket) % 4;
		
		if(i < offset_chunks) {
		
			put_chunk(dfs[index], filename, i, chunk_size, chunks[i]);
			put_chunk(dfs[(index+3)%4], filename, i, chunk_size, chunks[i]);
			
		} else {
		
			int chunk_minus_one = chunk_size - 1;
			put_chunk(dfs[index], filename, i, chunk_minus_one, chunks[i]);
			put_chunk(dfs[(index+3)%4], filename, i, chunk_minus_one, chunks[i]);
			
		}
	}
}

//send individual chunks to socket given by sock
void put_chunk(int sock, char *filename, int chunk, int chunk_size, char *contents) {
	socket_write(sock, "put ", 4);
	socket_write(sock, filename, strlen(filename));
	socket_write(sock, "\r\n\r\n", 4);
	socket_write(sock, (char *)&chunk, sizeof(int));
	socket_write(sock, (char *)&chunk_size, sizeof(int));
	socket_write(sock, contents, chunk_size);
}

int get_file_size(FILE *fp) {
	int retval;
	
	fseek(fp, 0, SEEK_END);
	retval = ftell(fp);
	fseek(fp, 0, SEEK_SET);
	
	return retval;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

//reads configuration file
//if errors, return -1
int read_conf_file(int dfs[]) {
	int connected = 0;
	char *home = getenv("HOME");
	
	char filepath[strlen(home) + 20];
	strncpy(filepath, home, strlen(home) + 20);
	strncat(filepath, "/dfc.conf", strlen(home) + 20 - strlen(filepath));
	
	FILE *fp = fopen(filepath, "r");
	if(fp==NULL) return -1;
	
	for(int i = 0; i < 4; i++) {
		char line[50];
		if(fgets(line, 50, fp)==NULL) {
			fclose(fp);
			return -1;
		}
		
		char *s = strtok(line, " ");
		if(s==NULL || strcmp(s, "server")!=0) {
			fclose(fp);
			return -1;
		}
		
		char *s_n = strtok(NULL, " ");
		if(s_n==NULL)
			return -1;
		char *hn = strtok(NULL, " \r\n");
		if(connect_to_host(dfs + i, hn) == -1) *(dfs + i) = -1;
		else connected++;
	}
	
	fclose(fp);
	return connected;
}

//connect to host, adapted from beej's guide
int connect_to_host(int *server_sock, char *hostname) {

	char host_port[strlen(hostname)+1];
	struct addrinfo hints, *servinfo, *p;
	char *host, *port, port_str[8];
	
	bzero(port_str, 8);
	strcpy(host_port, hostname);
	host = strtok(host_port, ":");
	if(host==NULL) return -1;
	port = strtok(NULL, ":");
	if(port==NULL) return -1;
	else strcpy(port_str, port);
	

	memset(&hints, 0, sizeof hints);
	hints.ai_family = AF_INET;
	hints.ai_socktype = SOCK_STREAM;
	
	if (getaddrinfo(host, port_str, &hints, &servinfo) != 0) {
	    return -1;
	}
	
	for(p = servinfo; p != NULL; p = p->ai_next) {
   		if ((*server_sock = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1) {
		        perror("socket");
        		continue;
    		}

        	if (connect(*server_sock, p->ai_addr, p->ai_addrlen) == -1)
        		return -1;

    		break;
	}
	
	return 0;
}

//receive lines delimited by \r\n\r\n
int recv_line(int client_sock, char *buffer) {
	char *ch_buf = buffer;
	int rcv_ch;
	int counter = 0;
	
	while((rcv_ch = recv(client_sock, ch_buf, 1,0))) {
		if(rcv_ch < 0) {
			perror("receiving command");
			return -1;
		}
		
		counter++;
		if(counter >= BUFSIZE) {
			perror("command too big");
			return -1;
		}
		
		if(counter >= 4 && strcmp(ch_buf - 3, "\r\n\r\n")==0)
			return 0;
		else
			ch_buf++;
	}
	
	return 1;
}
