#pragma once

#include <sys/time.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <unistd.h>
#include <netdb.h>
#include <string.h>
#include <poll.h>
#include <omp.h>

#include "parallel/turbo_dist_internal.hpp"
#include "common/atom.hpp"


#define IP_ADDRESS_MAX_LEN 15
//#define BASE_PORTNUM 9000

enum TCP_MODE
{
	SERVER,
	CLIENT
};


enum ReturnStatus {
	OK,
	DONE,
	FAIL,
	ON_GOING,
};


class turbo_tcp {

	public:
		turbo_tcp() {}

		/*turbo_channel(int base_portnum_) {
			client_addr = new struct sockaddr_in;
			base_portnum = base_portnum_;
		}*/

		~turbo_tcp() {
		}
		
		static ReturnStatus init_host() {
			hostname_list = new char*[PartitionStatistics::num_machines()];
			for(int i = 0; i < PartitionStatistics::num_machines(); i++)
				hostname_list[i] = new char[IP_ADDRESS_MAX_LEN];

			char hostname[100];
			struct hostent* host;

			gethostname(hostname, 100);
			host = gethostbyname(hostname);
			memcpy((void*)hostname_list[PartitionStatistics::my_machine_id()], (void*)inet_ntoa(*(struct in_addr*)host->h_addr_list[0]), IP_ADDRESS_MAX_LEN);
			//MPI initialize first
			int num_machines = PartitionStatistics::num_machines();
			for(int i = 0; i < num_machines; i++) {
				if (i < num_machines) {
					MPI_Bcast((void*)hostname_list[i], IP_ADDRESS_MAX_LEN, MPI_BYTE, i, MPI_COMM_WORLD);
				} else {
				}
			}
			return OK;
		}
		
		void set_port() {
			const char* base_portnum_str = getenv("TCP_BASE_PORTNUM");
			int BASE_PORTNUM = atoi(base_portnum_str);
			base_portnum = BASE_PORTNUM + PartitionStatistics::num_machines() * connection_count;
			close_count = 10000 * (PartitionStatistics::my_machine_id() + 1);
			if(PartitionStatistics::my_machine_id() == 0) {
				fprintf(stdout, "[turbo_tcp] Establishing tcp connections between all machines, base_portnum = %d\n", base_portnum);
			}
		}

		/*void set_channel_parameter(int level) {
			channel_level = level;
            //base_portnum = (UserArguments::VECTOR_PARTITIONS * PartitionStatistics::num_machines()) * (turbo_channel::connection_count * UserArguments::MAX_LEVEL + level);
		}*/


		ReturnStatus open_serversocket_all(bool set_timeo_) {
			tcp_mode = SERVER;
            set_timeo = set_timeo_;
			int temp_buf = 1;
			int64_t buf_size = 851968L;
			int flag = 1;
			socklen_t rn = sizeof(temp_buf);
            struct timeval tv_timeo = {10, 0};
			struct linger ling;
			ling.l_onoff = 1;
			ling.l_linger = 0;
            server_socket = new int[PartitionStatistics::num_machines()];
            server_addr = new struct sockaddr_in[PartitionStatistics::num_machines()];
            for(int i = 0; i < PartitionStatistics::num_machines(); i++) {
                server_socket[i] = socket(PF_INET, SOCK_STREAM, 0);
                if(server_socket[i] == -1) {
                    fprintf(stdout, "[turbo_tcp] Machine %ld failed to open server socket\n", PartitionStatistics::my_machine_id());
                    perror("");
                    return FAIL;
                }
                setsockopt(server_socket[i], SOL_SOCKET, SO_REUSEADDR, &temp_buf, sizeof(temp_buf));
                setsockopt(server_socket[i], SOL_SOCKET, SO_KEEPALIVE, &temp_buf, rn);
                setsockopt(server_socket[i], SOL_SOCKET, SO_LINGER, &ling, sizeof(ling));
                if (set_timeo) {
                    setsockopt(server_socket[i], SOL_SOCKET, SO_RCVTIMEO, &tv_timeo, sizeof(tv_timeo));
                    setsockopt(server_socket[i], SOL_SOCKET, SO_SNDTIMEO, &tv_timeo, sizeof(tv_timeo));
                }
                /*setsockopt(server_socket[i], SOL_SOCKET, SO_SNDBUF, &buf_size, (socklen_t)rn);
                setsockopt(server_socket[i], SOL_SOCKET, SO_RCVBUF, &buf_size, (socklen_t)rn);*/
                setsockopt(server_socket[i], IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(int));
                memset(&server_addr[i], 0, sizeof(sockaddr_in));
                server_addr[i].sin_family = AF_INET;
                server_addr[i].sin_port = htons(base_portnum + i);
                server_addr[i].sin_addr.s_addr = htonl(INADDR_ANY);
            }

            client_socket_for_server = new int[PartitionStatistics::num_machines()];

			// XXX
            client_socket_for_server_lock = new atom[PartitionStatistics::num_machines()];
			return OK;
		}

		ReturnStatus open_clientsocket_all(bool set_timeo_) {
			tcp_mode = CLIENT;
            set_timeo = set_timeo_;
			int64_t buf_size = 851968; //XXX
			int flag = 1;
			socklen_t rn = sizeof(flag);
            struct timeval tv_timeo = {10, 0};
			struct linger ling;
			ling.l_onoff = 1;
			ling.l_linger = 0;
			server_addr = new struct sockaddr_in[PartitionStatistics::num_machines()];
			client_socket_lock = new atom[PartitionStatistics::num_machines()];
			client_socket = new int[PartitionStatistics::num_machines()];
			for(int i = 0; i < PartitionStatistics::num_machines(); i++) {
				client_socket[i] = socket(PF_INET, SOCK_STREAM, 0);
				if(client_socket[i] == -1) {
					fprintf(stdout, "[turbo_tcp] Machine %ld failed to open client socket\n", PartitionStatistics::my_machine_id());
                    perror("");
					return FAIL;
				}
				setsockopt(client_socket[i], SOL_SOCKET, SO_KEEPALIVE, &flag, rn);
				setsockopt(client_socket[i], SOL_SOCKET, SO_LINGER, &ling, sizeof(ling));
                if (set_timeo) {
                    setsockopt(client_socket[i], SOL_SOCKET, SO_RCVTIMEO, &tv_timeo, sizeof(tv_timeo));
                    setsockopt(client_socket[i], SOL_SOCKET, SO_SNDTIMEO, &tv_timeo, sizeof(tv_timeo));
                }
				/*setsockopt(client_socket[i], SOL_SOCKET, SO_SNDBUF, &buf_size, (socklen_t)rn);
				setsockopt(client_socket[i], SOL_SOCKET, SO_RCVBUF, &buf_size, (socklen_t)rn);*/
				setsockopt(client_socket[i], IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(int));

				memset(&server_addr[i], 0, sizeof(sockaddr_in));
				server_addr[i].sin_family = AF_INET;
				server_addr[i].sin_port = htons(base_portnum + PartitionStatistics::my_machine_id());
				server_addr[i].sin_addr.s_addr = inet_addr(hostname_list[i]);
			}
			return OK;
		}

		ReturnStatus close_socket() {
			if(tcp_mode == SERVER) {
                for(int i = 0; i < PartitionStatistics::num_machines(); i++) {
                    close(client_socket_for_server[i]);
                    close(server_socket[i]);
                }
                delete[] server_socket;
                delete[] server_addr;
                delete[] client_socket_for_server;
                delete[] client_socket_for_server_lock;
			} else if (tcp_mode == CLIENT) {
				for(int i = 0; i < PartitionStatistics::num_machines(); i++) {
					close(client_socket[i]);
				}
                delete[] server_addr;
                delete[] client_socket;
                delete[] client_socket_lock;
			} else {
                D_ASSERT(false);
            }
            if (hostname_list != NULL) {
                for(int i = 0; i < PartitionStatistics::num_machines(); i++)
                    delete[] hostname_list[i];
                delete[] hostname_list;
                hostname_list = NULL;
            }
			return OK;
		}

		ReturnStatus bind_socket() {
			D_ASSERT(tcp_mode == SERVER);
            for(int i = 0; i < PartitionStatistics::num_machines(); i++) {
                if(bind(server_socket[i], (struct sockaddr*)&server_addr[i], sizeof(server_addr[i])) == -1) {
                    close(server_socket[i]);
					fprintf(stdout, "[turbo_tcp] Machine %ld failed to bind server socket\n", PartitionStatistics::my_machine_id());
                    perror("");
                    return FAIL;
                }
            }
            return OK;
		}

		ReturnStatus listen_socket() {
			D_ASSERT(tcp_mode == SERVER);
            for(int i = 0; i < PartitionStatistics::num_machines(); i++) {
                if(listen(server_socket[i], 24) == -1) { //XXX control listen queue size
                    perror("[turbo_tcp] Failed to listen");
                    return FAIL;
                }
            }
			return OK;
		}
		
		ReturnStatus accept_socket() {
			D_ASSERT(tcp_mode == SERVER);
			int flag;
			int temp_buf = 1;
            int64_t buf_size = 851968L;
			socklen_t rn = sizeof(flag);
            struct timeval tv_timeo = {10, 0};
			struct linger ling;
			ling.l_onoff = 1;
			ling.l_linger = 0;
            for(int i = 0; i < PartitionStatistics::num_machines(); i++) {
                while(listen(server_socket[i], 24) == -1) { //XXX control listen queue size
                    //fprintf(stdout, "[turbo tcp][type:%d,lv:%d] Failed to listen\n");
                    //return FAIL;
                }
                client_addr_size = sizeof(client_addr);
                setsockopt(client_socket_for_server[i], SOL_SOCKET, SO_REUSEADDR, &temp_buf, sizeof(temp_buf));
                setsockopt(client_socket_for_server[i], SOL_SOCKET, SO_KEEPALIVE, &flag, rn);
                setsockopt(client_socket_for_server[i], SOL_SOCKET, SO_LINGER, &ling, sizeof(ling));
                if (set_timeo) {
                    setsockopt(client_socket_for_server[i], SOL_SOCKET, SO_RCVTIMEO, &tv_timeo, sizeof(tv_timeo));
                    setsockopt(client_socket_for_server[i], SOL_SOCKET, SO_SNDTIMEO, &tv_timeo, sizeof(tv_timeo));
                }
				/*setsockopt(client_socket_for_server[i], SOL_SOCKET, SO_SNDBUF, &buf_size, (socklen_t)rn);
				setsockopt(client_socket_for_server[i], SOL_SOCKET, SO_RCVBUF, &buf_size, (socklen_t)rn);*/
                setsockopt(client_socket_for_server[i], IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(int));
                client_socket_for_server[i] = accept(server_socket[i], (struct sockaddr*)&client_addr, &client_addr_size);
                if(client_socket_for_server[i] == -1) {
                    perror("[turbo_tcp] Failed to accept");
                    return FAIL;
                }
            }
			return OK;
		}
		
        ReturnStatus accept_socket(int partition_id) {
			D_ASSERT(tcp_mode == SERVER);
			int flag;
			int temp_buf = 1;
            int64_t buf_size = 851968L;
			socklen_t rn = sizeof(flag);
            struct timeval tv_timeo = {10, 0};
			struct linger ling;
			ling.l_onoff = 1;
			ling.l_linger = 0;
            while(listen(server_socket[partition_id], 24) == -1) { //XXX control listen queue size
                //fprintf(stdout, "[turbo tcp][type:%d,lv:%d] Failed to listen\n");
                //return FAIL;
            }
            client_addr_size = sizeof(client_addr);
            setsockopt(client_socket_for_server[partition_id], SOL_SOCKET, SO_REUSEADDR, &temp_buf, sizeof(temp_buf));
            setsockopt(client_socket_for_server[partition_id], SOL_SOCKET, SO_KEEPALIVE, &flag, rn);
            setsockopt(client_socket_for_server[partition_id], SOL_SOCKET, SO_LINGER, &ling, sizeof(ling));
            if (set_timeo) {
                setsockopt(client_socket_for_server[partition_id], SOL_SOCKET, SO_RCVTIMEO, &tv_timeo, sizeof(tv_timeo));
                setsockopt(client_socket_for_server[partition_id], SOL_SOCKET, SO_SNDTIMEO, &tv_timeo, sizeof(tv_timeo));
            }
            /*setsockopt(client_socket_for_server[partition_id], SOL_SOCKET, SO_SNDBUF, &buf_size, (socklen_t)rn);
            setsockopt(client_socket_for_server[partition_id], SOL_SOCKET, SO_RCVBUF, &buf_size, (socklen_t)rn);*/
            setsockopt(client_socket_for_server[partition_id], IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(int));
            client_socket_for_server[partition_id] = accept(server_socket[partition_id], (struct sockaddr*)&client_addr, &client_addr_size);
            if(client_socket_for_server[partition_id] == -1) {
                fprintf(stdout, "[%ld] Failed to accept %d\n", PartitionStatistics::my_machine_id(), partition_id);
                perror("");
                return FAIL;
            }
            //fprintf(stdout, "[%ld] Accept success %ld\n", PartitionStatistics::my_machine_id(), partition_id);
			return OK;
		}

		ReturnStatus connect_socket(int partition_id) {
			D_ASSERT(tcp_mode == CLIENT);
            while(connect(client_socket[partition_id], (struct sockaddr*)&server_addr[partition_id], sizeof(struct sockaddr)) == -1) {}
	//			fprintf(stdout, "[%ld] Connect success\n", PartitionStatistics::my_machine_id());
			return OK;
		}
        
        void disconnect_socket(int partition_id) {
            if(tcp_mode == SERVER) {
                close(client_socket_for_server[partition_id]);
            } else {
                close(client_socket[partition_id]);
            }
        }
        
        bool randomly_disconnect_socket(int partition_id) {
            return false;
            //if (rand() % 16 == 0) {
            if ((rand() * (777 - PartitionStatistics::my_machine_id() * partition_id)) % 10000000 == 0) {
            //if ((rand() * (777 - PartitionStatistics::my_machine_id() * partition_id)) % 100000 == 0) {
            //if (false) {
                disconnect_socket(partition_id);
                return true;
            }
            return false;
        }
		
		int64_t send_to_client(const char* buf, int64_t start_offset, int64_t size_to_send, int partition_id, bool retry=false) {
			int64_t write_size;
			int64_t result;
			buf += start_offset;
            int fail_count = 0;
			
Retry:
			write_size = 0;

			while(write_size < 8) {
				result = write(client_socket_for_server[partition_id], (char*)&size_to_send + write_size, 8 - write_size);
				if (result < 0) {
                    //if (result == EAGAIN || result == EWOULDBLOCK) {
                    if (retry) {
                        //fprintf(stdout, "[%ld->%ld][turbo tcp] send_to_client EAGAIN || EWOULDBLOCK\n", PartitionStatistics::my_machine_id(), partition_id);
                        fprintf(stdout, "[turbo tcp] Send to client error from %ld to %ld, size_to_send = %ld, sent_size = %ld\n",  PartitionStatistics::my_machine_id(), partition_id, size_to_send, write_size);
                        perror("[turbo tcp]send to client size_to_send error");
                        reaccept_socket(partition_id);
                        fprintf(stdout, "[turbo tcp]Send to client %ld --> %ld ReStart\n", PartitionStatistics::my_machine_id(), partition_id);
                        goto Retry;
                    } else {
                        fprintf(stdout, "[turbo tcp] Send to client error from %ld to %ld, size_to_send = %ld, sent_size = %ld\n",  PartitionStatistics::my_machine_id(), partition_id, size_to_send, write_size);
                        perror("[turbo tcp]send to client size_to_send error");
                        //fail_count++;
                        if(fail_count > TCP_MAX_FAIL_COUNT)
                            return error_handling_mpi_send(buf, size_to_send, partition_id);
                        reaccept_socket(partition_id);
                        fprintf(stdout, "[turbo tcp]Send to client %ld --> %ld Return -1\n", PartitionStatistics::my_machine_id(), partition_id);
                        return -1;
                    }
					//goto Retry;
				}
				write_size += result;
			}

			D_ASSERT(write_size == 8);

			write_size = 0;
			while(write_size < size_to_send) {
                //if (create_error) {
                //    if (randomly_disconnect_socket(partition_id)) {
                //        fprintf(stdout, "[%ld] random disconnect at send_to_client to %d\n", PartitionStatistics::my_machine_id(), partition_id);
                //    }
                //}
				result = write(client_socket_for_server[partition_id], buf + write_size, size_to_send - write_size); //XXX control bufer length (+1? for null)
				D_ASSERT(result <= size_to_send - write_size);
				if (result < 0) {
                    //if (result == EAGAIN || result == EWOULDBLOCK) {
                    if (retry) {
                        fprintf(stdout, "[turbo tcp] Send to client error, from %ld to %ld, size_to_send = %ld, sent_size = %ld\n", PartitionStatistics::my_machine_id(), partition_id, size_to_send, write_size);
                        perror("[turbo tcp] send to client data error");
                        reaccept_socket(partition_id);
                        fprintf(stdout, "[turbo tcp] Send to client %ld --> %ld ReStart\n", PartitionStatistics::my_machine_id(), partition_id);
                        //fprintf(stdout, "[%ld->%ld][turbo tcp] send_to_client EAGAIN || EWOULDBLOCK\n", PartitionStatistics::my_machine_id(), partition_id);
                        goto Retry;
                    } else {
                        fprintf(stdout, "[turbo tcp] Send to client error, from %ld to %ld, size_to_send = %ld, sent_size = %ld\n", PartitionStatistics::my_machine_id(), partition_id, size_to_send, write_size);
                        perror("[turbo tcp] send to client data error");
                        //fail_count++;
                        if(fail_count > TCP_MAX_FAIL_COUNT)
                            return error_handling_mpi_send(buf, size_to_send, partition_id);
                        reaccept_socket(partition_id);
                        fprintf(stdout, "[turbo tcp] Send to client %ld --> %ld Return -1\n", PartitionStatistics::my_machine_id(), partition_id);
                        return -1;
                    }
				}
				write_size += result;
			}
            turbo_tcp::AddBytesSent(size_to_send + 8, partition_id);
			if(write_size != size_to_send)
				fprintf(stdout, "[turbo tcp] From %ld to %ld, Write size = %ld, Size to send = %ld\n", PartitionStatistics::my_machine_id(), partition_id, write_size, size_to_send);
			D_ASSERT(write_size == size_to_send);
			return write_size;
		}

		int64_t send_to_server(const char* buf, int64_t start_offset, int64_t size_to_send, int partition_id, bool retry=false) {
            int64_t write_size;
			int64_t result;
            int fail_count = 0;
			int socket_id = partition_id;
			buf += start_offset;

Retry:
			write_size = 0;
			while(write_size < 8) {
				result = write(client_socket[socket_id], (char*)&size_to_send + write_size, 8 - write_size);
				D_ASSERT(result <= 8 - write_size);
				if (result < 0) {
                    //if (result == EAGAIN || result == EWOULDBLOCK) {
                    if (retry) {
                        //fprintf(stdout, "[%ld->%ld][turbo tcp] send_to_server EAGAIN || EWOULDBLOCK\n", PartitionStatistics::my_machine_id(), partition_id);
                        fprintf(stdout, "[turbo tcp] Send to server error with errno %d, from %ld to %ld, size_to_send = %ld, sent_size = %ld\n", errno, PartitionStatistics::my_machine_id(), partition_id, size_to_send, write_size);
                        perror("[turbo tcp] send to server size_to_write error");
                        reconnect_socket(partition_id);
                        fprintf(stdout, "[%ld] [turbo tcp] Send to server %ld -> %ld ReStart\n", PartitionStatistics::my_machine_id(), PartitionStatistics::my_machine_id(), partition_id);
                        goto Retry;
                    } else {
                        fprintf(stdout, "[turbo tcp] Send to server error with errno %d, from %ld to %ld, size_to_send = %ld, sent_size = %ld\n", errno, PartitionStatistics::my_machine_id(), partition_id, size_to_send, write_size);
                        perror("[turbo tcp] send to server size_to_write error");
                        //fail_count++;
                        if(fail_count > TCP_MAX_FAIL_COUNT)
                            return error_handling_mpi_send(buf, size_to_send, partition_id);
                        reconnect_socket(partition_id);
                        fprintf(stdout, "[%ld] [turbo tcp] Send to server %ld -> %ld ReStart\n", PartitionStatistics::my_machine_id(), PartitionStatistics::my_machine_id(), partition_id);
                        return -1;
                    }
				}
				write_size += result;
			}
			
			D_ASSERT(size_to_send >= 0);
			write_size = 0;
			result = 0;
			while(write_size < size_to_send) {
                /*if (create_error) {
                    if (randomly_disconnect_socket(partition_id)) {
                        fprintf(stdout, "[%ld] random disconnect at send_to_server to %d\n", PartitionStatistics::my_machine_id(), partition_id);
                    }
                }*/
                result = write(client_socket[socket_id], buf + write_size, size_to_send - write_size); //XXX control bufer length (+1? for null)
                /*if ((rand() * (777 - PartitionStatistics::my_machine_id() * partition_id)) % 1000000 == 0) {
                    fprintf(stdout, "[turbo tcp] %ld -> %ld send to server random fail occur\n", PartitionStatistics::my_machine_id(), partition_id);
                    result = -1;
                } else {
                    result = write(client_socket[socket_id], buf + write_size, size_to_send - write_size); //XXX control bufer length (+1? for null)
                }*/
				D_ASSERT(result <= size_to_send - write_size);
				if (result < 0) {
                    //if (result == EAGAIN || result == EWOULDBLOCK) {
                    if (retry) {
                        fprintf(stdout, "[turbo tcp] Send to server error with errno %d, from %ld to %ld, size_to_send = %ld, sent_size = %ld\n", errno, PartitionStatistics::my_machine_id(), partition_id, size_to_send, write_size);
                        perror("[turbo tcp] send to server data error");
                        //fail_count++;
                        if(fail_count > TCP_MAX_FAIL_COUNT)
                            return error_handling_mpi_send(buf, size_to_send, partition_id);
                        reconnect_socket(partition_id);
                        fprintf(stdout, "[%ld] [turbo tcp] Send to server %ld -> %ld ReStart\n", PartitionStatistics::my_machine_id(), PartitionStatistics::my_machine_id(), partition_id);
                        //fprintf(stdout, "[%ld->%ld][turbo tcp] send_to_server EAGAIN || EWOULDBLOCK\n", PartitionStatistics::my_machine_id(), partition_id);
                        goto Retry;
                    } else {
                        fprintf(stdout, "[turbo tcp] Send to server error with errno %d, from %ld to %ld, size_to_send = %ld, sent_size = %ld\n", errno, PartitionStatistics::my_machine_id(), partition_id, size_to_send, write_size);
                        perror("[turbo tcp] send to server data error");
                        //fail_count++;
                        if(fail_count > TCP_MAX_FAIL_COUNT)
                            return error_handling_mpi_send(buf, size_to_send, partition_id);
                        reconnect_socket(partition_id);
                        fprintf(stdout, "[%ld] [turbo tcp] Send to server %ld -> %ld Return -1\n", PartitionStatistics::my_machine_id(), PartitionStatistics::my_machine_id(), partition_id);
                        return -1;
                    }
				}
				write_size += result;
			}
            turbo_tcp::AddBytesSent(size_to_send + 8, partition_id);
			if(write_size != size_to_send)
				fprintf(stdout, "[turbo tcp] From %ld to %ld, Write size = %ld, Size to send = %ld\n", PartitionStatistics::my_machine_id(), partition_id, write_size, size_to_send);
			D_ASSERT(write_size == size_to_send);
			return write_size;
		}

		int64_t recv_from_server(const char* buf, int64_t start_offset, int partition_id, bool retry=false) {
            //turbo_timer recv_timer;
			int64_t read_size;
			int64_t result;
			int64_t size_to_recv = -1;
            int fail_count = 0;
			int socket_id = partition_id;
			buf += start_offset;

Retry:
            //recv_timer.start_timer(0);
			read_size = 0;
			while(read_size < 8) {
				result = read(client_socket[socket_id], (char*)&size_to_recv + read_size, 8 - read_size);
				if (result < 0) {
                    //if (result == EAGAIN || result == EWOULDBLOCK) {
                    if (retry) {
                        //fprintf(stdout, "[%ld<-%ld][turbo tcp] recv_from_server EAGAIN || EWOULDBLOCK\n", PartitionStatistics::my_machine_id(), partition_id);
                        fprintf(stdout, "[turbo tcp] Recv from server error, from %ld to %ld, size_to_recv = %ld, read_size = %ld\n", partition_id, PartitionStatistics::my_machine_id(), size_to_recv, read_size);
                        perror("[turbo tcp] recv from server size_to_recv error");
                        reconnect_socket(partition_id);
                        fprintf(stdout, "[turbo tcp] Recv from server %ld <-- %ld ReStart\n", PartitionStatistics::my_machine_id(), partition_id);
                        goto Retry;
                    } else {
                        fprintf(stdout, "[turbo tcp] Recv from server error, from %ld to %ld, size_to_recv = %ld, read_size = %ld\n", partition_id, PartitionStatistics::my_machine_id(), size_to_recv, read_size);
                        perror("[turbo tcp] recv from server size_to_recv error");
                        //fail_count++;
                        if(fail_count > TCP_MAX_FAIL_COUNT)
                            return error_handling_mpi_recv(buf, partition_id);
                        reconnect_socket(partition_id);
                        fprintf(stdout, "[turbo tcp] Recv from server %ld <-- %ld Return -1\n", PartitionStatistics::my_machine_id(), partition_id);
                        return -1;
                    }
				}
				read_size += result;
			}
            //recv_timer.stop_timer(0);

			D_ASSERT(size_to_recv >= 0);
			read_size = 0;
			result = 0;

            //recv_timer.start_timer(1);
			while(read_size < size_to_recv) {
                /*if (create_error) {
                    if (randomly_disconnect_socket(partition_id)) {
                        fprintf(stdout, "[%ld] random disconnect at recv_from_server to %d\n", PartitionStatistics::my_machine_id(), partition_id);
                    }
                }*/
				result = read(client_socket[socket_id], (void*)buf + read_size, size_to_recv - read_size);
				D_ASSERT(result <= size_to_recv - read_size);
				if (result < 0) {
                    //if (result == EAGAIN || result == EWOULDBLOCK) {
                    if (retry) {
                        //fprintf(stdout, "[%ld<-%ld][turbo tcp] recv_from_server EAGAIN || EWOULDBLOCK\n", PartitionStatistics::my_machine_id(), partition_id);
                        fprintf(stdout, "[turbo tcp] Recv from server error, from %ld to %ld, size_to_recv = %ld, read_size = %ld\n", partition_id, PartitionStatistics::my_machine_id(), size_to_recv, read_size);
                        perror("[turbo tcp] recv from server data error");
                        reconnect_socket(partition_id);
                        fprintf(stdout, "[turbo tcp] Recv from server %ld <-- %ld ReStart\n", PartitionStatistics::my_machine_id(), partition_id);
                        goto Retry;
                    } else {
                        fprintf(stdout, "[turbo tcp] Recv from server error, from %ld to %ld, size_to_recv = %ld, read_size = %ld\n", partition_id, PartitionStatistics::my_machine_id(), size_to_recv, read_size);
                        perror("[turbo tcp] recv from server data error");
                        //fail_count++;
                        if(fail_count > TCP_MAX_FAIL_COUNT)
                            return error_handling_mpi_recv(buf, partition_id);
                        reconnect_socket(partition_id);
                        fprintf(stdout, "[turbo tcp] Recv from server %ld <-- %ld Return -1\n", PartitionStatistics::my_machine_id(), partition_id);
                        return -1;
                    }
				}
				read_size += result;
			}
            //recv_timer.stop_timer(1);
            //recv_timer.start_timer(2);
            turbo_tcp::AddBytesReceived(size_to_recv + 8, partition_id);
			if(read_size != size_to_recv)
				fprintf(stdout, "[turbo tcp] From %ld to %ld, Read size = %ld, Size to recv = %ld\n", partition_id, PartitionStatistics::my_machine_id(), read_size, size_to_recv);
			D_ASSERT(read_size == size_to_recv);
            //recv_timer.stop_timer(2);
            //fprintf(stdout, "[%ld] from %ld, %.3f %.3f %.3f, %.2f MB/s\n", PartitionStatistics::my_machine_id(), partition_id, recv_timer.get_timer(0), recv_timer.get_timer(1), recv_timer.get_timer(2), (double) size_to_recv / (1024 * 1024 * recv_timer.get_timer(1)));
			return read_size;
		}

		int64_t recv_from_client(const char* buf, int64_t start_offset, int partition_id, int64_t max_bytes=std::numeric_limits<int64_t>::max(), bool retry=false) {
            //turbo_timer tim;
            //tim.start_timer(0);
			int64_t read_size;
			int64_t result;
            int fail_count;
			int64_t size_to_recv;
			buf += start_offset;

Retry:
			read_size = 0;
			while(read_size < 8) {
				result = read(client_socket_for_server[partition_id], (char*)&size_to_recv + read_size, 8 - read_size);
				if (result < 0) {
                    //if (result == EAGAIN || result == EWOULDBLOCK) {
                    if (retry) {
                        fprintf(stdout, "[turbo tcp] Recv from client error with errno %d, from %ld to %ld, size_to_recv = %ld, read_size = %ld\n", errno, partition_id, PartitionStatistics::my_machine_id(), size_to_recv, read_size);
                        perror("[turbo tcp] recv from client size_to_recv error");
                        reaccept_socket(partition_id);
                        fprintf(stdout, "[%ld] [turbo tcp] Recv from client, %ld -> %ld ReStart\n", PartitionStatistics::my_machine_id(), partition_id, PartitionStatistics::my_machine_id());
                        //fprintf(stdout, "[%ld<-%ld][turbo tcp] recv_from_client EAGAIN || EWOULDBLOCK\n", PartitionStatistics::my_machine_id(), partition_id);
                        goto Retry;
                    } else {
                        fprintf(stdout, "[turbo tcp] Recv from client error with errno %d, from %ld to %ld, size_to_recv = %ld, read_size = %ld\n", errno, partition_id, PartitionStatistics::my_machine_id(), size_to_recv, read_size);
                        perror("[turbo tcp] recv from client size_to_recv error");
                        //fail_count++;
                        if(fail_count > TCP_MAX_FAIL_COUNT)
                            return error_handling_mpi_recv(buf, partition_id);
                        reaccept_socket(partition_id);
                        fprintf(stdout, "[%ld] [turbo tcp] Recv from client, %ld -> %ld Return -1\n", PartitionStatistics::my_machine_id(), partition_id, PartitionStatistics::my_machine_id());
                        return -1;
                    }
				}
				read_size += result;
			}
            
            if (size_to_recv > max_bytes || size_to_recv < 0) {
                fprintf(stdout, "[turbo tcp] Recv from client error with errno %d, from %ld to %ld, size_to_recv = %ld, read_size = %ld, max_bytes = %ld\n", errno, partition_id, PartitionStatistics::my_machine_id(), size_to_recv, read_size, max_bytes);
                return -1;
            }
		
			D_ASSERT(size_to_recv >= 0);
			read_size = 0;
			result = 0;
            //tim.stop_timer(0);

            //tim.start_timer(0);
			while(read_size < size_to_recv) {
                /*if (create_error) {
                    if (randomly_disconnect_socket(partition_id)) {
                        fprintf(stdout, "[%ld] random disconnect at recv_from_client to %d\n", PartitionStatistics::my_machine_id(), partition_id);
                    }
                }*/
                result = read(client_socket_for_server[partition_id], (void*)buf + read_size, size_to_recv - read_size);
				D_ASSERT(result <= size_to_recv - read_size);
				if (result < 0) {
                    if (retry) {
                        //fprintf(stdout, "[%ld<-%ld][turbo tcp] recv_from_client EAGAIN || EWOULDBLOCK\n", PartitionStatistics::my_machine_id(), partition_id);
                        fprintf(stdout, "[turbo tcp] Recv from client error with errno %d, from %ld to %ld, size_to_recv = %ld, read_size = %ld\n", errno, partition_id, PartitionStatistics::my_machine_id(), size_to_recv, read_size);
                        perror("[turbo tcp] recv from client data error");
                        reaccept_socket(partition_id);
                        fprintf(stdout, "[%ld] [turbo tcp] Recv from client, %ld -> %ld ReStart\n", PartitionStatistics::my_machine_id(), partition_id, PartitionStatistics::my_machine_id());
                        goto Retry;
                    } else {
                        fprintf(stdout, "[turbo tcp] Recv from client error with errno %d, from %ld to %ld, size_to_recv = %ld, read_size = %ld\n", errno, partition_id, PartitionStatistics::my_machine_id(), size_to_recv, read_size);
                        perror("[turbo tcp] recv from client data error");
                        //fail_count++;
                        if(fail_count > TCP_MAX_FAIL_COUNT)
                            return error_handling_mpi_recv(buf, partition_id);
                        reaccept_socket(partition_id);
                        fprintf(stdout, "[%ld] [turbo tcp] Recv from client, %ld -> %ld ReStart\n", PartitionStatistics::my_machine_id(), partition_id, PartitionStatistics::my_machine_id());
                        return -1;
                    }
				}
				read_size += result;
			}
            //tim.stop_timer(0);
            turbo_tcp::AddBytesReceived(size_to_recv + 8, partition_id);
			D_ASSERT(read_size == size_to_recv);
		    //fprintf(stdout, "[turbo tcp] From %ld to %ld, Read size = %ld, Size to recv = %ld\n", partition_id, PartitionStatistics::my_machine_id(), read_size, size_to_recv);
			//fprintf(stdout, "\tRecv from client %ld <-- %ld recv_bytes = %ld, elapsed = %.4f %.4f\n", PartitionStatistics::my_machine_id(), partition_id, size_to_recv, tim.get_timer(0), tim.get_timer(1));
			return read_size;
		}

		void recv_from_client_lock(int partition_id) { client_socket_for_server_lock[partition_id].lock(); }
		void recv_from_client_unlock(int partition_id) { client_socket_for_server_lock[partition_id].unlock(); }
		void send_to_server_lock(int partition_id) {
			int socket_id = partition_id;
			client_socket_lock[socket_id].lock(); 
		}
		void send_to_server_unlock(int partition_id) {
			int socket_id = partition_id;
			client_socket_lock[socket_id].unlock(); 
		}
		void lock_socket() { socket_lock.lock(); }
		void unlock_socket() { socket_lock.unlock(); }

        void check_connection() {
            /*struct pollfd fd;
            fd
            if (tcp_mode == SERVER) {
                poll();
            } else if (tcp_mode == CLIENT) {
                poll();
            }*/
        }

        static void establish_all_connections(turbo_tcp* server_sockets, turbo_tcp* client_sockets, bool set_timeo_=false) {
            server_sockets->set_port();
            client_sockets->set_port();
            //server_sockets->open_serversocket_all(set_timeo_);
            //client_sockets->open_clientsocket_all(set_timeo_);
            server_sockets->open_serversocket_all(false);
            client_sockets->open_clientsocket_all(false);
            PartitionStatistics::wait_for_all();
            server_sockets->bind_socket();

            for(int64_t i = 0; i < PartitionStatistics::num_machines(); i++) {
#pragma omp parallel num_threads(2)
                {
                    int tid = omp_get_thread_num();
                    int accept_id = (PartitionStatistics::my_machine_id() + i) % PartitionStatistics::num_machines();
                    int connect_id = (PartitionStatistics::my_machine_id() + PartitionStatistics::num_machines() - i) % PartitionStatistics::num_machines();
                    if (tid == 0)
                        server_sockets->accept_socket(accept_id);
                    else if (tid == 1)
                        client_sockets->connect_socket(connect_id);
                }
                PartitionStatistics::wait_for_all();
            }
            turbo_tcp::connection_count += 1;
        }
        
        static void Recv(turbo_tcp* server_sockets, const char* buf, int64_t start_offset, int partition_id, int64_t* bytes_receive, int64_t max_bytes=std::numeric_limits<int64_t>::max(), bool retry=false) {
            *bytes_receive = server_sockets->recv_from_client(buf, start_offset, partition_id, max_bytes, retry);
        }
        
        static void Send(turbo_tcp* client_sockets, const char* buf, int64_t start_offset, int64_t size_to_send, int partition_id, int64_t* bytes_send, bool retry=false) {
            *bytes_send = client_sockets->send_to_server(buf, start_offset, size_to_send, partition_id);
        }

        static void ResetBytesSentAndReceived() {
            accum_bytes_sent.store(0L);
            accum_bytes_received.store(0L);
        }
        
        static void AddBytesSent(int64_t bt, int64_t to = -1) {
            if (PartitionStatistics::my_machine_id() != to) {
                accum_bytes_sent.fetch_add(bt);
            }
        }
        
        static void AddBytesReceived(int64_t bt, int64_t from = -1) {
            if (PartitionStatistics::my_machine_id() != from) {
                accum_bytes_received.fetch_add(bt);
            }
        }
        
        static int64_t GetBytesSent() {
            return accum_bytes_sent.load();
        }
        
        static int64_t GetBytesReceived() {
            return accum_bytes_received.load();
        }

	private:
		ReturnStatus reconnect_socket(int partition_id) {
			D_ASSERT(tcp_mode == CLIENT);
			//XXX check really disconnected
			int socket_id = partition_id;
			int flag;
            int64_t buf_size = 851968L;
			socklen_t rn = sizeof(flag);
            struct timeval tv_timeo = {10, 0};
			struct linger ling;
			ling.l_onoff = 1;
			ling.l_linger = 0;
			fprintf(stdout, "[turbo_tcp] Machine %ld try to reconnect socket with Machine %ld\n", PartitionStatistics::my_machine_id(), partition_id);
			close(client_socket[socket_id]);
			sleep(1);
			client_socket[socket_id] = socket(PF_INET, SOCK_STREAM, 0);
			if(client_socket[socket_id] == -1) {
				fprintf(stdout, "[turbo_tcp] Machine %ld failed to open client socket\n", PartitionStatistics::my_machine_id());
				return FAIL;
			}
			setsockopt(client_socket[socket_id], SOL_SOCKET, SO_KEEPALIVE, &flag, rn);
			setsockopt(client_socket[socket_id], SOL_SOCKET, SO_LINGER, &ling, sizeof(ling));
            if (set_timeo) {
                setsockopt(client_socket[socket_id], SOL_SOCKET, SO_RCVTIMEO, &tv_timeo, sizeof(tv_timeo));
                setsockopt(client_socket[socket_id], SOL_SOCKET, SO_SNDTIMEO, &tv_timeo, sizeof(tv_timeo));
            }
            /*setsockopt(client_socket[socket_id], SOL_SOCKET, SO_SNDBUF, &buf_size, (socklen_t)rn);
            setsockopt(client_socket[socket_id], SOL_SOCKET, SO_RCVBUF, &buf_size, (socklen_t)rn);*/
			setsockopt(client_socket[socket_id], IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(int));
			while(connect(client_socket[socket_id], (struct sockaddr*)&server_addr[partition_id], sizeof(struct sockaddr)) == -1) {
				usleep (16*1024);
			}
			fprintf(stdout, "[turbo_tcp] Machine %ld reconnect success Machine %ld\n", PartitionStatistics::my_machine_id(), partition_id);
			return OK;
		}
		
		ReturnStatus reaccept_socket(int partition_id) {
			D_ASSERT(tcp_mode == SERVER);
			int flag;
            int temp_buf = 1;
            int64_t buf_size = 851968L;
			socklen_t rn = sizeof(flag);
            struct timeval tv_timeo = {10, 0};
			struct linger ling;
			ling.l_onoff = 1;
			ling.l_linger = 0;
			//XXX check really disconnected
			fprintf(stdout, "[turbo_tcp] Machine %ld try to reaccept socket Machine %ld\n", PartitionStatistics::my_machine_id(), partition_id);
			close(client_socket_for_server[partition_id]);
			sleep(1);
			while(listen(server_socket[partition_id], 24) == -1) {}
			fprintf(stdout, "[turbo_tcp] Machine %ld listen success Machine %ld\n", PartitionStatistics::my_machine_id(), partition_id);
			struct sockaddr_in* temp_client_addr = new struct sockaddr_in;
			socklen_t temp_client_addr_size = sizeof(temp_client_addr);
			setsockopt(client_socket_for_server[partition_id], SOL_SOCKET, SO_REUSEADDR, &temp_buf, sizeof(temp_buf));
			setsockopt(client_socket_for_server[partition_id], SOL_SOCKET, SO_KEEPALIVE, &flag, rn);
			setsockopt(client_socket_for_server[partition_id], SOL_SOCKET, SO_LINGER, &ling, sizeof(ling));
            if (set_timeo) {
                setsockopt(client_socket_for_server[partition_id], SOL_SOCKET, SO_RCVTIMEO, &tv_timeo, sizeof(tv_timeo));
                setsockopt(client_socket_for_server[partition_id], SOL_SOCKET, SO_SNDTIMEO, &tv_timeo, sizeof(tv_timeo));
            }
            /*setsockopt(client_socket_for_server[partition_id], SOL_SOCKET, SO_SNDBUF, &buf_size, (socklen_t)rn);
            setsockopt(client_socket_for_server[partition_id], SOL_SOCKET, SO_RCVBUF, &buf_size, (socklen_t)rn);*/
			setsockopt(client_socket_for_server[partition_id], IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(int));
			client_socket_for_server[partition_id] = accept(server_socket[partition_id], (struct sockaddr*)temp_client_addr, &temp_client_addr_size);
			if(client_socket_for_server[partition_id] == -1) {
				fprintf(stdout, "[turbo_tcp] Machine %ld failed to reaccept Machine %ld\n", PartitionStatistics::my_machine_id(), partition_id);
                perror("[turbo_tcp] Failed to accept");
				return FAIL;
			}
			fprintf(stdout, "[turbo_tcp] Machine %ld reaccept success Machine %ld, IP Address = %s\n", PartitionStatistics::my_machine_id(), partition_id, inet_ntoa(temp_client_addr->sin_addr));
			return OK;
		}

        int64_t error_handling_mpi_send(const char* buf, int64_t size_to_send, int partition_id) {
            int tag = base_portnum + partition_id;
            int mpi_error = -1;
            mpi_error = MPI_Send((void*)&size_to_send, sizeof(int64_t), MPI_CHAR, partition_id, tag, MPI_COMM_WORLD);
            D_ASSERT(mpi_error == MPI_SUCCESS);
            mpi_error = MPI_Send((void*)buf, size_to_send, MPI_CHAR, partition_id, tag, MPI_COMM_WORLD);
            D_ASSERT(mpi_error == MPI_SUCCESS);
            return size_to_send;
        }
        
        int64_t error_handling_mpi_recv(const char* buf, int partition_id) {
            int tag = base_portnum + PartitionStatistics::my_machine_id();
            int mpi_error = -1;
            int recv_bytes;
            MPI_Status stat;
            int64_t size_to_recv;

            mpi_error = MPI_Recv((void*)&size_to_recv, sizeof(int64_t), MPI_CHAR, partition_id, tag, MPI_COMM_WORLD, &stat);
            D_ASSERT(mpi_error == MPI_SUCCESS);
            MPI_Get_count(&stat, MPI_CHAR, &recv_bytes);
            D_ASSERT(recv_bytes == sizeof(int64_t));

            mpi_error = MPI_Recv((void*)buf, size_to_recv, MPI_CHAR, partition_id, tag, MPI_COMM_WORLD, &stat);
            D_ASSERT(mpi_error == MPI_SUCCESS);
            MPI_Get_count(&stat, MPI_CHAR, &recv_bytes);
            D_ASSERT(recv_bytes == size_to_recv);
            return size_to_recv;
        }	

	private:
		int* server_socket;
		int* client_socket_for_server;
		struct sockaddr_in* server_addr;

		int* client_socket;
		struct sockaddr_in* client_addr = new struct sockaddr_in;
		socklen_t client_addr_size;

		TCP_MODE tcp_mode;
		int64_t close_count = 10000 * (PartitionStatistics::my_machine_id() + 1);

		int base_portnum;

		atom* client_socket_lock;
		atom* client_socket_for_server_lock;
        atom socket_lock;
		atom lock1;
		atom lock2;
        bool set_timeo = true;

    public:
        static int connection_count;
        static int total_fail_count;
        static std::atomic<int64_t> accum_bytes_sent;
        static std::atomic<int64_t> accum_bytes_received;
		static char** hostname_list;
};
