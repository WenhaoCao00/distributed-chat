import socket
import threading
import time
import json
from service_discovery import ServiceDiscovery
# from election import initiate_election
# from multicast import multicast_message, receive_multicast

class Server:
    def __init__(self, port=10000):
        self.port = port
        self.local_ip = self.get_local_ip()
        self.discovery = ServiceDiscovery(port=50000)
        self.clients = {}
        self.is_leader = False
        self.leader_ip = None
        self.server_running = True
        self.last_heartbeat = {}
        print(f"Server initialized with IP: {self.local_ip} and port: {self.port}")

    def get_local_ip(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
        except Exception:
            ip = socket.gethostbyname(socket.gethostname())
        finally:
            s.close()
        return ip

    def start(self):
        print("Starting service discovery...")
        self.discovery.start()
        print("Service discovery started.")
        threading.Thread(target=self.heartbeat, daemon=True).start()
        threading.Thread(target=self.listen_for_heartbeats, daemon=True).start()  # 新增心跳监听线程
        threading.Thread(target=self.check_heartbeat, daemon=True).start()
        threading.Thread(target=self.listen_for_clients, daemon=True).start()
        print("Server started and listening for clients.")

    def listen_for_clients(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((self.local_ip, self.port))
        server_socket.listen(5)
        print(f"Server started on {self.local_ip}:{self.port}")

        while self.server_running:
            try:
                client_socket, client_address = server_socket.accept()
                print(f"Client connected: {client_address}")
                threading.Thread(target=self.handle_client, args=(client_socket, client_address), daemon=True).start()
            except Exception as e:
                print(f"Error accepting clients: {e}")

    def handle_client(self, client_socket, client_address):
        self.clients[client_address] = client_socket
        try:
            while True:
                data = client_socket.recv(1024)
                if not data:
                    break
                print(f"Received message from {client_address}: {data.decode()}")
                # Commenting out multicast logic for testing purposes
                # multicast_message(data.decode(), self.clients)
        except Exception as e:
            print(f"Error handling client {client_address}: {e}")
        finally:
            client_socket.close()
            del self.clients[client_address]
            print(f"Client disconnected: {client_address}")

    def heartbeat(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        while self.server_running:
            message = json.dumps({'type': 'heartbeat', 'ip': self.local_ip}).encode()
            for server_ip in self.discovery.get_servers():
                if server_ip != self.local_ip:
                    sock.sendto(message, (server_ip, 50000))
            time.sleep(5)

    def listen_for_heartbeats(self):  # 新增心跳监听方法
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind((self.local_ip, 50000))
        while self.server_running:
            data, addr = sock.recvfrom(1024)
            message = json.loads(data.decode())
            if message['type'] == 'heartbeat':
                self.last_heartbeat[addr[0]] = time.time()
                print(f"Received heartbeat from {addr[0]}")

    def check_heartbeat(self):
        while self.server_running:
            current_time = time.time()
            for server_ip in list(self.last_heartbeat.keys()):
                if current_time - self.last_heartbeat[server_ip] > 10:
                    print(f"Server {server_ip} is down, initiating election.")
                    # Commenting out election logic for testing purposes
                    # self.start_election()
                    # 打印检测到服务器宕机的信息
                    print(f"Detected server failure: {server_ip}")
                    break
            time.sleep(5)

    # Commenting out the start_election function for testing purposes
    # def start_election(self):
    #     servers = self.discovery.get_servers()
    #     new_leader = initiate_election(servers, self.local_ip)
    #     if new_leader == self.local_ip:
    #         self.is_leader = True
    #         self.leader_ip = self.local_ip
    #         print("I am the new leader.")
    #     else:
    #         self.is_leader = False
    #         self.leader_ip = new_leader
    #         print(f"New leader is {self.leader_ip}")

if __name__ == '__main__':
    server = Server(port=10000)
    server.start()
    while True:
        time.sleep(1)
