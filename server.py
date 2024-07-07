import socket
import threading
import time
import json
from service_discovery import ServiceDiscovery
from lamport_clock import LamportClock

class Server:
    def __init__(self, port=10000):
        self.port = port
        self.discovery = ServiceDiscovery(role='server')
        self.clients = {}
        self.server_running = True
        self.clock = LamportClock()
        self.message_queue = []
        self.client_clocks = {}

    def start(self):
        print("Starting service discovery...")
        self.discovery.start()
        print("Service discovery started.")
        threading.Thread(target=self.listen_for_clients, daemon=True).start()
        print("Server started and listening for clients.")

    def listen_for_clients(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((self.discovery.local_ip, self.port))
        server_socket.listen(5)
        print(f"Local server started on {self.discovery.local_ip}:{self.port}")

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
                message = json.loads(data.decode())
                self.clock.receive_event(message['timestamp'])
                #first time receive message from client
                if client_address not in self.client_clocks:
                    self.client_clocks[client_address] = int(message['timestamp']) -1
                print(f"{client_address}: {message['content']}:{message['timestamp']}:{self.client_clocks[client_address]}")
                
                
                if message['timestamp'] == self.client_clocks[client_address] + 1:
                    self.client_clocks[client_address] = message['timestamp']
                    self.forward_message(client_address, message)
                    self.process_message_queue()
                else:
                    self.message_queue.append((client_address, message))


                

        except Exception as e:
            print(f"Error handling client {client_address}: {e}")
        finally:
            client_socket.close()
            del self.clients[client_address]
            del self.client_clocks[client_address]
            print(f"Client disconnected: {client_address}")


    def process_message_queue(self):
        self.message_queue.sort(key=lambda x: x[1]['timestamp'])  # 按时间戳排序
        i = 0
        while i < len(self.message_queue):
            sender_address, message = self.message_queue[i]
            if message['timestamp'] == self.client_clocks[sender_address] + 1:
                # 更新客户端时钟
                self.client_clocks[sender_address] = message['timestamp']
                # 转发消息
                self.forward_message(sender_address, message)
                self.message_queue.pop(i)
            else:
                i += 1  # 继续检查下一条消息

    def forward_message(self, sender_address, message):
        for client_address, client_socket in self.clients.items():
            if client_address != sender_address:
                try:
                    self.clock.send_event()
                    forward_message = {
                        'sender': sender_address,
                        'content': message['content'],
                        'timestamp': self.clock.get_time()
                    }
                    client_socket.sendall(json.dumps(forward_message).encode())
                except Exception as e:
                    print(f"Error forwarding message to {client_address}: {e}")

if __name__ == '__main__':
    server = Server(port=10000)
    server.start()
    while True:
        time.sleep(1)
