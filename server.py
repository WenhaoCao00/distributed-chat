import socket
import threading
import time
import json
from service_discovery import ServiceDiscovery
from collections import defaultdict

class Server:
    def __init__(self, port=10000):
        self.port = port
        self.discovery = ServiceDiscovery(role='server')
        self.clients = {}
        self.server_running = True
        self.vector_clock = defaultdict(int)  # Initialize server vector clock
        self.message_queue = []

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
                
                # Update the server's vector clock
                self.vector_clock[self.discovery.local_ip] += 1
                print(f"Server vector clock: {self.vector_clock}")
                # Update vector clock with received message's clock
                self.update_vector_clock(message['vector_clock'])

                print(f"{client_address}: {message['content']}:{message['vector_clock']}")

                if self.is_causally_ready(message['vector_clock']):
                    self.process_message(client_address, message)
                    self.process_message_queue()
                else:
                    self.message_queue.append((client_address, message))

        except Exception as e:
            print(f"Error handling client {client_address}: {e}")
        finally:
            client_socket.close()
            del self.clients[client_address]
            print(f"Client disconnected: {client_address}")

    def update_vector_clock(self, received_clock):
        for ip, timestamp in received_clock.items():
            if ip in self.vector_clock:
                self.vector_clock[ip] = max(self.vector_clock[ip], timestamp)
            else:
                self.vector_clock[ip] = timestamp

    def is_causally_ready(self, received_clock):
        for ip, timestamp in received_clock.items():
            if ip == self.discovery.local_ip:
                if timestamp != self.vector_clock[ip]:
                    return False
            elif self.vector_clock[ip] < timestamp:
                return False
        return True

    def process_message_queue(self):
        self.message_queue.sort(key=lambda x: x[1]['vector_clock'])  # 按矢量时钟排序
        i = 0
        while i < len(self.message_queue):
            sender_address, message = self.message_queue[i]
            if self.is_causally_ready(message['vector_clock']):
                self.process_message(sender_address, message)
                self.message_queue.pop(i)
            else:
                i += 1  # 继续检查下一条消息

    def process_message(self, sender_address, message):
        self.update_vector_clock(message['vector_clock'])
        self.forward_message(sender_address, message)

    def forward_message(self, sender_address, message):
        for client_address, client_socket in self.clients.items():
            if client_address != sender_address:
                try:
                    forward_message = {
                        'sender': sender_address,
                        'content': message['content'],
                        'vector_clock': self.vector_clock.copy()
                    }
                    client_socket.sendall(json.dumps(forward_message).encode())
                except Exception as e:
                    print(f"Error forwarding message to {client_address}: {e}")

if __name__ == '__main__':
    server = Server(port=10000)
    server.start()
    while True:
        time.sleep(1)
