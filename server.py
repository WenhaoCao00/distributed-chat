import socket
import threading
import time
from service_discovery import ServiceDiscovery

class Server:
    def __init__(self, port=10000):
        self.port = port
        self.discovery = ServiceDiscovery(role='server')
        self.clients = {}
        self.server_running = True

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
        print(f"Server started on {self.discovery.local_ip}:{self.port}")

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
                message = data.decode()
                print(f"{client_address}: {message}")
                
                # Forward message to all other clients
                self.forward_message(client_address, message)

        except Exception as e:
            print(f"Error handling client {client_address}: {e}")
        finally:
            client_socket.close()
            del self.clients[client_address]
            print(f"Client disconnected: {client_address}")

    def forward_message(self, sender_address, message):
        for client_address, client_socket in self.clients.items():
            if client_address != sender_address:
                try:
                    client_socket.sendall(f"{sender_address}: {message}".encode())
                except Exception as e:
                    print(f"Error forwarding message to {client_address}: {e}")

if __name__ == '__main__':
    server = Server(port=10000)
    server.start()
    while True:
        time.sleep(1)
