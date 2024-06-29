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
        self.holdback_message_queue = []
        self.delivery_message_queue = []
        self.last_variable = []
        self.incoming_variable = []

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
        self.update_last_variable(client_address, 0)  # Initialize last variable for this client
        try:
            while True:
                data = client_socket.recv(1024)
                if not data:
                    break
                message = json.loads(data.decode())
                self.clock.receive_event(message['timestamp'])
                print(f"{client_address}: {message['content']}")

                # Update incoming variable
                self.update_incoming_variable(client_address, message['timestamp'])
                
                # Process the message based on checkpoint logic
                self.checkpoint(client_address, message)
                
                # Process message queue
                self.process_message_queue()

        except Exception as e:
            print(f"Error handling client {client_address}: {e}")
        finally:
            client_socket.close()
            del self.clients[client_address]
            print(f"Client disconnected: {client_address}")

    def update_last_variable(self, client_address, timestamp):
        for i, (addr, _) in enumerate(self.last_variable):
            if addr == client_address:
                self.last_variable[i] = (client_address, timestamp)
                return
        self.last_variable.append((client_address, timestamp))

    def update_incoming_variable(self, client_address, timestamp):
        for i, (addr, _) in enumerate(self.incoming_variable):
            if addr == client_address:
                self.incoming_variable[i] = (client_address, timestamp)
                return
        self.incoming_variable.append((client_address, timestamp))

    def get_last_timestamp(self, client_address):
        for addr, timestamp in self.last_variable:
            if addr == client_address:
                return timestamp
        return None

    def checkpoint(self, client_address, message):
        incoming_time = message['timestamp']
        last_time = self.get_last_timestamp(client_address)

        if incoming_time == last_time + 1:
            # Case 1: Sequential increase
            self.delivery_message_queue.append((client_address, message))
            self.update_last_variable(client_address, incoming_time)
        elif incoming_time > last_time + 1:
            # Case 2: Jump increase
            self.holdback_message_queue.append((client_address, message))
            # Check if any messages in holdback queue can be moved to delivery queue
            self.process_holdback_queue(client_address)
        else:
            # Case 3: Duplicate or out-of-order message
            print(f"Dropping message from {client_address} with timestamp {incoming_time}")

    def process_holdback_queue(self, client_address):
        for msg in self.holdback_message_queue:
            addr, message = msg
            if addr == client_address:
                incoming_time = message['timestamp']
                last_time = self.get_last_timestamp(client_address)
                if incoming_time == last_time + 1:
                    self.holdback_message_queue.remove(msg)
                    self.delivery_message_queue.append(msg)
                    self.update_last_variable(client_address, incoming_time)
                break

    def process_message_queue(self):
        while self.delivery_message_queue:
            sender_address, message = self.delivery_message_queue.pop(0)
            self.forward_message(sender_address, message)

    def forward_message(self, sender_address, message):
        for client_address, client_socket in self.clients.items():
            if client_address != sender_address:
                try:
                    self.clock.send_event()
                    forward_message = {
                        'sender': str(sender_address),
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
