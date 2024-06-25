import socket
import threading
import time
import json
from service_discovery import ServiceDiscovery
from lamport_clock import LamportClock

class ChatClient:

    def __init__(self):
        self.discovery = ServiceDiscovery(role='client')  # 修改：指定角色为client
        self.server_port = 10000
        self.client_socket = None
        self.leader_ip = None
        self.is_connected = False
        self.clock = LamportClock()

    def connect_to_leader(self):
        while not self.is_connected:
            self.leader_ip = self.discovery.get_leader()
            if self.leader_ip:
                try:
                    self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    self.client_socket.connect((self.leader_ip, self.server_port))
                    self.is_connected = True
                    print(f"Connected to leader at {self.leader_ip}")
                except Exception as e:
                    print(f"Failed to connect to leader: {e}")
                    time.sleep(5)
            else:
                print("No leader found, retrying...")
                time.sleep(5)

    def send_messages(self):
        while self.is_connected:
            try:
                message_content = input("Me: ")
                if message_content.lower() == "exit":
                    self.is_connected = False
                    break
                timestamp = self.clock.send_event()
                message = {
                    'sender': 'client_id',
                    'content': message_content,
                    'timestamp': timestamp
                }
                self.client_socket.sendall(json.dumps(message).encode())
            except Exception as e:
                print(f"Send message error: {e}")
                self.is_connected = False
            

    def receive_messages(self):
        while self.is_connected:
            try:
                data = self.client_socket.recv(1024).decode()
                if data:
                    message = json.loads(data)
                    self.clock.receive_event(message['timestamp'])
                    self.print_message(f"{message['sender']}:{message['content']}")
                   
            except Exception as e:
                print(f"Receive message error: {e}")
                self.is_connected = False

    def print_message(self, message):
        print(f"\r{' ' * 80}\r", end='', flush=True)
        print(f"{message}")
        print("Me: ", end='', flush=True)

    def handle_leader_change(self):
        while True:
            current_leader = self.discovery.get_leader()
            if current_leader != self.leader_ip:
                print(f"Leader changed to {current_leader}")
                self.is_connected = False
                if self.client_socket:
                    self.client_socket.close()
                self.connect_to_leader()
                threading.Thread(target=self.receive_messages, daemon=True).start()
            time.sleep(5)

    def start(self):
        self.discovery.start()
        time.sleep(5)  # 等待服务发现初始化
        self.connect_to_leader()
        threading.Thread(target=self.receive_messages, daemon=True).start()
        threading.Thread(target=self.handle_leader_change, daemon=True).start()
        self.send_messages()

if __name__ == '__main__':
    client = ChatClient()
    client.start()
