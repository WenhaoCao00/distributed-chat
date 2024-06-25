import socket
import threading
import time
from service_discovery import ServiceDiscovery

class ChatClient:

    def __init__(self):
        self.discovery = ServiceDiscovery(role='client')  # 修改：指定角色为client
        self.server_port = 10000
        self.client_socket = None
        self.leader_ip = None
        self.is_connected = False


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
                message = input("Enter message: ")
                if message.lower() == "exit":
                    self.is_connected = False
                    break
                self.client_socket.sendall(message.encode())
            except Exception as e:
                print(f"Send message error: {e}")
                self.is_connected = False

    def receive_messages(self):
        while self.is_connected:
            try:
                data = self.client_socket.recv(1024).decode()
                if data:
                    print(f"Received message: {data}")
            except Exception as e:
                print(f"Receive message error: {e}")
                self.is_connected = False

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
