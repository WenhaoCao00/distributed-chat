import socket
import threading
import time
import uuid
import json
from service_discovery import ServiceDiscovery
from lamport_clock import LamportClock

class ChatClient:
    def __init__(self, server_port=10000):
        self.server_port = server_port
        self.service_discovery = ServiceDiscovery(port=50000)
        self.clock = LamportClock()
        self.leader_address = None
        self.client_socket = None
        self.unconfirmed_messages = {}  # 保存未确认的消息
        self.ack_received = threading.Event()

    def start(self):
        self.service_discovery.start()
        time.sleep(5)  # 等待服务发现启动并找到Leader
        self.leader_address = self.service_discovery.get_leader()
        if not self.leader_address:
            print("No leader found, exiting.")
            return
        self.create_socket()
        receiver_thread = threading.Thread(target=self.receive_messages, daemon=True)
        receiver_thread.start()
        self.send_messages()

    def create_socket(self):
        if self.client_socket:
            self.client_socket.close()
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.client_socket.bind((self.service_discovery.local_ip, 0))

    def receive_messages(self):
        while True:
            try:
                data, _ = self.client_socket.recvfrom(1024)
                message = data.decode()
                if message.startswith("SERVER_ACK"):
                    msg_id = message.split(':')[1]
                    if msg_id in self.unconfirmed_messages:
                        del self.unconfirmed_messages[msg_id]
                    self.ack_received.set()
                    print(f"Message {msg_id} confirmed by server.")
                else:
                    print(f"Received message: {message}")
            except Exception as e:
                print(f"Error receiving messages: {e}")
                break

    def send_messages(self):
        try:
            while True:
                message = input("Enter message: ")
                if message.strip().lower() == "exit":
                    print("Exiting...")
                    break
                self.clock.increment()
                message_id = str(uuid.uuid4())
                full_message = f'CLIENT:{message_id}:{self.clock.get_time()}:{message}'
                self.unconfirmed_messages[message_id] = full_message

                while message_id in self.unconfirmed_messages:
                    self.ack_received.clear()
                    self.client_socket.sendto(full_message.encode(), (self.leader_address, self.server_port))
                    if not self.ack_received.wait(5):
                        print("No ACK received, initiating re-election...")
                        self.reconnect_to_leader()
                        if not self.leader_address:
                            print("No leader found after re-election, exiting.")
                            return

        except KeyboardInterrupt:
            print("Client is closing.")
        finally:
            self.client_socket.close()

    def reconnect_to_leader(self):
        self.leader_address = None
        while not self.leader_address:
            print("Attempting to find new leader...")
            self.leader_address = self.service_discovery.get_leader()
            if not self.leader_address:
                print("No leader found, retrying...")
                time.sleep(5)

if __name__ == '__main__':
    client = ChatClient(server_port=10000)
    client.start()
