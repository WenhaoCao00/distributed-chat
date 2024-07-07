import socket
import threading
import time
import json
from collections import defaultdict

class ServiceDiscovery:
    def __init__(self, role, broadcast_port=50000, heartbeat_port=50001, heartbeat_interval=5, heartbeat_timeout=10):
        self.role = role
        self.broadcast_port = broadcast_port
        self.heartbeat_port = heartbeat_port
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_timeout = heartbeat_timeout
        self.server_addresses = set()
        self.local_ip = self.get_local_ip()
        self.last_heartbeat = {}
        self.is_leader = False
        self.leader_ip = None
        self.heartbeat_running = False  # 控制心跳线程的运行
        self.vector_clock = defaultdict(int)  # Initialize vector clock

    def is_valid_ip(self, ip):
        return ip.startswith("192.168.") and ip != "127.0.0.1"  # 返回局域网IP并排除本地回环地址

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
        if self.role == 'server':
            threading.Thread(target=self.send_broadcast, daemon=True).start()
            threading.Thread(target=self.listen_for_broadcast, daemon=True).start()
            threading.Thread(target=self.listen_for_heartbeats, daemon=True).start()
            threading.Thread(target=self.check_heartbeat, daemon=True).start()
            threading.Thread(target=self.initial_heartbeat_check, daemon=True).start()
        elif self.role == 'client':
            threading.Thread(target=self.listen_for_heartbeats, daemon=True).start()

    def send_broadcast(self):
        message = f'SERVICE_DISCOVERY:{self.role}'.encode()
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.bind((self.local_ip, 0))
        while True:
            sock.sendto(message, ('<broadcast>', self.broadcast_port))
            time.sleep(5)

    def listen_for_broadcast(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(('', self.broadcast_port))
        while True:
            data, addr = sock.recvfrom(1024)
            try:
                message = data.decode()
                msg_type, role = message.split(':')
                if msg_type == 'SERVICE_DISCOVERY' and role == 'server' and self.is_valid_ip(addr[0]):
                    if addr[0] not in self.server_addresses:
                        self.server_addresses.add(addr[0])
                        self.vector_clock[addr[0]] = 0  # Add new server to vector clock
                        print(f"Discovered server: {addr[0]}")
                        if self.is_leader:
                            self.notify_new_server(addr[0])
                    if not self.leader_ip:
                        self.start_election()
            except Exception as e:
                print(f"Error decoding broadcast message: {e}")

    def start_heartbeat(self):
        if not self.heartbeat_running:
            self.heartbeat_running = True
            threading.Thread(target=self.heartbeat, daemon=True).start()

    def stop_heartbeat(self):
        self.heartbeat_running = False

    def heartbeat(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)  # 启用广播
        sock.bind((self.local_ip, 0))
        while self.heartbeat_running:
            message = json.dumps({
                'type': 'heartbeat',
                'ip': self.local_ip,
                'leader': self.leader_ip,
                'vector_clock': self.vector_clock
            }).encode()
            # 广播心跳消息给整个子网
            sock.sendto(message, ('<broadcast>', self.heartbeat_port))
            time.sleep(self.heartbeat_interval)

    def listen_for_heartbeats(self):
        print("Starting to listen for heartbeats...")
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(('', self.heartbeat_port))
        
        while True:
            data, addr = sock.recvfrom(1024)
            if data:
                try:
                    message = json.loads(data.decode())
                    if message['type'] == 'heartbeat':
                        self.last_heartbeat[addr[0]] = time.time()
                        self.leader_ip = message['leader']
                        if self.role == 'server':
                            if self.leader_ip != self.local_ip:
                                self.is_leader = False
                                self.stop_heartbeat()
                        elif self.role == 'client':
                            self.leader_ip = message['leader']
                        # Update vector clock with received data
                        for ip, timestamp in message['vector_clock'].items():
                            if ip not in self.vector_clock or self.vector_clock[ip] < timestamp:
                                self.vector_clock[ip] = timestamp
                    elif message['type'] == 'new_leader':
                        self.leader_ip = message['leader']
                        self.is_leader = (self.leader_ip == self.local_ip)
                        if self.role == 'server' and not self.is_leader:
                            self.stop_heartbeat()
                        if self.role == 'client':
                            self.leader_ip = message['leader']
                except json.JSONDecodeError:
                    pass

    def check_heartbeat(self):
        while True:
            current_time = time.time()
            for server_ip in list(self.last_heartbeat.keys()):
                if current_time - self.last_heartbeat[server_ip] > self.heartbeat_timeout:
                    print(f"Server {server_ip} is down, initiating election.")
                    self.server_addresses.remove(server_ip)
                    del self.last_heartbeat[server_ip]
                    if server_ip in self.vector_clock:
                        del self.vector_clock[server_ip]  # Remove from vector clock
                    self.start_election()
            time.sleep(5)
    
    def initial_heartbeat_check(self):
        # 等待一个心跳周期以收集其他服务器信息
        time.sleep(self.heartbeat_interval + 1)
        if len(self.server_addresses) == 1:
            print(f"I am the leader: {self.local_ip}")
            self.is_leader = True
            self.start_heartbeat()
        elif len(self.server_addresses) > 1:
            self.start_election()

    def start_election(self):
        if len(self.server_addresses) == 1 and self.local_ip in self.server_addresses:
            print(f"Only one server in the network, I am the leader: {self.local_ip}")
            self.is_leader = True
            self.start_heartbeat()
        else:
            print("Starting election...")
            self.leader_ip = min(self.server_addresses.union({self.local_ip}), key=lambda ip: tuple(map(int, ip.split('.'))))
            print(f"Elected leader: {self.leader_ip}")
            print(f"Server addresses: {self.server_addresses.union({self.local_ip})}")
            if self.leader_ip == self.local_ip:
                self.is_leader = True
                self.start_heartbeat()
                print(f"I am the leader: {self.local_ip}")
            else:
                self.is_leader = False
                self.stop_heartbeat()
                print(f"New leader is {self.leader_ip}")
            self.notify_new_leader()

    def notify_new_leader(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        message = json.dumps({
            'type': 'new_leader',
            'leader': self.leader_ip
        }).encode()
        for server_ip in list(self.server_addresses):
            if server_ip != self.local_ip:
                sock.sendto(message, (server_ip, self.heartbeat_port))
                print(f"Notified {server_ip} of new leader {self.leader_ip}")

    def notify_new_server(self, new_server_ip):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        message = json.dumps({
            'type': 'new_server',
            'vector_clock': self.vector_clock
        }).encode()
        sock.sendto(message, (new_server_ip, self.heartbeat_port))
        print(f"Notified new server {new_server_ip} of current vector clock")

    def get_leader(self):
        return self.leader_ip

    def get_servers(self):
        return list(self.server_addresses)
