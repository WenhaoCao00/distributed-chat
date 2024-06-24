import socket
import threading
import time
import json

class ServiceDiscovery:
    def __init__(self, broadcast_port=50000, heartbeat_port=50001, heartbeat_interval=5, heartbeat_timeout=10):
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
        threading.Thread(target=self.send_broadcast, daemon=True).start()
        threading.Thread(target=self.listen_for_broadcast, daemon=True).start()
        threading.Thread(target=self.listen_for_heartbeats, daemon=True).start()
        threading.Thread(target=self.check_heartbeat, daemon=True).start()

    def send_broadcast(self):
        message = b'SERVICE_DISCOVERY'
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
            if data == b'SERVICE_DISCOVERY' and self.is_valid_ip(addr[0]):
                if addr[0] not in self.server_addresses:
                    self.server_addresses.add(addr[0])
                    print(f"Discovered server: {addr[0]}")
                if not self.leader_ip:
                    self.start_election()

    def start_heartbeat(self):
        if not self.heartbeat_running:
            self.heartbeat_running = True
            threading.Thread(target=self.heartbeat, daemon=True).start()

    def stop_heartbeat(self):
        self.heartbeat_running = False

    def heartbeat(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((self.local_ip, 0))
        while self.heartbeat_running:
            message = json.dumps({
                'type': 'heartbeat',
                'ip': self.local_ip,
                'leader': self.leader_ip
            }).encode()
            for server_ip in list(self.server_addresses):  # 使用集合的副本进行遍历
                if server_ip != self.local_ip:
                    print(f"Sending heartbeat to {server_ip}")
                    sock.sendto(message, (server_ip, self.heartbeat_port))
            time.sleep(self.heartbeat_interval)

    def listen_for_heartbeats(self):
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
                        if self.leader_ip != self.local_ip:
                            self.is_leader = False
                            self.stop_heartbeat()
                        print(f"Received heartbeat from {addr[0]} with leader {self.leader_ip}")
                    elif message['type'] == 'new_leader':
                        self.leader_ip = message['leader']
                        self.is_leader = (self.leader_ip == self.local_ip)
                        if not self.is_leader:
                            self.stop_heartbeat()
                        print(f"Received new leader notification: {self.leader_ip}")
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
                    self.start_election()
            time.sleep(5)

    def start_election(self):
        print("Starting election...")
        self.leader_ip = min(self.server_addresses.union({self.local_ip}))
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
        for server_ip in self.server_addresses:
            if server_ip != self.local_ip:
                sock.sendto(message, (server_ip, self.heartbeat_port))
                print(f"Notified {server_ip} of new leader {self.leader_ip}")

    def get_leader(self):
        return self.leader_ip

    def get_servers(self):
        return list(self.server_addresses)

# 示例用法：
if __name__ == '__main__':
    discovery = ServiceDiscovery()
    discovery.start()
    time.sleep(10)  # 等待一些时间以发现服务器
    print("Discovered servers:", discovery.get_servers())
    print("Leader:", discovery.get_leader())
