import socket
import threading
import time

class ServiceDiscovery:
    def __init__(self, port=50000, broadcast_ip="255.255.255.255"):
        self.port = port
        self.broadcast_ip = broadcast_ip
        self.server_addresses = set()
        self.local_ip = self.get_local_ip()

    def is_valid_ip(self, ip):
        # 仅过滤局域网IP并排除本地回环地址
        return ip.startswith("192.168.0.") and ip != "127.0.0.1"

    def get_local_ip(self):
        # 获取本机IP地址
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
        except Exception:
            ip = socket.gethostbyname(socket.gethostname())
        finally:
            s.close()
        return ip

    def send_broadcast(self):
        # 广播服务存在消息
        message = b'SERVICE_DISCOVERY'
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.bind((self.local_ip, 0))
        while True:
            sock.sendto(message, (self.broadcast_ip, self.port))
            time.sleep(5)

    def listen_for_broadcast(self):
        # 监听广播消息以发现新服务器
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(('', self.port))
        while True:
            data, addr = sock.recvfrom(1024)
            if data == b'SERVICE_DISCOVERY' and self.is_valid_ip(addr[0]) and addr[0] not in self.server_addresses:
                self.server_addresses.add(addr[0])
                print(f"Discovered server: {addr[0]}")

    def start(self):
        # 启动广播和监听线程
        threading.Thread(target=self.send_broadcast, daemon=True).start()
        threading.Thread(target=self.listen_for_broadcast, daemon=True).start()

    def get_servers(self):
        # 返回当前发现的服务器地址列表
        return list(self.server_addresses)

# 示例用法：
if __name__ == '__main__':
    discovery = ServiceDiscovery()
    discovery.start()
    time.sleep(10)  # 等待一些时间以发现服务器
    print("Discovered servers:", discovery.get_servers())
