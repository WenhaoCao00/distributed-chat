class VectorClock:
    def __init__(self, process_id, num_processes):
        self.process_id = process_id
        self.num_processes = num_processes
        self.time_vector = [0] * num_processes

    def local_event(self):
        self.time_vector[self.process_id] += 1
        return self.time_vector.copy()

    def send_event(self):
        self.time_vector[self.process_id] += 1
        return self.time_vector.copy()

    def receive_event(self, received_vector):
        for i in range(self.num_processes):
            self.time_vector[i] = max(self.time_vector[i], received_vector[i])
        self.time_vector[self.process_id] += 1
        return self.time_vector.copy()

    def get_time(self):
        return self.time_vector.copy()

# # 示例用法
# if __name__ == "__main__":
#     num_processes = 3
#     vc1 = VectorClock(0, num_processes)
#     vc2 = VectorClock(1, num_processes)
#     vc3 = VectorClock(2, num_processes)

#     print("Initial state:")
#     print(f"vc1: {vc1.get_time()}")
#     print(f"vc2: {vc2.get_time()}")
#     print(f"vc3: {vc3.get_time()}")

#     # Simulate events
#     print("\nvc1 local event:")
#     vc1.local_event()
#     print(f"vc1: {vc1.get_time()}")

#     print("\nvc1 sends event to vc2:")
#     send_vector = vc1.send_event()
#     vc2.receive_event(send_vector)
#     print(f"vc1: {vc1.get_time()}")
#     print(f"vc2: {vc2.get_time()}")

#     print("\nvc2 sends event to vc3:")
#     send_vector = vc2.send_event()
#     vc3.receive_event(send_vector)
#     print(f"vc2: {vc2.get_time()}")
#     print(f"vc3: {vc3.get_time()}")
