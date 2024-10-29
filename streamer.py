# do not import anything else from loss_socket besides LossyUDP
from lossy_socket import LossyUDP
# do not import anything else from socket except INADDR_ANY
from socket import INADDR_ANY
import struct
import time
from concurrent.futures import ThreadPoolExecutor

class Streamer:
    MAX = 1472
    HEADER = '!II'
    MAX_PAYLOAD = MAX - struct.calcsize(HEADER)
    
    seq_num = 0
    def __init__(self, dst_ip, dst_port,
                 src_ip=INADDR_ANY, src_port=0):
        """Default values listen on all network interfaces, chooses a random source port,
           and does not introduce any simulated packet loss."""
        self.socket = LossyUDP()
        self.socket.bind((src_ip, src_port))
        self.dst_ip = dst_ip
        self.dst_port = dst_port
        self.recv_buff = {}
        self.seq_expected = 0
        self.ack_num = 0

        self.closed = False
        self.ack = False
        
        executor = ThreadPoolExecutor(max_workers=1)
        executor.submit(self.listener)

    def send(self, data_bytes: bytes) -> None:
        """Note that data_bytes can be larger than one packet."""
        # Your code goes here!  The code below should be changed!
        
        bytes_size = len(data_bytes)
        
        for i in range(0, bytes_size, self.MAX_PAYLOAD):
            last_index = min(i + self.MAX_PAYLOAD, bytes_size)
            chunk = data_bytes[i:last_index] #w
            
            header = struct.pack(self.HEADER, self.seq_num, 1)
            packet = header + chunk
            
            self.ack = False
            self.ack_num = self.seq_num
            
            self.socket.sendto(packet, (self.dst_ip, self.dst_port))
            self.seq_num = self.seq_num + 1

            #for now I'm just sending the raw application-level data in one UDP payload
        #self.socket.sendto(data_bytes, (self.dst_ip, self.dst_port))
            while not self.ack:
                time.sleep(.01)


    def listener(self):
        while not self.closed:
            try:
                data, addr = self.socket.recvfrom()
                
                if len(data) < struct.calcsize(self.HEADER):
                    print("Received an incomplete packet; skipping.")
                    continue  # Skip this iteration if the packet is too small

                seq_num, flag = struct.unpack(self.HEADER, data[:min(struct.calcsize(self.HEADER), len(data))])
                
                if flag == 0:
                    print("ack received")
                    self.ack = True
                    continue
                print("data received")
                payload = data[struct.calcsize(self.HEADER):]
                self.recv_buff[seq_num] = payload
                
                ack_header = struct.pack(self.HEADER, seq_num, 0)
                self.socket.sendto(ack_header, (self.dst_ip, self.dst_port))

            except Exception as e:
                print("listener died!")
                print(e)


    def recv(self) -> bytes:
        """Blocks (waits) if no data is ready to be read from the connection."""
        # your code goes here!  The code below should be changed!
        
        # this sample code just calls the recvfrom method on the LossySocket
        ordered = b''
        
        while True:
            if self.seq_expected in self.recv_buff:
                ret = self.recv_buff[self.seq_expected]
                del self.recv_buff[self.seq_expected]
                self.seq_expected = self.seq_expected + 1
                return ret
            else:
                time.sleep(.01)

    def close(self) -> None:
        """Cleans up. It should block (wait) until the Streamer is done with all
           the necessary ACKs and retransmissions"""
        # your code goes here, especially after you add ACKs and retransmissions.
        self.closed = True
        self.socket.stoprecv()
        pass
