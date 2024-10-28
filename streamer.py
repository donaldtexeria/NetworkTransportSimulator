# do not import anything else from loss_socket besides LossyUDP
from lossy_socket import LossyUDP
# do not import anything else from socket except INADDR_ANY
from socket import INADDR_ANY
import struct
import time

class Streamer:
    MAX = 1472
    HEADER = '!I'
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

    def send(self, data_bytes: bytes) -> None:
        """Note that data_bytes can be larger than one packet."""
        # Your code goes here!  The code below should be changed!
        
        bytes_size = len(data_bytes)
        
        for i in range(0, bytes_size, self.MAX_PAYLOAD):
            last_index = min(i + self.MAX_PAYLOAD, bytes_size)
            chunk = data_bytes[i:last_index] #w
            
            header = struct.pack(self.HEADER, self.seq_num)
            packet = header + chunk
            
            self.socket.sendto(packet, (self.dst_ip, self.dst_port))
            self.seq_num = self.seq_num + 1

            #for now I'm just sending the raw application-level data in one UDP payload
        #self.socket.sendto(data_bytes, (self.dst_ip, self.dst_port))

    def recv(self) -> bytes:
        """Blocks (waits) if no data is ready to be read from the connection."""
        # your code goes here!  The code below should be changed!
        
        # this sample code just calls the recvfrom method on the LossySocket
        ordered = b''
        if self.seq_expected in self.recv_buff:
                ret = self.recv_buff[self.seq_expected]
                del self.recv_buff[self.seq_expected]
                self.seq_expected = self.seq_expected + 1
                return ret
        while True:
            data, addr = self.socket.recvfrom()
            
            seq_num = struct.unpack(self.HEADER, data[:struct.calcsize(self.HEADER)])[0]
            payload = data[struct.calcsize(self.HEADER):]

            self.recv_buff[seq_num] = payload
            
            if self.seq_expected in self.recv_buff:
                ret = self.recv_buff[self.seq_expected]
                del self.recv_buff[self.seq_expected]
                self.seq_expected = self.seq_expected + 1
                break
            
            ordered += payload
        # For now, I'll just pass the full UDP payload to the app
        return ret

    def close(self) -> None:
        """Cleans up. It should block (wait) until the Streamer is done with all
           the necessary ACKs and retransmissions"""
        # your code goes here, especially after you add ACKs and retransmissions.
        pass
