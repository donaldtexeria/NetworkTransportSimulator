# do not import anything else from loss_socket besides LossyUDP
from lossy_socket import LossyUDP
# do not import anything else from socket except INADDR_ANY
from socket import INADDR_ANY
import struct
import time
from concurrent.futures import ThreadPoolExecutor
import hashlib
import threading

class Streamer:
    MAX = 1472
    HEADER = '!II16s'
    MAX_PAYLOAD = MAX - struct.calcsize(HEADER)
    ACK_TIMEOUT = .25
    
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
        self.final_ack_val = 0
        self.all_data_acked = False
        self.fin_ack_recvd = False
        self.fin_recvd = False

        self.closed = False
        self.ack = -1
        self.lock = threading.Lock()

        self.received_seqnums = set()
        
        executor = ThreadPoolExecutor(max_workers=1)
        executor.submit(self.listener)

    def send(self, data_bytes: bytes) -> None:
        """Note that data_bytes can be larger than one packet."""
        # Your code goes here!  The code below should be changed!
        
        bytes_size = len(data_bytes)
        
        for i in range(0, bytes_size, self.MAX_PAYLOAD):
            md5 = hashlib.md5()
            last_index = min(i + self.MAX_PAYLOAD, bytes_size)
            chunk = data_bytes[i:last_index] #w
            #hashvalue = md5(chunk).digest()
            
            #md5.update(chunk)

            
            with self.lock:
                header = struct.pack(self.HEADER, self.seq_num, 1, b'0')
                md5.update(header)
                md5.update(chunk)
                hashvalue = md5.digest()

                header = struct.pack(self.HEADER, self.seq_num, 1, hashvalue)

                packet = header + chunk
                self.ack = -1
                self.final_ack_val = self.seq_num
                self.seq_num = self.seq_num + 1

            if self.closed:
                return
            self.socket.sendto(packet, (self.dst_ip, self.dst_port))

            
            

            #sends packets one by one, waiting for ACK before sending the next one
            time_start = time.time()
            while self.ack != self.seq_num - 1:
               time.sleep(.01)
               if self.closed:
                   return
               if time.time() - time_start >= self.ACK_TIMEOUT:
                   if self.closed:
                       return
                   self.socket.sendto(packet, (self.dst_ip, self.dst_port))
                   time_start = time.time()


    def listener(self):
        while not self.closed:
            try:
                if self.closed:
                    return
                data, addr = self.socket.recvfrom()
                
                if len(data) < struct.calcsize(self.HEADER):
                    continue

                seq_num, flag, hash_recvd = struct.unpack(self.HEADER, data[:min(struct.calcsize(self.HEADER), len(data))])
                md5 = hashlib.md5()
                header = struct.pack(self.HEADER, seq_num, flag, b'0')
                payload = data[struct.calcsize(self.HEADER):]
                md5.update(header)
                md5.update(payload)
                packet_hash = md5.digest()
                if packet_hash != hash_recvd and flag == 1:
                    #print("stuck in continue")
                    continue

                if flag == 0:
                    #print("ack received:", seq_num)
                    self.ack = seq_num
                    if seq_num == self.final_ack_val:
                        #print("all data ACKed")
                        self.all_data_acked = True
                    continue
                elif flag == 2:
                    #the other side is done sending
                    #print("fin received", seq_num)
                    self.fin_recvd = True

                    # acknowledging the other side is done
                    fin_ack_header = struct.pack(self.HEADER, seq_num, 3, b'\x00' * 16)
                    self.socket.sendto(fin_ack_header, (self.dst_ip, self.dst_port)) #___ 8001 or #____ 8002
                    continue
                elif flag == 3:

                    self.fin_ack_recvd = True
                    continue
                #print("data received")
                payload = data[struct.calcsize(self.HEADER):]
                
                if seq_num not in self.received_seqnums:
                    self.recv_buff[seq_num] = payload
                    self.received_seqnums.add(seq_num)
                
                ack_header = struct.pack(self.HEADER, seq_num, 0, b'\x00' * 16)
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
                if self.seq_expected == 339:
                    print(f"butterfly {ret}")
                del self.recv_buff[self.seq_expected]
                with self.lock:
                    self.seq_expected = self.seq_expected + 1
                return ret
            else:
                time.sleep(.01)

    def close(self) -> None:
        """Cleans up. It should block (wait) until the Streamer is done with all
           the necessary ACKs and retransmissions"""
        # your code goes here, especially after you add ACKs and retransmissions.
        
        fin_pack = struct.pack(self.HEADER, self.seq_num, 2, b'\x00' * 16)
        self.socket.sendto(fin_pack, (self.dst_ip, self.dst_port))
        
        time_start = time.time()
        while not self.fin_ack_recvd:
            curr_time = time.time() - time_start
            time.sleep(.01)
            if time.time() - time_start >= self.ACK_TIMEOUT:
                print('mama')
                self.socket.sendto(fin_pack, (self.dst_ip, self.dst_port))
                time_start = time.time()

        while not self.fin_recvd:
            time.sleep(0.01)    
                    
        time.sleep(2)
        
        self.closed = True
        self.socket.stoprecv()