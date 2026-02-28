from __future__ import annotations

import fcntl
import random
import socket
import struct
import threading
from collections.abc import Callable

import netifaces

from utils import msg
from proto import UnitDefinition_pb2 as UnitDefinition
from proto import WorkResponse_pb2 as WorkResponse
from proto.UnitDefinition_pb2 import TcpPackageType


class TCPClient:
    def __init__(self, **kwargs) -> None:
        self.active_connection: bool = False
        self.sock: socket.socket | None = None
        self.receive_worker: threading.Thread | None = None
        self.unit_type = kwargs["unit_type"]
        self.unit_info = kwargs["unit_info"]
        self.pretty_name = kwargs.get("name", "Python Client")
        self.callbacks: dict[TcpPackageType, Callable[[msg.TCPMessage], None]] = {}
        self.callbacks[TcpPackageType.TEXT] = self.print_text_message
        self.callbacks[TcpPackageType.UPDATE_UNIT_TYPE] = self.send_unit_update
        self.callbacks[TcpPackageType.TASK_FINISHED] = self.ack_task_finished
        self.uuid: int = 0
        self.connection_up: bool = False
        self.connect_condition = threading.Condition()
        self.send_lock = threading.Lock()
        self.verbose: bool = kwargs.get("verbose", False)

    def __is_interface_up(self, interface: str) -> bool:
        addr = netifaces.ifaddresses(interface)
        return netifaces.AF_INET in addr

    def __get_mac_for_connected(self) -> bytes:
        """Fetch MAC address of the first active non-docker interface.

        Raises RuntimeError if no suitable interface is found.
        """
        ifnames = socket.if_nameindex()
        for ifname in ifnames:
            ifn = ifname[1]
            if "docker" in ifn:
                continue
            if not self.__is_interface_up(ifn):
                continue
            # 0x8927 == SIOCGIFHWADDR
            info = fcntl.ioctl(
                self.sock.fileno(),
                0x8927,
                struct.pack("256s", bytes(ifn[:15], "utf-8")),
            )
            intval = int.from_bytes(info[18:24], byteorder="big")
            if intval > 0:
                return info[18:24]
        raise RuntimeError("No suitable network interface found for MAC address retrieval")

    def __generate_uuid(self) -> int:
        mac_bytes = self.__get_mac_for_connected()
        rand_bytes = random.getrandbits(16).to_bytes(2, byteorder="big")
        # alternatively just return random.getrandbits(64)
        # However, 6 Bytes MAC allows for debugging!
        return int.from_bytes((mac_bytes+rand_bytes), byteorder="big")

    def __notify_connection_up(self):
        with self.connect_condition:
            self.connection_up = True
            self.connect_condition.notify_all()

    def register_callback(
        self,
        package_type: TcpPackageType,
        func: Callable[[msg.TCPMessage], None],
    ):
        if self.verbose:
            print(f"[TCPClient] Registered new callback for PackageType {package_type}")
        self.callbacks[package_type] = func

    def print_text_message(self, message: msg.TCPMessage):
        text = str(message.payload.decode())
        print(f"Received Text: {text}")

    def wait_until_connected(self):
        with self.connect_condition:
            while not self.connection_up:
                self.connect_condition.wait()

    def send_unit_update(self, _message: msg.TCPMessage):
        outgoing = msg.TCPMessage(unit_type=self.unit_type, package_type=TcpPackageType.UPDATE_UNIT_TYPE)
        ud = UnitDefinition.UnitDefinition()
        ud.unit_type = self.unit_type
        ud.info = self.unit_info
        ud.prettyName = self.pretty_name
        outgoing.payload = ud.SerializeToString()
        self.send_message(outgoing)
        self.__notify_connection_up()

    def ack_task_finished(self, message: msg.TCPMessage):
        response = WorkResponse.WorkResponse()
        response.ParseFromString(message.payload)
        if self.verbose:
            print(f"Task Finished. Response from <{message.src_uuid}>:\n{response}")

    def connect(self, ip: str, port: int):
        if self.active_connection:
            print("[TCPClient] Error. Connection already established. Call ignored.")
            return

        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.uuid = self.__generate_uuid()
            self.sock.connect((ip, port))
            self.active_connection = True
            self.receive_worker = threading.Thread(target=self.__receive_message, daemon=True)
            self.receive_worker.start()
            self.wait_until_connected()
        except Exception as e:
            print(f"Error connecting to server: {e}")
            # Clean up the socket if connect failed
            if self.sock:
                self.sock.close()
                self.sock = None
            self.active_connection = False
            raise

    def disconnect(self):
        if not self.active_connection:
            return

        self.active_connection = False

        # will see the error or EOF and exit.
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass  # Already closed or not connected

        try:
            self.sock.close()
        except OSError:
            pass

        if self.receive_worker is not None:
            self.receive_worker.join(timeout=5.0)
        self.sock = None

    def send_message(self, package: msg.TCPMessage):
        if not self.active_connection:
            print("[TCPClient] Error. No active connection. Data was NOT sent.")
            return

        package.src_uuid = self.uuid
        data = bytes(package)

        with self.send_lock:
            try:
                self.sock.sendall(data)
            except Exception as e:
                print(f"[TCPClient] Sending failed: {e}")

    def __receive_message(self):
        buffer_size = 1024 * 64  # 64 KB
        data = bytearray()

        if self.verbose:
            print(f"[TCPClient] Initialized receive buffer with {buffer_size} bytes.")

        while self.active_connection:
            # another thread doesn't crash the receive loop
            try:
                chunk = self.sock.recv(buffer_size)
            except OSError:
                if not self.active_connection:
                    break  # Expected during disconnect
                print("[TCPClient] Socket error during receive.")
                break

            if not chunk:
                print("[TCPClient] Receive reached EOF. Socket closed? Terminating Receive.")
                self.connection_up = False
                break

            data += chunk

            processed_bytes = 0
            for message in msg.TCPMessage.parse_from_bytes(data):
                if message.complete:
                    processed_bytes += message.size
                    if self.verbose:
                        print(f"[TCPClient] received PackageType: {message.package_type}")
                    if message.package_type in self.callbacks:
                        self.callbacks[message.package_type](message)
                    else:
                        print(f"[TCPClient] WARNING: No callback for PackageType {message.package_type}")
                else:
                    break

            data = data[processed_bytes:]

        print("[TCPClient] Receive terminated.")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.disconnect()