import socket
import threading
import json
import time
import uuid
from storage import ChatStorage


class DistributedChatNode:
    def __init__(self, node_id, host, port, peers):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.peers = peers

        self.running = True
        self.seen_messages = set()

        # Semana 4–6
        self.last_seen = {}
        self.heartbeat_timeout = 5

        # Semana 5
        self.in_election = False
        self.leader_id = None

        # Semana 3
        self.storage = ChatStorage(node_id)
        history = self.storage.get_history()
        print(f"[{self.node_id}] Base de datos cargada: {len(history)} mensajes previos.")

    # ---------------- SERVIDOR ----------------
    def start_server(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.host, self.port))
        server_socket.listen(5)

        while self.running:
            try:
                client_sock, _ = server_socket.accept()
                threading.Thread(
                    target=self.handle_client,
                    args=(client_sock,),
                    daemon=True
                ).start()
            except:
                pass

    # ---------------- CLIENTE ----------------
    def handle_client(self, client_socket):
        try:
            data = client_socket.recv(1024).decode('utf-8')
            message = json.loads(data)

            # Semana 4: heartbeat
            if message.get("type") == "heartbeat":
                self.last_seen[message["from"]] = time.time()
                return

            # Semana 5: election
            if message.get("type") == "election":
                sender = message["from"]
                print(f"[{self.node_id}] Elección recibida de {sender}")

                reply = {"type": "ok", "from": self.node_id}
                peer = next(p for p in self.peers if p["id"] == sender)
                self.send_direct_message(peer["host"], peer["port"], reply)

                if not self.in_election:
                    self.start_election()
                return

            if message.get("type") == "ok":
                self.in_election = False
                return

            if message.get("type") == "leader":
                self.leader_id = message["from"]
                self.in_election = False
                print(f"[{self.node_id}] Nuevo líder: {self.leader_id}")
                return

            # Mensaje normal
            msg_id = message.get("id")
            if msg_id in self.seen_messages:
                return

            self.seen_messages.add(msg_id)
            sender = message["from"]
            content = message["content"]

            self.storage.save_message(msg_id, sender, content)
            print(f"\n[{sender}]: {content}")

            self.broadcast_message(message, original_sender=False)

        except Exception as e:
            print("Error:", e)
        finally:
            client_socket.close()

    # ---------------- MENSAJES ----------------
    def send_direct_message(self, host, port, msg):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((host, port))
            s.send(json.dumps(msg).encode())
            s.close()
        except:
            pass

    def broadcast_message(self, msg, original_sender=True):
        if original_sender:
            msg_id = str(uuid.uuid4())
            msg["id"] = msg_id
            self.seen_messages.add(msg_id)
            self.storage.save_message(msg_id, self.node_id, msg["content"])

        for peer in self.peers:
            if peer["id"] != msg.get("from"):
                self.send_direct_message(peer["host"], peer["port"], msg)

    # ---------------- HEARTBEATS ----------------
    def send_heartbeats(self):
        while self.running:
            for peer in self.peers:
                hb = {"type": "heartbeat", "from": self.node_id}
                self.send_direct_message(peer["host"], peer["port"], hb)
            time.sleep(2)

    def monitor_peers(self):
        while self.running:
            now = time.time()
            for peer in self.peers:
                last = self.last_seen.get(peer["id"])
                if last and now - last > self.heartbeat_timeout:
                    print(f"[{self.node_id}] Nodo {peer['id']} caído")
            time.sleep(2)

    # ---------------- ELECCIÓN ----------------
    def start_election(self):
        print(f"[{self.node_id}] Iniciando elección")
        self.in_election = True

        higher = [p for p in self.peers if p["id"] > self.node_id]
        if not higher:
            self.become_leader()
            return

        msg = {"type": "election", "from": self.node_id}
        for peer in higher:
            self.send_direct_message(peer["host"], peer["port"], msg)

    def become_leader(self):
        self.leader_id = self.node_id
        self.in_election = False
        print(f"[{self.node_id}] SOY EL LÍDER")

        msg = {"type": "leader", "from": self.node_id}
        for peer in self.peers:
            self.send_direct_message(peer["host"], peer["port"], msg)

    def monitor_leader(self):
        while self.running:
            if self.leader_id:
                last = self.last_seen.get(self.leader_id)
                if last and time.time() - last > self.heartbeat_timeout:
                    print(f"[{self.node_id}] Líder caído, nueva elección")
                    self.leader_id = None
                    self.start_election()
            time.sleep(2)

    # ---------------- CLI ----------------
    def start(self):
        threading.Thread(target=self.start_server, daemon=True).start()
        threading.Thread(target=self.send_heartbeats, daemon=True).start()
        threading.Thread(target=self.monitor_peers, daemon=True).start()
        threading.Thread(target=self.monitor_leader, daemon=True).start()

        time.sleep(1)
        print(f"--- Nodo {self.node_id} Activo ---")
        print("/historial  /salir")

        while self.running:
            try:
                cmd = input(f"({self.node_id}) > ")
                if cmd == "/salir":
                    self.running = False
                elif cmd == "/historial":
                    self.show_history()
                elif cmd:
                    self.broadcast_message(
                        {"from": self.node_id, "content": cmd},
                        original_sender=True
                    )
            except KeyboardInterrupt:
                break

    def show_history(self):
        rows = self.storage.get_history()
        for r in rows:
            print(r)
