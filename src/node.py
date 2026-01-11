import socket
import threading
import json
import time
import uuid
from src.storage import ChatStorage

class DistributedChatNode:
    def __init__(self, node_id, host, port, peers):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.peers = peers

        self.running = True
        self.seen_messages = set()

        # Semana 4–6: Monitoreo
        self.last_seen = {}
        self.heartbeat_timeout = 8 # Aumentado un poco para evitar falsos positivos

        # Semana 5: Elección
        self.in_election = False
        self.leader_id = None # Al inicio no sabemos quién es el líder

        # Semana 3: Persistencia
        self.storage = ChatStorage(node_id)
        history = self.storage.get_history()
        print(f"[{self.node_id}] Base de datos cargada: {len(history)} mensajes previos.")

    # ---------------- SERVIDOR (Escucha) ----------------
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

    def handle_client(self, client_socket):
        """Procesa TODOS los mensajes entrantes."""
        try:
            data = client_socket.recv(1024).decode('utf-8')
            if not data: return
            message = json.loads(data)

            msg_type = message.get("type")

            # --- SEMANA 4: HEARTBEAT ---
            if msg_type == "heartbeat":
                self.last_seen[message["from"]] = time.time()
                
                # CORRECCIÓN DE SEGURIDAD:
                # Si recibo heartbeat de alguien mayor y YO creo ser líder, debo renunciar.
                if message["from"] > self.node_id and self.leader_id == self.node_id:
                    print(f"[{self.node_id}] ¡Detecté un nodo mayor ({message['from']})! Renuncio al liderazgo.")
                    self.leader_id = None
                    self.start_election()
                return

            # --- SEMANA 5: ELECCIÓN ---
            if msg_type == "election":
                sender = message["from"]
                # Responder OK
                reply = {"type": "ok", "from": self.node_id}
                peer = next((p for p in self.peers if p["id"] == sender), None)
                if peer: self.send_direct_message(peer["host"], peer["port"], reply)
                
                # Iniciar mi propia elección si no estoy en una
                if not self.in_election:
                    self.start_election()
                return

            if msg_type == "ok":
                # Alguien mayor respondió, me retiro
                self.in_election = False
                return

            if msg_type == "leader":
                self.leader_id = message["from"]
                self.in_election = False
                # print(f"[{self.node_id}] Nuevo líder reconocido: {self.leader_id}")
                return

            # --- SEMANA 6: CONSENSO (Paxos Simplificado / Centralizado) ---
            
            # CASO 1: Recibo un REQUEST (Alguien quiere enviar un mensaje)
            # Solo me importa si SOY EL LÍDER.
            if msg_type == "request":
                if self.node_id == self.leader_id:
                    # Soy el Líder: Autorizo el mensaje y ordeno el COMMIT
                    commit_msg = {
                        "type": "commit",
                        "id": message["id"],
                        "content": message["content"],
                        "original_sender": message["from"], # Respetamos quien lo escribió
                        "from": self.node_id # Firmado por mí (Líder)
                    }
                    # 1. Guardar localmente (sin usar red)
                    self.handle_commit_internal(commit_msg)
                    # 2. Enviar a todos los demás
                    self.send_to_all_peers(commit_msg)
                return

            # CASO 2: Recibo un COMMIT (Orden final de guardado)
            # Esto viene del Líder. Lo guardo en DB y lo muestro.
            if msg_type == "commit":
                self.handle_commit_internal(message)
                return

        except Exception as e:
            # print(f"Error handling client: {e}")
            pass
        finally:
            client_socket.close()

    def handle_commit_internal(self, message):
        """Lógica común para guardar y mostrar mensaje confirmado."""
        msg_id = message.get("id")
        if msg_id in self.seen_messages: return
        
        self.seen_messages.add(msg_id)
        
        orig_sender = message.get("original_sender")
        content = message.get("content")
        
        # Persistencia (Semana 3)
        self.storage.save_message(msg_id, orig_sender, content)
        
        # Mostrar en pantalla
        print(f"\n[{orig_sender}]: {content}")
        # Restaurar prompt visual
        print(f"({self.node_id}) > ", end="", flush=True)

    # ---------------- ENVÍO (Cliente) ----------------
    def send_direct_message(self, host, port, msg):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(2) # Timeout corto para no bloquear
            s.connect((host, port))
            s.send(json.dumps(msg).encode())
            s.close()
        except:
            pass

    def send_to_all_peers(self, msg):
        for peer in self.peers:
            self.send_direct_message(peer["host"], peer["port"], msg)

    def send_new_message(self, content):
        """Lógica de envío Semana 6: Request vs Commit"""
        msg_id = str(uuid.uuid4())
        
        # Opción A: SOY EL LÍDER
        if self.leader_id == self.node_id:
            commit_msg = {
                "type": "commit",
                "id": msg_id,
                "content": content,
                "original_sender": self.node_id,
                "from": self.node_id
            }
            self.handle_commit_internal(commit_msg) # Guardar yo mismo
            self.send_to_all_peers(commit_msg)      # Mandar a otros

        # Opción B: HAY UN LÍDER (y no soy yo)
        elif self.leader_id:
            req_msg = {
                "type": "request",
                "id": msg_id,
                "content": content,
                "from": self.node_id
            }
            # Buscar IP/Port del líder
            leader_peer = next((p for p in self.peers if p["id"] == self.leader_id), None)
            if leader_peer:
                self.send_direct_message(leader_peer["host"], leader_peer["port"], req_msg)
            else:
                print("Error: Sé quién es el líder pero no tengo su IP/Port en configuración.")

        # Opción C: NO HAY LÍDER
        else:
            print("⚠️ Error: No hay líder electo. Esperando elección...")
            self.start_election()

    # ---------------- HEARTBEATS & FAILOVER (Semana 4-5) ----------------
    def send_heartbeats(self):
        while self.running:
            hb = {"type": "heartbeat", "from": self.node_id}
            # Si soy líder, podría mandar un heartbeat especial, 
            # pero por ahora mandamos normal a todos
            self.send_to_all_peers(hb)
            time.sleep(3)

    def monitor_peers(self):
        """Detecta nodos caídos generales."""
        while self.running:
            now = time.time()
            # Copiamos para evitar errores de modificación durante iteración
            for peer in self.peers:
                pid = peer["id"]
                last = self.last_seen.get(pid)
                if last and (now - last > self.heartbeat_timeout):
                    # print(f"(DEBUG) Nodo {pid} parece inactivo.")
                    pass
            time.sleep(3)

    def monitor_leader(self):
        """Vigila específicamente al Líder."""
        while self.running:
            if self.leader_id and self.leader_id != self.node_id:
                last = self.last_seen.get(self.leader_id)
                if last and (time.time() - last > self.heartbeat_timeout):
                    print(f"\n[ALERTA] Líder {self.leader_id} ha caído. Iniciando elección.")
                    self.leader_id = None
                    self.start_election()
            time.sleep(3)

    # ---------------- ELECCIÓN (Bully) ----------------
    def start_election(self):
        self.in_election = True
        # En Bully, enviamos mensaje de elección solo a los IDs mayores
        higher_nodes = [p for p in self.peers if p["id"] > self.node_id]
        
        if not higher_nodes:
            # Nadie es mayor que yo -> ¡Yo gano!
            self.become_leader()
        else:
            print(f"[{self.node_id}] Iniciando elección contra nodos mayores...")
            msg = {"type": "election", "from": self.node_id}
            for p in higher_nodes:
                self.send_direct_message(p["host"], p["port"], msg)
            
            # Esperar un momento a ver si responden "OK"
            # (En una implementación real esto sería con timeouts más complejos)
            # Simplificación: Si en 3 segundos no dejo de estar en elección, me autoproclamo
            # (Esto es un hack para simplificar el threading)
            threading.Timer(3.0, self.check_election_result).start()

    def check_election_result(self):
        if self.in_election:
            # Si sigo en modo elección, significa que nadie mayor respondió OK
            self.become_leader()

    def become_leader(self):
        self.leader_id = self.node_id
        self.in_election = False
        print(f"\n[{self.node_id}] ¡SOY EL NUEVO LÍDER!")
        msg = {"type": "leader", "from": self.node_id}
        self.send_to_all_peers(msg)

    # ---------------- INTERFAZ (CLI) ----------------
    def show_history(self):
        print(f"\n--- HISTORIAL DE MENSAJES ({self.node_id}) ---")
        rows = self.storage.get_history()
        for row in rows:
            # row = (sender, content, timestamp)
            timestamp = row[2]
            sender = row[0]
            content = row[1]
            print(f"[{timestamp}] {sender}: {content}")
        print("----------------------------------------------")

    def start(self):
        # Iniciar hilos en segundo plano
        threading.Thread(target=self.start_server, daemon=True).start()
        threading.Thread(target=self.send_heartbeats, daemon=True).start()
        threading.Thread(target=self.monitor_peers, daemon=True).start()
        threading.Thread(target=self.monitor_leader, daemon=True).start()

        # (Código NUEVO en método start)
        # Esperamos 2 segundos para asegurar que los sockets de los demás estén listos
        time.sleep(2)
        
        # Al iniciar, si soy el ID más alto, me autoproclamo inmediatamente
        # para que los demás sepan que volví.
        higher = [p for p in self.peers if p["id"] > self.node_id]
        if not higher:
            print(f"[{self.node_id}] Soy el mayor, reclamando liderazgo...")
            self.become_leader()
        else:
            self.start_election()

        print(f"--- Nodo {self.node_id} Activo en puerto {self.port} ---")

        print(f"--- Nodo {self.node_id} Activo en puerto {self.port} ---")
        print("Comandos: /historial, /salir, o escribe un mensaje.")




        while self.running:
            try:
                cmd = input(f"({self.node_id}) > ")
                
                if cmd.strip() == "": continue
                
                if cmd == "/salir":
                    self.running = False
                    print("Apagando nodo...")
                    break
                
                elif cmd == "/historial":
                    self.show_history()
                
                else:
                    # Mensaje normal -> Enviar usando lógica de consenso
                    self.send_new_message(cmd)
                    
            except KeyboardInterrupt:
                self.running = False
                break