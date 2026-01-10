import socket
import threading
import json
import time
import uuid
from storage import ChatStorage # <--- NUEVO SEMANA 3: Importar almacenamiento

class DistributedChatNode:
    def __init__(self, node_id, host, port, peers):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.peers = peers
        self.running = True
        self.seen_messages = set()
        
        # NUEVO SEMANA 3: Inicializar base de datos
        self.storage = ChatStorage(node_id)
        
        # Cargar mensajes antiguos a la memoria "vista" para no reprocesarlos
        history = self.storage.get_history()
        print(f"[{self.node_id}] Base de datos cargada: {len(history)} mensajes previos.")

    def start_server(self):
        # (Igual que antes...)
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.host, self.port))
        server_socket.listen(5)
        
        while self.running:
            try:
                client_sock, address = server_socket.accept()
                threading.Thread(target=self.handle_client, args=(client_sock,)).start()
            except Exception as e:
                if self.running: print(f"Error en servidor: {e}")

    def handle_client(self, client_socket):
    try:
        data = client_socket.recv(1024).decode('utf-8')
        message = json.loads(data)

        # SEMANA 4: Heartbeat recibido
        if message.get("type") == "heartbeat":
            sender = message.get("from")
            self.last_seen[sender] = time.time()
            return
        # SEMANA 5: election
if message.get("type") == "election":
    sender = message.get("from")
    print(f"[{self.node_id}] Elección recibida de {sender}")

    reply = {"type": "ok", "from": self.node_id}
    self.send_direct_message(
        next(p['host'] for p in self.peers if p['id'] == sender),
        next(p['port'] for p in self.peers if p['id'] == sender),
        reply
    )

    if not self.in_election:
        self.start_election()
    return


# SEMANA 5: ok
if message.get("type") == "ok":
    self.in_election = False
    return


# SEMANA 5: leader
if message.get("type") == "leader":
    self.leader_id = message.get("from")
    self.in_election = False
    print(f"[{self.node_id}] Nuevo líder: {self.leader_id}")
    return

        
msg_id = message.get('id')
        if msg_id in self.seen_messages:
            return

        self.seen_messages.add(msg_id)
        sender = message.get('from')
        content = message.get('content')

        # Guardar en DB
        self.storage.save_message(msg_id, sender, content)

        print(f"\n[{sender}]: {content}")

        self.broadcast_message(message, original_sender=False)

    except Exception as e:
        print(f"Error recibiendo datos: {e}")
    finally:
        client_socket.close()


    def send_direct_message(self, target_host, target_port, message_dict):
        # (Igual que antes...)
        try:
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.connect((target_host, target_port))
            client_socket.send(json.dumps(message_dict).encode('utf-8'))
            client_socket.close()
        except Exception:
            pass

    def broadcast_message(self, message_dict, original_sender=True):
        if original_sender:
            msg_id = str(uuid.uuid4()) 
            message_dict['id'] = msg_id
            self.seen_messages.add(msg_id)
            
            # NUEVO SEMANA 3: Guardar mis propios mensajes en MI base de datos
            self.storage.save_message(msg_id, self.node_id, message_dict['content'])
        
        for peer in self.peers:
            if peer['id'] != message_dict.get('from'): 
                self.send_direct_message(peer['host'], peer['port'], message_dict)

    def show_history(self):
        """Muestra el historial SIN usar tabulate."""
        rows = self.storage.get_history()
        print("\n--- HISTORIAL DE MENSAJES ---")
        print(f"{'HORA':<20} | {'REMITENTE':<10} | {'MENSAJE'}")
        print("-" * 50)
        for row in rows:
            # row[0]=sender, row[1]=content, row[2]=timestamp
            sender = row[0]
            content = row[1]
            time_str = row[2]
            print(f"{time_str:<20} | {sender:<10} | {content}")
        print("-" * 50)

    def show_help(self):
        """NUEVO SEMANA 3: Muestra comandos disponibles."""
        print("\n--- Comandos ---")
        print("/historial : Ver mensajes guardados")
        print("/salir     : Apagar nodo")
        print("Cualquier otro texto se enviará como mensaje.")

    def start(self):
        server_thread = threading.Thread(target=self.start_server)
        server_thread.daemon = True
        server_thread.start()
        monitor_thread = threading.Thread(target=self.monitor_leader)
        monitor_thread.daemon = True
        monitor_thread.start()


        time.sleep(1)
        print(f"--- Nodo {self.node_id} Activo ---")
        self.show_help()

        #SEMANA 4:  Hilos de heartbeats y monitoreo
        heartbeat_thread = threading.Thread(target=self.send_heartbeats)
        heartbeat_thread.daemon = True
        heartbeat_thread.start()

        monitor_thread = threading.Thread(target=self.monitor_peers)
        monitor_thread.daemon = True
        monitor_thread.start()

        # NUEVO SEMANA 3: Bucle de comandos mejorado (CLI)
        while self.running:
            try:
                command = input(f"({self.node_id}) > ")
                
                if command.strip() == "/salir":
                    self.running = False
                    # Generar un socket falso para desbloquear el accept() del servidor y cerrar limpio
                    try:
                        dummy = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        dummy.connect((self.host, self.port))
                        dummy.close()
                    except: pass
                    break
                
                elif command.strip() == "/historial":
                    self.show_history()
                
                elif command.strip() == "/ayuda":
                    self.show_help()
                
                elif command.strip() != "":
                    # Es un mensaje normal
                    msg = {
                        "from": self.node_id,
                        "content": command
                    }
                    self.broadcast_message(msg, original_sender=True)
            except KeyboardInterrupt:
                self.running = False
                break

    def send_heartbeats(self):
    while self.running:
        heartbeat_msg = {
            "type": "heartbeat",
            "from": self.node_id
        }

        for peer in self.peers:
         self.send_direct_message(peer['host'], peer['port'], heartbeat_msg)


        time.sleep(self.heartbeat_interval)

    def monitor_peers(self):
    while self.running:
        now = time.time()
        for peer_id, last in self.last_seen.items():
            if now - last > self.timeout_interval:
                print(f"[ALERTA] Nodo {peer_id} no responde (caído)")
        time.sleep(1)

def start_election(self):
    print(f"[{self.node_id}] Iniciando elección de líder")
    self.in_election = True
    higher_nodes = [p for p in self.peers if p['id'] > self.node_id]

    if not higher_nodes:
        self.become_leader()
        return

    msg = {
        "type": "election",
        "from": self.node_id
    }

    for peer in higher_nodes:
        self.send_direct_message(peer['host'], peer['port'], msg)

        def become_leader(self):
    self.leader_id = self.node_id
    self.in_election = False
    print(f"[{self.node_id}] SOY EL NUEVO LÍDER")

    msg = {
        "type": "leader",
        "from": self.node_id
    }

    for peer in self.peers:
        self.send_direct_message(peer['host'], peer['port'], msg)

        #SEMANA 6: Crear monitor del lider
def monitor_leader(self):
    while self.running:
        time.sleep(2)

        if self.leader_id is None:
            continue

        last = self.last_seen.get(self.leader_id)

        if last is None:
            continue

        if time.time() - last > self.heartbeat_timeout:
            print(f"[{self.node_id}] Líder {self.leader_id} caído. Iniciando elección...")
            self.leader_id = None
            self.start_election()



