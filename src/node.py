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
            
            msg_id = message.get('id')
            if msg_id in self.seen_messages:
                return 

            self.seen_messages.add(msg_id)
            sender = message.get('from')
            content = message.get('content')
            
            # NUEVO SEMANA 3: Guardar en DB cuando recibimos algo nuevo
            self.storage.save_message(msg_id, sender, content)
            
            print(f"\n[{sender}]: {content}") # Formato más limpio

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
        
        time.sleep(1)
        print(f"--- Nodo {self.node_id} Activo ---")
        self.show_help()

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