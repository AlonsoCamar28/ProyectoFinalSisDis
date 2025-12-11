import socket
import threading
import json
import time
import uuid  # <--- NUEVO: Para generar IDs únicos

class DistributedChatNode:
    def __init__(self, node_id, host, port, peers):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.peers = peers
        self.running = True
        
        # NUEVO: Memoria para guardar IDs de mensajes que ya vimos
        # Esto evita bucles infinitos y duplicados 
        self.seen_messages = set() 

    def start_server(self):
        # (Este método queda igual que la semana pasada)
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.host, self.port))
        server_socket.listen(5)
        print(f"[{self.node_id}] Escuchando en {self.host}:{self.port}")

        while self.running:
            try:
                client_sock, address = server_socket.accept()
                threading.Thread(target=self.handle_client, args=(client_sock,)).start()
            except Exception as e:
                print(f"Error en servidor: {e}")

    def handle_client(self, client_socket):
        """Maneja la recepción y REENVÍO de mensajes."""
        try:
            data = client_socket.recv(1024).decode('utf-8')
            message = json.loads(data)
            
            # NUEVO: Lógica de Deduplicación y Reenvío [cite: 36, 61]
            msg_id = message.get('id')
            
            # 1. Si ya vimos este mensaje, lo ignoramos (Deduplicación)
            if msg_id in self.seen_messages:
                return 

            # 2. Si es nuevo, lo registramos y procesamos
            self.seen_messages.add(msg_id)
            sender = message.get('from')
            content = message.get('content')
            print(f"\n[{self.node_id}] Recibido de {sender}: {content}")

            # 3. REENVÍO (Flooding): Propagar a todos mis vecinos
            # (El protocolo gossip básico: "cuéntaselo a tus amigos")
            self.broadcast_message(message, original_sender=False)

        except Exception as e:
            print(f"Error recibiendo datos: {e}")
        finally:
            client_socket.close()

    def send_direct_message(self, target_host, target_port, message_dict):
        """Envía un paquete JSON a un destino específico (TCP)."""
        try:
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.connect((target_host, target_port))
            client_socket.send(json.dumps(message_dict).encode('utf-8'))
            client_socket.close()
        except Exception as e:
            # Es normal que falle si un nodo no está conectado
            pass

    def broadcast_message(self, message_dict, original_sender=True):
        """Envía el mensaje a TODOS los pares conocidos."""
        
        # Si soy el creador original del mensaje, le pongo ID y lo marco como visto
        if original_sender:
            msg_id = str(uuid.uuid4()) # Generar UUID único 
            message_dict['id'] = msg_id
            self.seen_messages.add(msg_id)
        
        # Iterar sobre todos los vecinos y enviar
        for peer in self.peers:
            # Evitar reenviar al nodo que me lo acaba de mandar (optimización básica)
            if peer['id'] != message_dict.get('from'): 
                self.send_direct_message(peer['host'], peer['port'], message_dict)

    def start(self):
        server_thread = threading.Thread(target=self.start_server)
        server_thread.daemon = True
        server_thread.start()
        
        time.sleep(1)
        if self.peers:
            print("Escribe mensaje (o 'salir'):")
            while self.running:
                msg_text = input()
                if msg_text == 'salir':
                    self.running = False
                    break
                
                # Construir mensaje nuevo
                msg = {
                    "from": self.node_id,
                    "content": msg_text
                }
                # Usar broadcast en lugar de envío directo
                self.broadcast_message(msg, original_sender=True)