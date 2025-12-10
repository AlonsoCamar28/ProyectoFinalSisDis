import socket
import threading
import json
import time

class DistributedChatNode:
    def __init__(self, node_id, host, port, peers):
        # Inicialización basada en el documento [cite: 50-57]
        self.node_id = node_id
        self.host = host
        self.port = port
        self.peers = peers # Lista de diccionarios con info de otros nodos
        self.running = True

    def start_server(self):
        """Inicia el servidor TCP para escuchar mensajes entrantes."""
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.host, self.port))
        server_socket.listen(5)
        print(f"[{self.node_id}] Escuchando en {self.host}:{self.port}")

        while self.running:
            try:
                client_sock, address = server_socket.accept()
                # Creamos un hilo para manejar cada conexión entrante [cite: 17, 47]
                threading.Thread(target=self.handle_client, args=(client_sock,)).start()
            except Exception as e:
                print(f"Error en servidor: {e}")

    def handle_client(self, client_socket):
        """Maneja la recepción de datos de un nodo."""
        try:
            data = client_socket.recv(1024).decode('utf-8')
            message = json.loads(data) # Serialización con JSON [cite: 12]
            print(f"\n[{self.node_id}] Mensaje recibido: {message}")
        except Exception as e:
            print(f"Error recibiendo datos: {e}")
        finally:
            client_socket.close()

    def send_message(self, target_host, target_port, message_dict):
        """Cliente para envío de mensajes[cite: 48]."""
        try:
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.connect((target_host, target_port))
            client_socket.send(json.dumps(message_dict).encode('utf-8'))
            client_socket.close()
            print(f"[{self.node_id}] Mensaje enviado a {target_port}")
        except Exception as e:
            print(f"Error enviando mensaje: {e}")

    def start(self):
        """Método principal para iniciar el nodo."""
        # Iniciar el servidor en un hilo separado para permitir enviar mensajes desde la consola
        server_thread = threading.Thread(target=self.start_server)
        server_thread.daemon = True
        server_thread.start()
        
        # Bucle simple para probar envío manual (simulación de consola de Semana 1)
        time.sleep(1) # Esperar a que el servidor inicie
        if self.peers:
            print("Escribe un mensaje para enviar a los pares (o 'salir'):")
            while self.running:
                msg_text = input()
                if msg_text == 'salir':
                    self.running = False
                    break
                
                # Enviar a todos los pares conocidos
                for peer in self.peers:
                    msg = {"from": self.node_id, "content": msg_text}
                    self.send_message(peer['host'], peer['port'], msg)