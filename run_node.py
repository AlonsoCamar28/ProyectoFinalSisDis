import sys
from src.node import DistributedChatNode

if __name__ == "__main__":
    # Configuración de argumentos [cite: 117-118]
    # Uso: python run_node.py <node_id> <port>
    node_id = sys.argv[1] if len(sys.argv) > 1 else "node1"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 5000

    # Configuración MANUAL de pares para prueba rápida [cite: 119-128]
    # Si soy el nodo 1, mi par es el nodo 2, y viceversa.
    peers = []
    if node_id == "node1":
        peers = [{"id": "node2", "host": "localhost", "port": 5001}]
    elif node_id == "node2":
        peers = [{"id": "node1", "host": "localhost", "port": 5000}]

    print(f"Iniciando nodo {node_id} en puerto {port} con pares: {peers}")
    
    # Instanciar y arrancar [cite: 132-134]
    node = DistributedChatNode(node_id, "localhost", port, peers)
    node.start()