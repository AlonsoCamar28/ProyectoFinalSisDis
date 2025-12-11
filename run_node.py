import sys
import os
# Ajuste para importar src correctamente
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))
from node import DistributedChatNode

if __name__ == "__main__":
    node_id = sys.argv[1] if len(sys.argv) > 1 else "node1"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 5000

    # CONFIGURACIÓN DE PARES (Topología de prueba para Semana 2)
    peers = []
    
    if node_id == "node1":
        # Nodo 1 solo conoce al 2
        peers = [{"id": "node2", "host": "localhost", "port": 5001}]
    
    elif node_id == "node2":
        # Nodo 2 es el puente: conoce al 1 y al 3
        peers = [
            {"id": "node1", "host": "localhost", "port": 5000},
            {"id": "node3", "host": "localhost", "port": 5002}
        ]
    
    elif node_id == "node3":
        # Nodo 3 solo conoce al 2
        peers = [{"id": "node2", "host": "localhost", "port": 5001}]

    print(f"--- Iniciando {node_id} ---")
    print(f"Puerto: {port}")
    print(f"Pares: {[p['id'] for p in peers]}")
    
    node = DistributedChatNode(node_id, "localhost", port, peers)
    node.start()