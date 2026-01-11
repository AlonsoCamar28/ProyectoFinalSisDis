import sys
import time
# Ajusta la importación según tu estructura de carpetas
from src.node import DistributedChatNode

if __name__ == "__main__":
    # Uso: python run_node.py node1 5000
    node_id = sys.argv[1] if len(sys.argv) > 1 else "node1"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 5000

    # --- CONFIGURACIÓN "FULL MESH" (TODOS SE CONOCEN) ---
    # Esto es vital para que el consenso funcione correctamente.
    
    # Definimos el diccionario de toda la red
    network_map = {
        "node1": {"host": "localhost", "port": 5000},
        "node2": {"host": "localhost", "port": 5001},
        "node3": {"host": "localhost", "port": 5002}
    }

    # Creamos la lista de pares excluyéndonos a nosotros mismos
    peers = []
    for nid, info in network_map.items():
        if nid != node_id:
            peers.append({
                "id": nid, 
                "host": info["host"], 
                "port": info["port"]
            })

    print(f"--- Iniciando {node_id} ---")
    print(f"Puerto: {port}")
    print(f"Pares conocidos: {[p['id'] for p in peers]}")
    
    node = DistributedChatNode(node_id, "localhost", port, peers)
    node.start()