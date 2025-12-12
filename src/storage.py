import sqlite3
from datetime import datetime

class ChatStorage:
    def __init__(self, node_id):
        # Cada nodo tendrá su propio archivo de base de datos (ej. node1_chat.db)
        self.db_name = f"{node_id}_chat.db"
        self.init_db()

    def init_db(self):
        """Crea la tabla de mensajes si no existe."""
        # check_same_thread=False es necesario porque escribiremos desde distintos hilos
        conn = sqlite3.connect(self.db_name, check_same_thread=False)
        cursor = conn.cursor()
        
        # Tabla simple: ID, Quién lo envió, Contenido, Fecha
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS messages (
                id TEXT PRIMARY KEY,
                sender TEXT,
                content TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()
        conn.close()

    def save_message(self, msg_id, sender, content):
        """Guarda un mensaje en la base de datos."""
        try:
            conn = sqlite3.connect(self.db_name, check_same_thread=False)
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR IGNORE INTO messages (id, sender, content)
                VALUES (?, ?, ?)
            ''', (msg_id, sender, content))
            conn.commit()
            conn.close()
            # print(f"(DEBUG) Mensaje {msg_id[:5]} guardado en DB") 
        except Exception as e:
            print(f"Error guardando mensaje: {e}")

    def get_history(self):
        """Recupera todos los mensajes ordenados por fecha."""
        conn = sqlite3.connect(self.db_name, check_same_thread=False)
        cursor = conn.cursor()
        cursor.execute('SELECT sender, content, timestamp FROM messages ORDER BY timestamp ASC')
        rows = cursor.fetchall()
        conn.close()
        return rows