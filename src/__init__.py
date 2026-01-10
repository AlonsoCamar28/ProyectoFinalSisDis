# SEMANA 4: Heartbeats
self.last_seen = {}
self.heartbeat_interval = 2
self.timeout_interval = 5

for peer in self.peers:
    self.last_seen[peer['id']] = time.time()

    # SEMANA 5: Elección de líder (Bully)
self.leader_id = None
self.in_election = False

    #Semana 6
self.heartbeat_timeout = 6  # segundos


