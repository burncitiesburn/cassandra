from cassandra.cluster import Cluster

class cassandra:
    session = None

    def connect(self,nodes):
        cluster = Cluster(nodes)
        metadata = cluster.metadata
        self.session = cluster.connect()

    def close(self):
        self.session.cluster.shutdown()
        self.session.shutdown()

    def create_keyspace(self):
        self.session.execute("""CREATE KEYSPACE cassandra WITH replication = {'class':'SimpleStrategy', 'replication_factor':3};""")

    def create_schema(self):
         self.session.execute("""
            CREATE TABLE simplex.songs (
                id uuid PRIMARY KEY,
                title text,
                album text,
                artist text,
                tags set<text>,
                data blob
            );
        """)