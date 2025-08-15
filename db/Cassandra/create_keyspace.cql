
    CREATE KEYSPACE IF NOT EXISTS ecommerce 
    WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};
    