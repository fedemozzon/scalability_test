import time
from mgclient import connect

# Connect to Memgraph
conn = connect(host="localhost", port=7687)
cursor = conn.cursor()

# Define the query
query = """
MATCH (u1:User)-[:RECOMMENDS]->(g:Game)<-[:RECOMMENDS]-(u2:User)
WHERE u1.user_id <> u2.user_id
RETURN u1.user_id, u2.user_id, g.title
"""

# Execute the query and measure time
start_time = time.time()
cursor.execute(query)
end_time = time.time()

# Fetch the results if needed
# results = cursor.fetchall()

# Print the execution time
print(f"Tiempo de ejecución: {end_time - start_time:.3f} segundos")

# Close the connection
cursor.close()
conn.close()
