import boto3, os
from dotenv import load_dotenv
import funcionesNeo4j as fn
import funcionesNeo4jEC2 as fne

load_dotenv("secrets.env")
ec2 = boto3.client('ec2', region_name='us-east-1')
_, public_ip, _ = fne.find_instance_by_name(ec2, 'Neo4j-EC2')
conn = fn.Neo4jConnection(uri=f"bolt://{public_ip}:7687",
                          user=os.getenv("NEO4J_USER"),
                          pwd=os.getenv("NEO4J_PASSWORD"),
                          encrypted=False)

print("IP:", public_ip)
print("\n--- LABELS ---")
for r in fn.neo4jQuery("CALL db.labels() YIELD label RETURN label ORDER BY label", conn):
    print(" ", r["label"])

print("\n--- RELATIONSHIP TYPES ---")
for r in fn.neo4jQuery("CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType ORDER BY relationshipType", conn):
    print(" ", r["relationshipType"])

print("\n--- NODE COUNTS ---")
for r in fn.neo4jQuery("CALL db.labels() YIELD label CALL apoc.cypher.run('MATCH (n:`'+label+'`) RETURN count(n) AS c',{}) YIELD value RETURN label, value.c AS count ORDER BY label", conn):
    print(f"  {r['label']}: {r['count']}")

print("\n--- REL COUNTS ---")
for r in fn.neo4jQuery("CALL db.relationshipTypes() YIELD relationshipType CALL apoc.cypher.run('MATCH ()-[r:`'+relationshipType+'`]->() RETURN count(r) AS c',{}) YIELD value RETURN relationshipType, value.c AS count ORDER BY relationshipType", conn):
    print(f"  {r['relationshipType']}: {r['count']}")

print("\n--- SCHEMA (node properties) ---")
for r in fn.neo4jQuery("CALL db.schema.nodeTypeProperties() YIELD nodeType, propertyName, propertyTypes RETURN nodeType, propertyName, propertyTypes ORDER BY nodeType, propertyName", conn):
    print(f"  {r['nodeType']}.{r['propertyName']} :: {r['propertyTypes']}")

print("\n--- SCHEMA (rel patterns) ---")
for r in fn.neo4jQuery("CALL db.schema.relTypeProperties() YIELD relType, propertyName, propertyTypes RETURN relType, propertyName, propertyTypes ORDER BY relType", conn):
    print(f"  {r['relType']}.{r['propertyName']} :: {r['propertyTypes']}")

print("\n--- CONNECTIONS (observed triples) ---")
for r in fn.neo4jQuery("MATCH (a)-[r]->(b) RETURN DISTINCT labels(a) AS src, type(r) AS rel, labels(b) AS tgt ORDER BY rel", conn):
    print(f"  {r['src']} -[{r['rel']}]-> {r['tgt']}")
