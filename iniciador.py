import boto3
import funcionesNeo4jEC2 as fne


ec2 = boto3.client('ec2', region_name='us-east-1')
instance_name = 'Neo4j-EC2' #'Neo4j-EC2_JLV' #
instance_id, public_ip, state = fne.find_instance_by_name(ec2, instance_name)
# instance_id, public_ip = fne.find_running_instance(ec2, instance_name)

# ─── OPEN NEO4J BROWSER ────────────────────────────────────────────────────
# fne.neo4jbrowser(public_ip)

# ─── STOP INSTANCE ────────────────────────────────────────────────────────
# fne.stop_instance(ec2, instance_id)

# ─── START INSTANCE ───────────────────────────────────────────────────────
# fne.start_instance(ec2, instance_id)

a = 1

# sudo systemctl status neo4j -l
# sudo systemctl start neo4j

# // 1) Delete every node (and thus all relationships via DETACH) in batches
# CALL apoc.periodic.iterate(
#   'MATCH (n) RETURN n',
#   'DETACH DELETE n',
#   {batchSize:500, parallel:false}
# );

# // 2) Drop every index and constraint
# CALL apoc.schema.assert({}, {}, true);

# DROP CONSTRAINT constraint_name

