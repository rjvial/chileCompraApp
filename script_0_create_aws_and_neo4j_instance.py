import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import time
import paramiko
import funcionesNeo4jEC2 as fne

import os
from dotenv import load_dotenv

load_dotenv("secrets.env")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")



# ─── CONFIGURATION ─────────────────────────────────────────────────────────────
instance_name = "Neo4j-EC2" #"Neo4j-EC2-Prueba" # 

REGION          = 'us-east-1'
KEY_NAME        = 'neo4j-key-pair'
KEY_PATH        = f'{KEY_NAME}.pem'
SEC_GROUP_NAME  = 'neo4j-sg'
INSTANCE_TYPE   = 't3.medium'
NEO4J_VERSION   = '5.12.0'
SSH_USER        = 'ec2-user'
SSH_PORT        = 22
EBBSIZE_GIB     = 120 #30 #

# ─── AWS CLIENT ────────────────────────────────────────────────────────────────
# Configure with exponential backoff retry strategy
retry_config = Config(
    retries={
        'max_attempts': 10,
        'mode': 'adaptive'  # Uses adaptive retry mode with exponential backoff
    }
)
ec2 = boto3.client('ec2', region_name=REGION, config=retry_config)

def get_default_vpc_id():
    vpcs = ec2.describe_vpcs(Filters=[{'Name':'isDefault','Values':['true']}])
    return vpcs['Vpcs'][0]['VpcId']

def get_latest_amzn2_ami():
    images = ec2.describe_images(
        Owners=['amazon'],
        Filters=[{'Name':'name','Values':['amzn2-ami-hvm-*-x86_64-gp2']}]
    )['Images']
    images.sort(key=lambda img: img['CreationDate'], reverse=True)
    return images[0]['ImageId']

def find_instance_by_name(name: str):
    """Find running or pending instance by Name tag"""
    response = ec2.describe_instances(
        Filters=[
            {'Name': 'tag:Name', 'Values': [name]},
            {'Name': 'instance-state-name', 'Values': ['running', 'pending', 'stopped']}
        ]
    )
    for reservation in response['Reservations']:
        if reservation['Instances']:
            return reservation['Instances'][0]
    return None


# ─── HELPER: TRANSFER CSVs FROM S3 TO NEO4J IMPORT ─────────────────────────────
def transfer_csvs_to_neo4j_import(
    public_ip: str,
    s3_client,
    s3_bucket: str = 'landengines-data',
    s3_prefix: str = 'kg/',
    target_files: list[str] | None = None,
    ssh_user: str = 'ec2-user',
    ssh_port: int = SSH_PORT,
    key_path: str = 'neo4j-key-pair.pem',
    import_dir: str = '/var/lib/neo4j/import'
):
    # If no specific files passed, list all CSVs under the prefix
    if target_files is None:
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=s3_bucket, Prefix=s3_prefix)
        target_files = [
            obj['Key']
            for page in pages for obj in page.get('Contents', [])
            if obj['Key'].lower().endswith('.csv')
        ]

    # SSH in and ensure import directory exists & is owned by neo4j
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(public_ip, port=ssh_port, username=ssh_user, key_filename=key_path)
    setup_cmd = f"sudo mkdir -p {import_dir} && sudo chown neo4j:neo4j {import_dir}"
    _, out, err = ssh.exec_command(setup_cmd)
    if out.channel.recv_exit_status() != 0:
        raise RuntimeError(err.read().decode())

    sftp = ssh.open_sftp()
    for key in target_files:
        filename = os.path.basename(key)
        local_path = f"/tmp/{filename}"
        # download from S3
        s3_client.download_file(s3_bucket, key, local_path)
        # push into Neo4j import dir
        remote_path = f"{import_dir}/{filename}"
        sftp.put(local_path, remote_path)
        # fix ownership so Neo4j can read
        ssh.exec_command(f"sudo chown neo4j:neo4j {remote_path}")
        os.remove(local_path)

    sftp.close()
    ssh.close()
    print(f"✔ Transferred {len(target_files)} CSV file(s) to {import_dir}")

# ─── STEP 1: CREATE KEY PAIR ────────────────────────────────────────────────────
try:
    kp = ec2.create_key_pair(KeyName=KEY_NAME, KeyType='rsa', KeyFormat='pem')
    with open(KEY_PATH, 'w') as f:
        f.write(kp['KeyMaterial'])
    os.chmod(KEY_PATH, 0o400)
    print(f"✔ Key pair '{KEY_NAME}' created and saved to {KEY_PATH}")
except ec2.exceptions.ClientError as e:
    if 'InvalidKeyPair.Duplicate' in str(e):
        print(f"ℹ Key pair '{KEY_NAME}' already exists, skipping.")
    else:
        raise

# ─── STEP 2: CREATE SECURITY GROUP ─────────────────────────────────────────────
vpc_id = get_default_vpc_id()
neo4j_browser_port = 7474
neo4j_bolt_port = 7687
try:
    sg = ec2.create_security_group(
        GroupName=SEC_GROUP_NAME,
        Description=f'Neo4j access (SSH, HTTP {neo4j_browser_port}, Bolt {neo4j_bolt_port})',
        VpcId=vpc_id
    )
    sg_id = sg['GroupId']
    ec2.authorize_security_group_ingress(
        GroupId=sg_id,
        IpPermissions=[
            {'IpProtocol':'tcp','FromPort':SSH_PORT,  'ToPort':SSH_PORT,  'IpRanges':[{'CidrIp':'0.0.0.0/0'}]},
            {'IpProtocol':'tcp','FromPort':neo4j_browser_port,'ToPort':neo4j_browser_port,'IpRanges':[{'CidrIp':'0.0.0.0/0'}]},
            {'IpProtocol':'tcp','FromPort':neo4j_bolt_port,'ToPort':neo4j_bolt_port,'IpRanges':[{'CidrIp':'0.0.0.0/0'}]},
        ]
    )
    print(f"✔ Security group '{SEC_GROUP_NAME}' ({sg_id}) created and rules added")
except ec2.exceptions.ClientError as e:
    if 'InvalidGroup.Duplicate' in str(e):
        existing = ec2.describe_security_groups(
            Filters=[{'Name':'group-name','Values':[SEC_GROUP_NAME]}]
        )['SecurityGroups'][0]
        sg_id = existing['GroupId']
        print(f"ℹ Security group '{SEC_GROUP_NAME}' exists ({sg_id}), using it")
    else:
        raise

# ─── STEP 3: LAUNCH EC2 INSTANCE WITH USER DATA & CUSTOM EBS ───────────────────
ami_id = get_latest_amzn2_ami()
user_data = f'''#!/bin/bash
set -e

# Add Neo4j repo
rpm --import https://debian.neo4j.com/neotechnology.gpg.key
cat <<EOF > /etc/yum.repos.d/neo4j.repo
[neo4j]
name=Neo4j RPM Repository
baseurl=https://yum.neo4j.com/stable/5
enabled=1
gpgcheck=1
EOF

# Install Java & Neo4j
yum install -y java-17-amazon-corretto-headless
export NEO4J_ACCEPT_LICENSE_AGREEMENT=yes
yum install -y neo4j-{NEO4J_VERSION}

# Install APOC plugin
wget -O /var/lib/neo4j/plugins/apoc-{NEO4J_VERSION}-core.jar \
     https://github.com/neo4j/apoc/releases/download/{NEO4J_VERSION}/apoc-{NEO4J_VERSION}-core.jar

# Enable on boot & set initial password
systemctl enable neo4j
neo4j-admin dbms set-initial-password {NEO4J_PASSWORD}
systemctl start neo4j
'''

# ─── LAUNCH INSTANCE ───────────────────────────────────────────────────────────
# Check if instance already exists
existing_instance = find_instance_by_name(instance_name)
if existing_instance:
    instance_id = existing_instance['InstanceId']
    instance_state = existing_instance['State']['Name']
    print(f"ℹ Instance '{instance_name}' already exists ({instance_id}) in state: {instance_state}")

    # Start if stopped
    if instance_state == 'stopped':
        print(f"⏳ Starting stopped instance {instance_id}...")
        ec2.start_instances(InstanceIds=[instance_id])
        ec2.get_waiter('instance_running').wait(InstanceIds=[instance_id])
        print(f"✔ Instance {instance_id} started")

    # Wait until running
    if instance_state in ['pending', 'stopped']:
        print("⏳ Waiting for instance to run...")
        ec2.get_waiter('instance_running').wait(InstanceIds=[instance_id])

    info = ec2.describe_instances(InstanceIds=[instance_id])['Reservations'][0]['Instances'][0]
    public_ip = info['PublicIpAddress']
    print(f"🚀 Instance is up at {public_ip}")
else:
    print(f"⏳ Launching new instance '{instance_name}' with AMI {ami_id}...")
    try:
        resp = ec2.run_instances(
            ImageId=ami_id,
            InstanceType=INSTANCE_TYPE,
            KeyName=KEY_NAME,
            SecurityGroupIds=[sg_id],
            UserData=user_data,
            MinCount=1,
            MaxCount=1,
            BlockDeviceMappings=[{
                'DeviceName': '/dev/xvda',
                'Ebs': {
                    'VolumeSize': EBBSIZE_GIB,
                    'VolumeType': 'gp3',
                    'DeleteOnTermination': True
                }
            }],
            TagSpecifications=[{
                'ResourceType': 'instance',
                'Tags': [{'Key':'Name','Value':instance_name}]
            }]
        )
        instance_id = resp['Instances'][0]['InstanceId']
        print(f"✔ Launched instance {instance_id} with {EBBSIZE_GIB}GiB root volume")

        print("⏳ Waiting for instance to run...")
        ec2.get_waiter('instance_running').wait(InstanceIds=[instance_id])
        info = ec2.describe_instances(InstanceIds=[instance_id])['Reservations'][0]['Instances'][0]
        public_ip = info['PublicIpAddress']
        print(f"🚀 Instance is up at {public_ip}")
    except ClientError as e:
        if 'RequestLimitExceeded' in str(e):
            print("⚠️  AWS API rate limit exceeded. Waiting 60 seconds before retrying...")
            time.sleep(60)
            print("⏳ Retrying instance launch...")
            resp = ec2.run_instances(
                ImageId=ami_id,
                InstanceType=INSTANCE_TYPE,
                KeyName=KEY_NAME,
                SecurityGroupIds=[sg_id],
                UserData=user_data,
                MinCount=1,
                MaxCount=1,
                BlockDeviceMappings=[{
                    'DeviceName': '/dev/xvda',
                    'Ebs': {
                        'VolumeSize': EBBSIZE_GIB,
                        'VolumeType': 'gp3',
                        'DeleteOnTermination': True
                    }
                }],
                TagSpecifications=[{
                    'ResourceType': 'instance',
                    'Tags': [{'Key':'Name','Value':instance_name}]
                }]
            )
            instance_id = resp['Instances'][0]['InstanceId']
            print(f"✔ Launched instance {instance_id} with {EBBSIZE_GIB}GiB root volume")

            print("⏳ Waiting for instance to run...")
            ec2.get_waiter('instance_running').wait(InstanceIds=[instance_id])
            info = ec2.describe_instances(InstanceIds=[instance_id])['Reservations'][0]['Instances'][0]
            public_ip = info['PublicIpAddress']
            print(f"🚀 Instance is up at {public_ip}")
        else:
            raise

# ─── STEP 4: PATCH neo4j.conf & apoc.conf VIA SSH ───────────────────────────────
time.sleep(60)  # wait for cloud-init & Neo4j startup

# these values you can tune as needed
HEAP_INITIAL = "512m"
HEAP_MAX     = "1G"
TX_GLOBAL    = "512m"

sed_cmds = [
    # Increase heap sizes
    f"sudo sed -i 's|^#\\?dbms.memory.heap.initial_size=.*|dbms.memory.heap.initial_size={HEAP_INITIAL}|' /etc/neo4j/neo4j.conf",
    f"sudo sed -i 's|^#\\?dbms.memory.heap.max_size=.*|dbms.memory.heap.max_size={HEAP_MAX}|' /etc/neo4j/neo4j.conf",

    # Remove any existing transaction total.max lines and replace with the desired value
    "sudo sed -i '/^dbms.memory.transaction.total.max/d' /etc/neo4j/neo4j.conf",
    f"echo 'dbms.memory.transaction.total.max={TX_GLOBAL}' | sudo tee -a /etc/neo4j/neo4j.conf",

    # Networking & APOC configuration
    "sudo sed -i 's|^#\\?server.default_listen_address=.*|server.default_listen_address=0.0.0.0|' /etc/neo4j/neo4j.conf",
    f"sudo sed -i 's|^#\\?server.http.listen_address=.*|server.http.listen_address=0.0.0.0:{neo4j_browser_port}|' /etc/neo4j/neo4j.conf",
    f"sudo sed -i 's|^#\\?server.bolt.listen_address=.*|server.bolt.listen_address=0.0.0.0:{neo4j_bolt_port}|' /etc/neo4j/neo4j.conf",
    "sudo sed -i '/^dbms.security.procedures.unrestricted=/d' /etc/neo4j/neo4j.conf",
    "echo 'dbms.security.procedures.unrestricted=apoc.*' | sudo tee -a /etc/neo4j/neo4j.conf",
    "sudo sed -i 's|^#\\?apoc.import.file.enabled=.*|apoc.import.file.enabled=true|' /etc/neo4j/neo4j.conf",
    "sudo sed -i 's|^#\\?apoc.import.file.use_neo4j_config=.*|apoc.import.file.use_neo4j_config=true|' /etc/neo4j/neo4j.conf",
    "echo 'apoc.import.file.enabled=true' | sudo tee /etc/neo4j/apoc.conf",
    "echo 'apoc.import.file.use_neo4j_config=true' | sudo tee -a /etc/neo4j/apoc.conf",

    # Restart Neo4j to apply all changes
    "sudo systemctl restart neo4j"
]

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(hostname=public_ip, username=SSH_USER, key_filename=KEY_PATH, port=SSH_PORT)

for cmd in sed_cmds:
    stdin, stdout, stderr = ssh.exec_command(cmd)
    exit_code = stdout.channel.recv_exit_status()
    print(f"[{exit_code}] {cmd}")
    out = stdout.read().decode().strip()
    err = stderr.read().decode().strip()
    if out: print("OUT:", out)
    if err: print("ERR:", err)
    time.sleep(2)

ssh.close()
print("✅ neo4j.conf & apoc.conf patched and service restarted")

# ─── FINAL: OPEN NEO4J BROWSER ─────────────────────────────────────────────────
fne.neo4jbrowser(public_ip)
