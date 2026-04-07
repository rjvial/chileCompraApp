import paramiko
import os
import pandas as pd
import funcionesNeo4j as fn
import funcionesAws as fa


def find_instance_by_name(ec2_client, instance_name):
    filters = [{'Name': 'tag:Name', 'Values': [instance_name]}]

    resp = ec2_client.describe_instances(Filters=filters)
    for reservation in resp.get('Reservations', []):
        for instance in reservation.get('Instances', []):
            instance_id = instance['InstanceId']
            public_ip = instance.get('PublicIpAddress')
            state = instance['State']['Name']
            print(f"Found instance {instance_id} @ {public_ip} (state: {state})")
            return instance_id, public_ip, state

    raise RuntimeError(f"No instance found with Name tag '{instance_name}'")


def find_running_instance(ec2_client, instance_name):

    resp = ec2_client.describe_instances(
        Filters=[
            {'Name': f'tag:{'Name'}',       'Values': [instance_name]},
            {'Name': 'instance-state-name',  'Values': ['running']}
        ]
    )
    for r in resp['Reservations']:
        for i in r['Instances']:
            instance_id = i['InstanceId']
            public_ip = i['PublicIpAddress']
            print(f"Found instance {instance_id} @ {public_ip}")
            return instance_id, public_ip
    
    raise RuntimeError(f"No running instance found with tag {'Name'}={instance_name}")
    return None, None

def start_instance(ec2_client, instance_id):
    """Start the given EC2 instance."""
    print(f"Starting instance {instance_id}...")
    resp = ec2_client.start_instances(InstanceIds=[instance_id])
    for inst in resp.get('StartingInstances', []):
        print(f"Instance {inst['InstanceId']} state: {inst['PreviousState']['Name']} → {inst['CurrentState']['Name']}")
    return resp

def stop_instance(ec2_client, instance_id):
    """Stop the given EC2 instance."""
    print(f"Stopping instance {instance_id}...")
    resp = ec2_client.stop_instances(InstanceIds=[instance_id])
    stopping = resp.get('StoppingInstances', [])
    for inst in stopping:
        print(f"Instance {inst['InstanceId']} state: {inst['PreviousState']['Name']} → {inst['CurrentState']['Name']}")
    return resp

def transfer_csvs_to_neo4j_import(
    public_ip: str,
    s3_client,
    s3_bucket: str = 'landengines-data',
    s3_prefix: str = 'kg/',
    specific_s3_key: str | None = None,
    target_files: list[str] = [],
    ssh_user: str = 'ec2-user',
    ssh_port: int = 22,
    key_path: str = 'neo4j-key-pair.pem',
    import_dir: str = '/var/lib/neo4j/import'
):
    """
    Transfer CSV files from S3 to the Neo4j import directory on EC2.

    Parameters:
    - target_files: Controls deletion and import behavior:
        * Empty list [] (default): Delete all files from EC2, then import all files from S3
        * Non-empty list: Delete only those files from EC2, then import only those files
    """
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(public_ip, port=ssh_port, username=ssh_user, key_filename=key_path)

    # 1) Ensure import dir exists and is owned by neo4j
    setup = f"sudo mkdir -p {import_dir} && sudo chown neo4j:neo4j {import_dir}"
    _, out, err = ssh.exec_command(setup)
    if out.channel.recv_exit_status() != 0:
        raise RuntimeError(err.read().decode())
    print(f"✔ Ensured {import_dir} exists and is owned by neo4j")

    # 2) Handle deletion based on target_files parameter
    if len(target_files) == 0:
        # Empty list: delete all files in import directory
        cmd = f"sudo rm -f {import_dir}/*"
        _, out, err = ssh.exec_command(cmd)
        if out.channel.recv_exit_status() == 0:
            print(f"✔ Deleted all files in {import_dir}")
        else:
            print(f"✖ Failed to delete files: {err.read().decode()}")
    else:
        # Non-empty list: delete only specified files
        for filename in target_files:
            path = os.path.join(import_dir, filename)
            cmd = f"sudo rm -f '{path}'"
            _, out, err = ssh.exec_command(cmd)
            if out.channel.recv_exit_status() == 0:
                print(f"✔ Deleted {filename}")
            else:
                print(f"✖ Failed to delete {filename}: {err.read().decode()}")

    # 3) Decide which keys to transfer
    keys_to_transfer = []
    if specific_s3_key:
        keys_to_transfer = [f'{s3_prefix}{specific_s3_key}']
    else:
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=s3_bucket, Prefix=s3_prefix):
            for obj in page.get('Contents', []):
                keys_to_transfer.append(obj['Key'])

    # 4) Transfer files
    for key in keys_to_transfer:
        filename = os.path.basename(key)

        # If target_files is non-empty, only transfer files in the list
        if len(target_files) > 0 and filename not in target_files:
            print(f"→ Skipping {filename}: not in target_files")
            continue

        dest = f"{import_dir}/{filename}"

        # 5) Download via presigned URL + curl
        url = s3_client.generate_presigned_url(
            ClientMethod='get_object',
            Params={'Bucket': s3_bucket, 'Key': key},
            ExpiresIn=3600
        )
        dl_cmd = f"sudo curl -sSL '{url}' -o '{dest}'"
        _, out, err = ssh.exec_command(dl_cmd)
        if out.channel.recv_exit_status() == 0:
            # 6) Fix ownership on the new file
            ssh.exec_command(f"sudo chown neo4j:neo4j '{dest}'")
            print(f"✔ Transferred {filename}")
        else:
            print(f"✖ Failed to transfer {filename}: {err.read().decode()}")

    ssh.close()
    

def delete_import_files(
    public_ip,
    ssh_user='ec2-user',
    ssh_port=22,
    key_path='neo4j-key-pair.pem',
    import_dir='/var/lib/neo4j/import',
    target_files=None
):
    """
    Connects via SSH and deletes files in the Neo4j import directory.

    - If target_files is a list of filenames, deletes only those.
    - If target_files is None, deletes everything in import_dir.
    """
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(public_ip, port=ssh_port, username=ssh_user, key_filename=key_path)

    # 1) Ensure import dir exists
    check_dir = f"sudo test -d {import_dir}"
    _, out, err = ssh.exec_command(check_dir)
    if out.channel.recv_exit_status() != 0:
        raise RuntimeError(f"Import directory not found: {err.read().decode()}")

    # 2) Delete files
    if target_files:
        for filename in target_files:
            path = os.path.join(import_dir, filename)
            cmd = f"sudo rm -f '{path}'"
            _, out, err = ssh.exec_command(cmd)
            if out.channel.recv_exit_status() == 0:
                print(f"✔ Deleted {filename}")
            else:
                print(f"✖ Failed to delete {filename}: {err.read().decode()}")
    else:
        # delete everything in the folder
        cmd = f"sudo rm -f {import_dir}/*"
        _, out, err = ssh.exec_command(cmd)
        if out.channel.recv_exit_status() == 0:
            print(f"✔ Deleted all files in {import_dir}")
        else:
            print(f"✖ Failed to delete files: {err.read().decode()}")

    ssh.close()


def neo4jbrowser(public_ip: str):
    """
    Opens the Neo4j browser in the default web browser.
    """
    import webbrowser
    url = f"http://{public_ip}:7474"
    webbrowser.open(url)  # this will launch the default browser to the Neo4j URL


def save_and_upload_csv(df, file_name, file_config, aws_config):
    """Save DataFrame to CSV and upload to S3"""
    fileDir_full = file_config['local_dir'] + file_name
    df.to_csv(fileDir_full, index=False, sep="|")
    s3_file_name = f"{file_config['s3_prefix']}{file_name}"
    fa.upload_file(aws_config['s3_client'], fileDir_full, file_config['s3_bucket'], s3_file_name)


def neo4j_process_save(query, file_name, neo4j_config, file_config, aws_config, process_fn=None):
    """Execute Neo4j Cypher query, optionally process DataFrame, save and upload

    Args:
        query: Cypher query string
        file_name: Output CSV file name
        neo4j_config: Dict with key: conn_neo4j (Neo4jConnection object)
        file_config: Dict with keys: local_dir, s3_bucket, s3_prefix
        aws_config: Dict with key: s3_client (needed for S3 upload)
        process_fn: Optional function to process DataFrame before saving
    """
    df = fn.neo4jToDataframe(query, neo4j_config['conn_neo4j'])
    if df is None or df.empty:
        raise ValueError(f"Neo4j query failed to return data for file: {file_name}.")
    if process_fn:
        df = process_fn(df)
    save_and_upload_csv(df, file_name, file_config, aws_config)
    return df
