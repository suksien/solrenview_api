import paramiko
import pandas as pd

# sftp connection params
hostname = 'sftp.firstsolar.com'
port = 22
username = 'utoledo'
password = 'fD3GeH9j:IN^'

# create SSH client
# does not work in MacOS, only in windows 11 so far
client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

client.connect(hostname, port, username, password)

# create sftp session
sftp = client.open_sftp()

homedir = '/home/university of Toledo/'
files = sftp.listdir(homedir)

met_files = [file for file in files if 'MET 2026' in file]
latest_met_file = met_files[-1]

sftp.get(homedir + latest_met_file, latest_met_file)

df = pd.read_csv(latest_met_file, low_memory=False)
df.to_csv(latest_met_file + '.txt')