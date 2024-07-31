import os
import platform
import re
import paramiko
import requests

class UpdateID:

    def __init__(self, SERVER_IP, SERVER_PASSWORD, postgresql_version):
        self.SERVER_IP = SERVER_IP
        self.SERVER_PASSWORD = SERVER_PASSWORD
        self.postgresql_version = postgresql_version


    def set_env_variable(self, var_name, value):
        current_os = platform.system()
        if os.getenv(var_name) is None:
            if current_os == 'Windows':
                os.environ[var_name] = value
                os.system(f'setx {var_name} "{value}"')
            elif current_os in ['Linux']:
                os.environ[var_name] = value
                with open(os.path.expanduser("~/.bashrc"), 'a') as bashrc:
                    bashrc.write('\nexport {var_name}="{value}"\n'.format(var_name=var_name, value=value))
                os.system('source ~/.bashrc')

    def get_curr_conf_ip(self, ssh, PG_HBA_CONF_PATH, username):
        awk_cmd = f"awk '/^[[:space:]]*host/{{if ($0 ~ /{username}/) print $0}}' {PG_HBA_CONF_PATH}"
        stdin, stdout, stderr = ssh.exec_command(awk_cmd)
        awk_result = stdout.read().decode('utf-8').strip()

        TARGET_IP_ADDRESS = ''
        match = re.search(r'\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}/32\b', awk_result)
        if match:
            TARGET_IP_ADDRESS = match.group(0)

        return TARGET_IP_ADDRESS

    def update_conf(self, ssh, TARGET_IP_ADDRESS, PG_HBA_CONF_PATH, CURRENT_IP_ADDRESS, username):
        if TARGET_IP_ADDRESS:
            sed_cmd = f"sudo sed -i '/{username}/d' {PG_HBA_CONF_PATH}"
            ssh.exec_command(sed_cmd)
            # stderr.read().decode('utf-8').strip()

        ssh_cmd = f"echo 'host    all             all             {CURRENT_IP_ADDRESS}/32        scram-sha-256  # {username}' | sudo tee -a {PG_HBA_CONF_PATH} && sudo systemctl restart postgresql"
        ssh.exec_command(ssh_cmd)

    def main(self):
        SERVER_IP = self.SERVER_IP
        SERVER_PASSWORD = self.SERVER_PASSWORD

        self.set_env_variable('SERVER_PASSWORD', SERVER_PASSWORD)

        SERVER_USER = os.getlogin()
        PG_HBA_CONF_PATH = f'/etc/postgresql/{self.postgresql_version}/main/pg_hba.conf'

        response = requests.get('http://ipinfo.io/ip')
        CURRENT_IP_ADDRESS = response.text.strip()

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(SERVER_IP, username='root', password=SERVER_PASSWORD)

        TARGET_IP_ADDRESS = self.get_curr_conf_ip(ssh, PG_HBA_CONF_PATH, SERVER_USER)
        self.update_conf(ssh, TARGET_IP_ADDRESS, PG_HBA_CONF_PATH, CURRENT_IP_ADDRESS, SERVER_USER)

        ssh.close()


