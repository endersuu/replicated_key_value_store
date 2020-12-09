#!/usr/bin/python3
import os
import subprocess


def shell_command(shell_command='ls'):
  process = subprocess.run([shell_command],
                           capture_output=True,
                           shell=True)
  return process.stdout, process.stderr


response, _ = shell_command('ps -a')

# kill other python3 processes
for line in response.decode().split('\n'):
  if 'python3' in line or 'server.sh' in line:
    if os.getpid() == int(line[:7]):
      continue

    command = f'kill {line[:7]}'
    print(command)
    shell_command(command)

for line in shell_command('ps -a')[0].decode().split('\n'):
  print(line)

# delete 0.db 1.db 2.db 3.db
files = (f'{index}.db' for index in range(4))

for file in files:
  if os.path.exists(file):
    os.remove(file)
