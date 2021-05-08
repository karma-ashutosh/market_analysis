import errno
import os
import subprocess
import threading


def create_dir_if_not_exists(file_name):
    os.makedirs(os.path.dirname(file_name), exist_ok=True)


def delete_file_silently(file_path):
    try:
        os.remove(file_path)
    except OSError as e:
        if e.errno != errno.ENOENT:  # errno.ENOENT: no such file or directory
            raise e


def execute_command_and_get_console_output(command: str) -> list:
    command = command.split(" ")
    stdout = str(subprocess.Popen(command, stdout=subprocess.PIPE).stdout.read())
    lines = stdout.split("\\n")
    return lines


def run_in_background(job, args=()):
    threading.Thread(target=job, args=args).start()
