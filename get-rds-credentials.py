#!/usr/bin/env python3
import functools
import json
import os
import shlex
import subprocess
import sys
from typing import Dict, List, Optional

GET_INSTANCES_SH = (
    """sh -c '{aws} rds describe-db-instances | {jq} ".DBInstances[].Endpoint" -c'"""
)
GET_CREDENTIALS_COMMAND = """{aws} rds generate-db-auth-token --hostname {hostname} --port {port} --username {username} --region eu-central-1 """
FZF_COMMAND = """{fzf} --header 'Choose a server to get the credentials'"""

REQUIRED_COMMANDS = {
    "aws": "awscli is missing",
    "fzf": "fzf is missing",
    "jq": "jq is missing",
}


def main(username):
    ensure_commands()
    instances = list(get_instances())
    instance_name = choose_instances(instances)
    instance, *_ = [
        instance for instance in instances if instance["Address"] == instance_name
    ]
    print_credentials(instance, username)


def ensure_commands():
    missing_commands = []
    for command, description in REQUIRED_COMMANDS.items():
        if not which(command):
            missing_commands.append(description)
    if missing_commands:
        print(
            "You must install a set of commands for this script to work.\n{}".format(
                "\n".join(missing_commands)
            ),
            file=sys.stderr,
        )
        sys.exit(1)


@functools.lru_cache()
def which(command) -> Optional[str]:
    paths = os.environ["PATH"].split(os.pathsep)
    for path in paths:
        full_path = os.path.join(path, command)
        if os.path.exists(full_path):
            return full_path
    return None


def get_instances():
    command = GET_INSTANCES_SH.format(**get_commands_paths(["aws", "jq"]))
    output = subprocess.check_output(shlex.split(command))
    for line in output.decode().splitlines():
        yield json.loads(line)


def get_commands_paths(commands: List[str]) -> Dict[str, str]:
    return {command: shlex.quote(which(command)) for command in commands}  # type: ignore


def choose_instances(instances):
    command = FZF_COMMAND.format(**get_commands_paths(["fzf"]))
    process = subprocess.Popen(
        shlex.split(command), stdin=subprocess.PIPE, stdout=subprocess.PIPE
    )
    process.stdin.writelines(
        ["{}\n".format(instance["Address"]).encode() for instance in instances]
    )
    process.stdin.close()
    process.wait()
    return process.stdout.read().decode().strip()


def print_credentials(instance, username):
    command = GET_CREDENTIALS_COMMAND.format(
        hostname=instance["Address"],
        port=instance["Port"],
        username=username,
        **get_commands_paths(["aws"])
    )
    output = subprocess.check_output(shlex.split(command))
    print(output.decode())


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(
            "You must specify a username e.g.: {} username".format(sys.argv[0]),
            file=sys.stderr,
        )
        exit(1)
    main(sys.argv[1])
