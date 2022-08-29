# -*- coding: utf-8 -*-
#
from pathlib import Path
from dotenv import load_dotenv, dotenv_values

def get_own_docker_id():
    container_env_file = Path("/run/.containerenv")
    if container_env_file.exists():
        # Running in podman or CRI-O
        bits = dotenv_values(container_env_file)
        if len(bits) < 1 or 'id' not in bits:
            return get_own_docker_id_podman_altmethod()
        return bits['id']
    docker_env_file = Path("/.dockerenv")
    if docker_env_file.exists():
        return get_own_docker_id_docker_engine()


def get_own_docker_id_podman_altmethod():
    with open('/proc/self/mountinfo', 'r') as f:
        for l in f.readlines():
            if '/run/.containerenv' in l and '/containers/overlay-containers/' in l:
                line_id = l.split('/containers/overlay-containers/', 1)[-1]  # Take only text to the right
                line_id = line_id.split('/', 1)[0]  # Take only text to the left
                break
        else:
            raise RuntimeError("Cannot determine Container ID of pyOfelia container running in podman "
                               "using alternative method.")
    return line_id


def get_own_docker_id_docker_engine_altmethod():
    with open('/proc/self/mountinfo', 'r') as f:
        for l in f.readlines():
            if '/etc/hostname' in l and '/docker/containers/' in l:
                line_id = l.split('/docker/containers/', 1)[-1]  # Take only text to the right
                line_id = line_id.split('/', 1)[0]  # Take only text to the left
                break
        else:
            raise RuntimeError("Cannot determine Container ID of pyOfelia container running in docker engine "
                               "using alternative method.")
    return line_id


def get_own_docker_id_docker_engine():
    cg_lines = []
    try:
        with open("/proc/self/cgroup", 'rb') as f:
            for l in f.readlines():
                cg_lines.append(l)
    except Exception as e:
        print("Cannot read /proc/self/cgroup")
        print(e)
        cg_lines.clear()

    if len(cg_lines) < 2:
        # Is empty, or contains basic "0::/" entry
        return get_own_docker_id_docker_engine_altmethod()

    # Check for systemd cgroup driver
    for l in cg_lines:
        if b"name=systemd:" in l:
            l = l.rstrip(b'\n')
            split_point = l.rindex(b":")
            path = l[split_point + 1:]
            my_id = path.split(b"/")[-1]
            break
    else:
        my_id = None

    if my_id is None:
        # Now fallback to old method of detecting
        search_term = b"cpuset:"
        for l in cg_lines:
            if search_term in l:
                l = l.rstrip(b'\n')
                split_point = l.rindex(b":")
                path = l[split_point+1:]
                my_id = path.split(b"/")[-1]
                break
        else:
            raise LookupError("Cannot find any entries on cgroup with name=systemd or cpuset.")

    my_id_len = len(my_id)
    if my_id_len < 7:
        raise RuntimeError("PyOfelia _must_ be run in a container.")
    if my_id[:7] == b"docker-":
        my_id = my_id[7:]
    if my_id[-6:] == b".scope":
        my_id = my_id[:-6]
    if my_id == b"/" or my_id == b"":
        raise RuntimeError("PyOfelia _must_ be run in a container.")

    return my_id.decode("latin-1")

def do_load_dotenv():
    if do_load_dotenv.completed:
        return True
    load_dotenv()
    do_load_dotenv.completed = True
    return True
do_load_dotenv.completed = False
