from .ganymed import GanymedServer


def _jupyter_server_extension_points():
    return [{"module": "ganymed", "app": GanymedServer}]