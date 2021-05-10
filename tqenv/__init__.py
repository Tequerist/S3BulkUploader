from os import environ, path


def load_env(env_file):
    if path.isfile(env_file):
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if not line.startswith('#'):
                    config = line.split('=', 1)
                    if len(config) == 2:
                        environ.setdefault(config[0], config[1])


def get_ev(e, default=None): return environ[e] if e in environ else default
