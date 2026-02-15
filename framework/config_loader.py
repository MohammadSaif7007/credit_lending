import yaml

def load_config(path):
    """Load YAML config file"""
    with open(path) as f:
        return yaml.safe_load(f)