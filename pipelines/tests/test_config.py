# pipelines/tests/test_config.py
import os
import yaml
import pytest

# Dynamically find all config.yaml files
def get_all_config_paths(root_folder):
    config_paths = []
    for dirpath, _, files in os.walk(root_folder):
        for file in files:
            if file.endswith("config.yaml"):
                config_paths.append(os.path.join(dirpath, file))
    return config_paths


@pytest.mark.parametrize(
    "config_path",
    get_all_config_paths(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
)
def test_config_keys(config_path):  # <-- include the fixture here
    """Test required keys in config.yaml files"""
    with open(config_path) as f:
        config = yaml.safe_load(f)

    # Common required keys
    required_keys = ["layer", "table_name"]
    for key in required_keys:
        assert key in config, f"{key} missing in {config_path}"

    # Only curated layer has input_tables
    if config.get("layer") == "curated":
        assert "input_tables" in config, f"input_tables missing in curated config {config_path}"