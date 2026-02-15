import importlib

def load_helper(layer):
    return importlib.import_module(f"pipelines.{layer}.python_helper")

def load_transform(table, layer="curated"):
    return importlib.import_module(f"pipelines.{layer}.{table}.transformation")