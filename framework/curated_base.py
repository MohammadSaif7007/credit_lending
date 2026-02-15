# framework/curated_base.py
from pyspark.sql import DataFrame

class CuratedTableTransform:
    """
    Base class for all curated tables.
    Subclasses must implement the self.df transformations in methods starting with 'step_'.
    """

    def __init__(self, df: DataFrame):
        self.df = df

    def execute(self):
        """
        Dynamically execute all transformation methods in the order they are defined.
        Any method starting with 'step_' will be executed on self.df.
        """
        # Get all methods starting with 'step_' in the order defined in the class
        steps = [getattr(self, func) for func in dir(self) if callable(getattr(self, func)) and func.startswith("step_")]

        for step in steps:
            step()  # updates self.df in place