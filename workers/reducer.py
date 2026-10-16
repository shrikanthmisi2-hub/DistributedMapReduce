"""
Reducer module.
Contains the logic for the Reduce phase (e.g., aggregating key-value pairs to produce final output).
"""

from typing import List, Tuple

def reduce_function(key: str, values: List[int]) -> Tuple[str, int]:
    """
    Takes a word and a list of counts, returns the word and the sum of counts.
    """
    if not key:
        return ("", 0)
    
    total_count = sum(values)
    return (key, total_count)
