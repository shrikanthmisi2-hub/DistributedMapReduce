"""
Mapper module.
Contains the logic for the Map phase (e.g., splitting input data into key-value pairs).
"""

import re
from typing import List, Tuple

def map_function(line: str) -> List[Tuple[str, int]]:
    """
    Takes a line of text, tokenizes it into words, and returns a list of (word, 1) pairs.
    Handles punctuation and converts to lowercase.
    """
    if not line or not line.strip():
        return []
    
    # Use regex to find words (alphanumeric characters)
    words = re.findall(r'\b\w+\b', line.lower())
    
    # Return (word, 1) for each word found
    return [(word, 1) for word in words]
