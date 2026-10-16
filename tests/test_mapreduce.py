import unittest
from workers.mapper import map_function
from workers.reducer import reduce_function
from run_local import shuffle_and_group

class TestMapReduce(unittest.TestCase):
    
    def test_mapper(self):
        # Normal sentence
        result = map_function("Hello World")
        self.assertEqual(result, [("hello", 1), ("world", 1)])
        
        # Punctuation and mixed case
        result2 = map_function("Hello, world! Hello.")
        self.assertEqual(result2, [("hello", 1), ("world", 1), ("hello", 1)])
        
        # Blank line
        self.assertEqual(map_function("   \n"), [])
        
        # Empty string
        self.assertEqual(map_function(""), [])

    def test_shuffle_and_group(self):
        mapped_data = [("hello", 1), ("world", 1), ("hello", 1)]
        grouped = shuffle_and_group(mapped_data)
        
        self.assertIn("hello", grouped)
        self.assertEqual(grouped["hello"], [1, 1])
        
        self.assertIn("world", grouped)
        self.assertEqual(grouped["world"], [1])

    def test_reducer(self):
        # Normal reduce
        self.assertEqual(reduce_function("hello", [1, 1, 1]), ("hello", 3))
        
        # Single count
        self.assertEqual(reduce_function("world", [1]), ("world", 1))
        
        # Empty values
        self.assertEqual(reduce_function("empty", []), ("empty", 0))

if __name__ == "__main__":
    unittest.main()
