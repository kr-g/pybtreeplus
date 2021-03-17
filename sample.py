import unittest

from tests.test_default import *

suite = unittest.TestLoader().loadTestsFromName(
    "test_insert_split_leaf_2", module=BTreePlusDefaultTestCase
)
unittest.TextTestRunner().run(suite)
