from tasks import ETL
import unittest


class TestETLMethods(unittest.TestCase):
    def setUp(self):
        self.etl = ETL()
        self.etl.max_stringlen = 40


    def test_transform_field(self):
        max_length_string = '0123456789abcdefghijklmnopqrstuvwxyz,./;'
        test_cases = (
            ('stuff', 'stuff'),
            (['stuff', 'stuff'], "['stuff', 'stuff']"),
            (set(['stuff', 'stuff2']), "['stuff', 'stuff2']"),
            (max_length_string + '[', max_length_string),
            (46, 46),
            ([u'stuff', u'stuff'], "[u'stuff', u'stuff']"),
            ([u'stuff', 'stuff'], "[u'stuff', 'stuff']"),
            (None, None),
        )
        for test_case in test_cases:
            self.assertEqual(
                self.etl.transform_field(test_case[0]),
                test_case[1]
            )


if __name__ == '__main__':
    unittest.main()
