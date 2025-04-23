import unittest
from unittest.mock import patch
from main import extract_data, transform_data

class TestETL(unittest.TestCase):

    @patch("main.requests.get")
    def test_extract_data_success(self, mock_get):
        fake_response = [{"id": 1, "title": "Fake Product", "price": 100, "category": "electronics", "description": "desc", "image": "img.jpg", "rating": {"rate": 4.5}}]
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = fake_response

        result = extract_data()
        self.assertIsInstance(result, list)
        self.assertEqual(result[0]["title"], "Fake Product")

    def test_transform_data_structure(self):
        input_data = [
            {
                "id": 1,
                "title": "Test",
                "price": 9.99,
                "category": "books",
                "description": "A test product",
                "image": "image.jpg",
                "rating": {"rate": 4.2}
            }
        ]

        result = transform_data(input_data)

        self.assertIn("sales", result)
        self.assertIn("product", result)
        self.assertIn("finance", result)
        self.assertEqual(len(result["sales"]), 1)
        self.assertEqual(result["sales"][0]["title"], "Test")

if __name__ == "__main__":
    unittest.main()
