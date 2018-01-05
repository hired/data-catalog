import unittest

from catalog.dao.data_sources import get


class DataSourceTest(unittest.TestCase):
    def test_get_valid_data_source(self):
        local_db_url = get(source_name="local")
        expected = "jdbc:postgresql://localhost:5432/hired_dev?user=postgres&password=HIRED"
        self.assertEqual(expected, local_db_url)

    def test_get_valid_data_source_with_extra_params(self):
        local_db_url = get(source_name="follower")
        expected = "jdbc:postgresql://ec2-34-202-89-194.compute-1.amazonaws.com:" \
                   "5432/d95bs1r2i3i05q?user=u91k8jkdq669sj" \
                   "&password=p2u2opl1gsnlrnamfjm6fjuea8j&sslmode=require"
        self.assertEqual(expected, local_db_url)

    def test_get_invalid_datasource(self):
        with self.assertRaises(AttributeError) as context:
            get(source_name="invalid")
        self.assertTrue(isinstance(context.exception, AttributeError),
                        msg="Method is expected to throw AttributeError")
