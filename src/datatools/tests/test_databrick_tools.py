import pytest
import os
from datatools.databrick_tools import requirements_file_to_dependency_list


class TestDatabricksTools():
    """
        Tests for databrick_tools class
    """
    # Setup the test environment.
    # Will create a temporary file containing requirements
    @pytest.fixture(autouse=True)
    def setup(self, tmpdir):
        self.req_file_name = os.path.join(tmpdir.strpath, "reqs.txt")
        self.req_packages = [
            'requests',
            'ndg-httpsclient',
            'pyOpenSSL',
            'pyasn1',
            'GitPython',
            'mock',
            'mocker',
            'pytest',
            'pytest-cov'
        ]

        # Write the content of req_packages into a
        # temporary file
        fh = open(self.req_file_name, 'w')
        fh.write("\n".join(self.req_packages))
        fh.close()

        self.failing_req_file_name = os.path.join(tmpdir.strpath, "failing_reqs.txt")
        fh = open(self.failing_req_file_name, 'w')
        fh.write('''
six==1.10.0
smmap==0.9.0
traitlets==4.2.1
unicodecsv==0.14.1
requests==2.10.0
-e pytest-cov
        ''')
        fh.close()

    # Test if retrieved requirements packages are the same as the
    # requirements saved in the temporary file
    def test_requirements_parser(self):
        actual_respone = requirements_file_to_dependency_list(self.req_file_name)

        # Intersect the content of input with the output
        # self.req_packages & actual_respone
        packages_in_common = set(
            map(lambda x: x['pypi']['package'], actual_respone)
        ) \
            & set(self.req_packages)

        assert(len(packages_in_common) == len(self.req_packages))

    # Test if we throw an exception when
    # the requiements fail contains an editable library rerefence
    def test_faling_requirements_parser(self):
        with pytest.raises(NameError):
            requirements_file_to_dependency_list(self.failing_req_file_name)
