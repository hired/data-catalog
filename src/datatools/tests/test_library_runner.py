
import pytest
from datatools.library import Library
from datatools.library_runner import DatabricksLibraryInstaller, DatabricksJobRunner, DatabricksError
from mock import MagicMock, call



class TestDatabricksLibraryInstaller():

    """

    These tests test the library_runner class

    """

    def setup_method(self, method):
        self.library = Library('', 'attribute_generation', 'model_attribute_generator-0.2-py2.7', branch='master')


    # TEST CLUSTER STATE METHODS
    # Test that we can extract information about the running cluster from databricks

    def test_check_cluster_state_happy(self):
        runner = DatabricksLibraryInstaller('','','', '0711-175915-poet2')
        runner.client.perform_query = MagicMock(return_value={u'clusters': [{u'node_type_id': u'memory-optimized', u'state_message': u'', u'cluster_memory_mb': 153600, u'terminated_time': 0, u'executors': [{u'node_id': u'0a2c44b3ad2c4678bb9f60964e52fd35', u'public_dns': u'ec2-54-88-146-35.compute-1.amazonaws.com', u'node_aws_attributes': {u'is_spot': True}}, {u'node_id': u'317f148730f243b2bee25370d8ae9030', u'public_dns': u'ec2-52-23-237-67.compute-1.amazonaws.com', u'node_aws_attributes': {u'is_spot': True}}, {u'node_id': u'c3d7251bf8cd4bbebd387efd7af704c5', u'public_dns': u'ec2-52-23-237-67.compute-1.amazonaws.com', u'node_aws_attributes': {u'is_spot': True}}, {u'node_id': u'e84b55183416481ea23ff7902598d2de', u'public_dns': u'ec2-54-88-146-35.compute-1.amazonaws.com', u'node_aws_attributes': {u'is_spot': True}}], u'last_state_loss_time': 1468345041090, u'jdbc_port': 10000, u'num_workers': 4, u'driver': {u'node_id': u'db0c7363ee6f4205a51c4b4da66d4558', u'public_dns': u'ec2-54-164-94-169.compute-1.amazonaws.com', u'node_aws_attributes': {u'is_spot': False}}, u'cluster_cores': 55.9, u'cluster_name': u'TestCluster', u'state': u'RUNNING', u'spark_version': u'1.6.1-ubuntu15.10-hadoop1', u'cluster_id': u'0711-175915-poet2', u'start_time': 1468259955307, u'aws_attributes': {u'availability': u'SPOT_WITH_FALLBACK', u'zone_id': u'us-east-1b', u'first_on_demand': 0}}]})

        assert(runner.check_cluster_state()) == 'RUNNING'


    # Test that None is returned when the cluster is not found

    def test_check_cluster_state_no_cluster(self):
        runner = DatabricksLibraryInstaller('','','', 'no_cluster')
        runner.client.perform_query = MagicMock(return_value={u'clusters': [{u'node_type_id': u'memory-optimized', u'state_message': u'', u'cluster_memory_mb': 153600, u'terminated_time': 0, u'executors': [{u'node_id': u'0a2c44b3ad2c4678bb9f60964e52fd35', u'public_dns': u'ec2-54-88-146-35.compute-1.amazonaws.com', u'node_aws_attributes': {u'is_spot': True}}, {u'node_id': u'317f148730f243b2bee25370d8ae9030', u'public_dns': u'ec2-52-23-237-67.compute-1.amazonaws.com', u'node_aws_attributes': {u'is_spot': True}}, {u'node_id': u'c3d7251bf8cd4bbebd387efd7af704c5', u'public_dns': u'ec2-52-23-237-67.compute-1.amazonaws.com', u'node_aws_attributes': {u'is_spot': True}}, {u'node_id': u'e84b55183416481ea23ff7902598d2de', u'public_dns': u'ec2-54-88-146-35.compute-1.amazonaws.com', u'node_aws_attributes': {u'is_spot': True}}], u'last_state_loss_time': 1468345041090, u'jdbc_port': 10000, u'num_workers': 4, u'driver': {u'node_id': u'db0c7363ee6f4205a51c4b4da66d4558', u'public_dns': u'ec2-54-164-94-169.compute-1.amazonaws.com', u'node_aws_attributes': {u'is_spot': False}}, u'cluster_cores': 55.9, u'cluster_name': u'TestCluster', u'state': u'RUNNING', u'spark_version': u'1.6.1-ubuntu15.10-hadoop1', u'cluster_id': u'0711-175915-poet2', u'start_time': 1468259955307, u'aws_attributes': {u'availability': u'SPOT_WITH_FALLBACK', u'zone_id': u'us-east-1b', u'first_on_demand': 0}}]})

        assert(runner.check_cluster_state()) == None


    # TEST LIBRARY STATE METHODS
    # Test that we can extract state information about the installed libraries from databricks

    def test_check_library_state_happy(self):
        runner = DatabricksLibraryInstaller('','','', '0711-175915-poet2')
        runner.client.perform_query = MagicMock(return_value={u'library_statuses': [{u'status': u'INSTALLED', u'is_library_for_all_clusters': False, u'library': {u'pypi': {u'package': u'geopy'}}}, {u'status': u'INSTALLED', u'is_library_for_all_clusters': False, u'library': {u'pypi': {u'package': u'pandas'}}}, {u'status': u'INSTALLED', u'is_library_for_all_clusters': False, u'library': {u'pypi': {u'package': u'numpy'}}}, {u'status': u'INSTALLED', u'is_library_for_all_clusters': False, u'library': {u'pypi': {u'package': u'scikit-learn'}}}, {u'status': u'INSTALLED', u'is_library_for_all_clusters': False, u'library': {u'pypi': {u'package': u'scipy'}}}, {u'status': u'INSTALLED', u'is_library_for_all_clusters': False, u'library': {u'egg': u'dbfs:/mnt/hired-ds/development/libraries/attribute_generation/master/model_attribute_generator-0.2-py2.7.egg'}}], u'cluster_id': u'0711-175915-poet2'})

        assert(runner.check_library_state(self.library)) == 'INSTALLED'

    # Test that none is returned when on library is found

    def test_check_library_state_no_library(self):
        runner = DatabricksLibraryInstaller('','','', '0711-175915-poet2')
        runner.client.perform_query = MagicMock(return_value={u'library_statuses': [{u'status': u'INSTALLED', u'is_library_for_all_clusters': False, u'library': {u'pypi': {u'package': u'geopy'}}}, {u'status': u'INSTALLED', u'is_library_for_all_clusters': False, u'library': {u'pypi': {u'package': u'pandas'}}}, {u'status': u'INSTALLED', u'is_library_for_all_clusters': False, u'library': {u'pypi': {u'package': u'numpy'}}}, {u'status': u'INSTALLED', u'is_library_for_all_clusters': False, u'library': {u'pypi': {u'package': u'scikit-learn'}}}, {u'status': u'INSTALLED', u'is_library_for_all_clusters': False, u'library': {u'pypi': {u'package': u'scipy'}}}, {u'status': u'INSTALLED', u'is_library_for_all_clusters': False, u'library': {u'egg': u'dbfs:/mnt/hired-ds/libraries/attribute_generation/master/model_attribute_generator-0.2-py2.7.egg'}}], u'cluster_id': u'0711-175915-poet2'})

        self.library.library_name = 'not_installed'

        assert(runner.check_library_state(self.library)) == None


class TestDatabricksJobRunner():

    def perform_query_response(self, method, path, data = None, headers = None):
        return {u'jobs': [{u'created_time': 1468521347026, u'job_id': 10, u'settings': {u'notebook_task': {u'notebook_path': u'/Infrastructure/InfraScripts/ModelAttributeGenerator'}, u'new_cluster': {u'node_type_id': u'compute-optimized', u'num_workers': 10, u'spark_version': u'1.6.1-ubuntu15.10-hadoop1', u'aws_attributes': {u'availability': u'ON_DEMAND'}}, u'name': u'ModelAttributeBuilder', u'timeout_seconds': 3600, u'libraries': [{u'egg': u'dbfs:/mnt/hired-ds/libraries/model_attribute_generation/master/model_attribute_generation.egg'}], u'max_retries': 1, u'email_notifications': {}, u'min_retry_interval_millis': 0}}, {u'created_time': 1468360311863, u'job_id': 4, u'settings': {u'notebook_task': {u'notebook_path': u'/Infrastructure/InfraScripts/ModelAttributeGenerator'}, u'existing_cluster_id': u'0711-175915-poet2', u'timeout_seconds': 0, u'name': u'ModelAttributeGenerator', u'email_notifications': {}}}]}

    def test_update_libraries_job_not_found(self):
        runner = DatabricksJobRunner()
        mock = MagicMock(side_effect=self.perform_query_response)
        runner.client.perform_query = mock

        # Test library not found

        with pytest.raises(DatabricksError):
            runner.update_job_libraries('UNDEFINED_JOB', [{'egg': 'somenewlibrary'}])

    def test_update_library_update_failure(self):
        def raise_exception_for_reset(request_type, url, data):
            if url == '/jobs/reset':
                raise Exception('Boom!')
            else:
                return self.perform_query_response('', '')
        runner = DatabricksJobRunner()
        mock = MagicMock(side_effect=raise_exception_for_reset)
        runner.client.perform_query = mock
        with pytest.raises(DatabricksError) as excinfo:
            runner.update_job_libraries('ModelAttributeBuilder', [{'egg': 'somenewlibrary'}])
        assert 'There was an error while trying to reset the job.' in str(excinfo.value)

    def test_update_libraries_happy_path(self):
        runner = DatabricksJobRunner()
        mock = MagicMock(side_effect=self.perform_query_response)
        runner.client.perform_query = mock
        runner.update_job_libraries('ModelAttributeBuilder', [{'egg': 'somenewlibrary'}])

        assert(mock.call_count == 2)
        mock.assert_has_calls([
            call('GET', '/jobs/list', data={}),
            call('POST', '/jobs/reset', data={
                'job_id': 10,
                'new_settings': {
                    'notebook_task': {
                        'notebook_path': '/Infrastructure/InfraScripts/ModelAttributeGenerator'
                    },
                    'libraries': [{'egg': 'somenewlibrary'}],
                    'new_cluster': {
                        'node_type_id': 'compute-optimized',
                        'spark_version': '1.6.1-ubuntu15.10-hadoop1',
                        'aws_attributes': {'availability': 'ON_DEMAND'},
                        'num_workers': 10
                    },
                    'max_concurrent_runs': 1000,
                    'name': 'ModelAttributeBuilder',
                    'email_notifications': {},
                    'timeout_seconds': 3600,
                    'max_retries': 1,
                    'min_retry_interval_millis': 0
                }
            })
        ])

    def test_job_creation(self):
        runner = DatabricksJobRunner()
        mock = MagicMock(side_effect=self.perform_query_response)
        runner.client.perform_query = mock
        runner.create_job('SomeJobName', '/some/notebook/path',
                          [{'egg': 'somenewlibrary'}])
        assert(mock.call_count == 1)
        mock.assert_has_calls([
            call('POST', '/jobs/create', data={
                'notebook_task': {'notebook_path': '/some/notebook/path'},
                'new_cluster': {
                    'node_type_id': 'compute-optimized',
                    'num_workers': 4,
                    'spark_version': '1.6.1-ubuntu15.10-hadoop1',
                    'aws_attributes': {'availability': 'ON_DEMAND'}
                },
                'name': 'SomeJobName',
                'max_concurrent_runs': 1000,
                'libraries': [{'egg': 'somenewlibrary'}],
                'max_retries': 1,
                'email_notifications': {
                    'on_failure': [],
                    'on_success': [],
                    'on_start': []
                }
            })
        ])
