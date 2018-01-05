import pytest
from datatools.library import Library
from datatools.universal_library_runner import UniversalLibraryRunner
from mock import MagicMock, call, patch


class TestUniversalLibraryRunner():

    @property
    def job_response(self):
        return {
            'created_time': 1468521347026,
            'job_id': 10,
            'settings': {
                'notebook_task': {
                    'notebook_path': '/Infrastructure/InfraScripts/ModelAttributeGenerator'
                },
                'new_cluster': {
                    'node_type_id': 'compute-optimized',
                    'num_workers': 10,
                    'spark_version': '1.6.1-ubuntu15.10-hadoop1',
                    'aws_attributes': {'availability': 'ON_DEMAND'}
                },
                'name': 'ModelAttributeBuilder',
                'timeout_seconds': 3600,
                'libraries': [{
                    'egg': 'dbfs:/mnt/hired-ds/libraries/model_attribute_generation/master/model_attribute_generation.egg'
                }],
                'max_retries': 1,
                'email_notifications': {},
                'min_retry_interval_millis': 0
            }
        }

    def perform_query_response(self, method, path, data = None, headers = None):
        return self.job_response

    @patch("datatools.universal_library_runner.UniversalLibraryRunner.get_job_by_name",
           MagicMock(return_value=None))
    def test_update_libraries(self):
        runner = UniversalLibraryRunner()
        mock = MagicMock(side_effect=self.perform_query_response)
        runner.client.perform_query = mock
        runner.create_or_update_job('job_name', 'module_name', 'method_name', {'param1': 'value1'},
                                    [{'egg': 'somenewlibrary'}])

        assert(mock.call_count == 1)
        mock.assert_has_calls([
            call('POST', '/jobs/create', data={
                'notebook_task': {
                    'base_parameters': {
                        'params': '{"param1": "value1"}',
                        'method': 'method_name',
                        'module': 'module_name'
                    },
                    'notebook_path': '/Infrastructure/InfraScripts/UniversalDatabricksRunner'
                },
                'new_cluster': {
                    'node_type_id': 'compute-optimized',
                    'num_workers': 6,
                    'spark_version': '1.6.1-ubuntu15.10-hadoop1',
                    'aws_attributes': {
                        'availability': 'ON_DEMAND'
                    }
                },
                'name': 'job_name',
                'timeout_seconds': 0,
                'max_concurrent_runs': 1000,
                'retry_on_timeout': False,
                'libraries': [{'egg': 'somenewlibrary'}]
            })
        ])

    @patch("datatools.universal_library_runner.UniversalLibraryRunner.get_job_by_name")
    def test_job_creation(self, job_getter):
        runner = UniversalLibraryRunner()
        mock = MagicMock(side_effect=self.perform_query_response)
        runner.client.perform_query = mock
        job_getter.return_value = self.job_response
        runner.create_or_update_job('job_name', 'module_name', 'method_name', {'param1': 'value1'},
                                    [{'egg': 'somenewlibrary'}])
        assert(mock.call_count == 1)
        mock.assert_has_calls([
            call('POST', '/jobs/reset', data={
                'job_id': 10,
                'new_settings': {
                    'notebook_task': {
                        'base_parameters': {
                            'params': '{"param1": "value1"}',
                            'method': 'method_name',
                            'module': 'module_name'
                        },
                        'notebook_path': '/Infrastructure/InfraScripts/UniversalDatabricksRunner'
                    },
                    'libraries': [{'egg': 'somenewlibrary'}],
                    'new_cluster': {
                        'node_type_id': 'compute-optimized',
                        'spark_version': '1.6.1-ubuntu15.10-hadoop1',
                        'aws_attributes': {'availability': 'ON_DEMAND'},
                        'num_workers': 6
                    },
                    'name': 'ModelAttributeBuilder',
                    'max_concurrent_runs': 1000,
                    'timeout_seconds': 0,
                    'max_retries': 1,
                    'retry_on_timeout': False,
                    'min_retry_interval_millis': 0
                }
            })
        ])
