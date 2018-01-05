import os
import json
from datatools.api_client import ApiClient

class DatabricksError(Exception):

    """

    Define an error wrapper for errors having to do with databricks

    """

    def __init__(self, message):
        super(DatabricksError, self).__init__(message)

class UniversalLibraryRunner:

    """

    This class abstracts away the complexities of running code libraries on databricks clusters

    Note: At the moment this relies on a job being configured in the databricks UI and associated
    with a cluster with an existing library already present on it.

    """

    UNIVERSAL_RUNNER_PATH = '/Infrastructure/InfraScripts/UniversalDatabricksRunner'
    SPARK_VERSION = '1.6.1-ubuntu15.10-hadoop1'
    DEFAULT_WORKERS = 6
    MAX_CONCURRENT_RUNS = 1000

    def __init__(self, databricks_username=None, databricks_password=None, databricks_host=None ):

        databricks_username = databricks_username or os.environ['DATABRICKS_USERNAME']
        databricks_password = databricks_password or os.environ['DATABRICKS_PASSWORD']
        databricks_host = databricks_host or os.environ['DATABRICKS_HOST']

        self.client = ApiClient(user=databricks_username,
                                password=databricks_password, host=databricks_host)

    def get_job_by_name(self, name):
        jobs = self.__list_jobs()
        if 'jobs' in jobs:
            jobs = jobs['jobs']

            for job in jobs:
                if job['settings']['name'] == name:
                    return job

        return None

    def create_or_update_job(self,
                             name,
                             module_name,
                             method_name,
                             method_params,
                             libraries,
                             workers=DEFAULT_WORKERS,
                             timeout_seconds=0,
                             retry_on_timeout=False,
                             cron_settings=None,
                             email_notifications=None):
        job = self.get_job_by_name(name)
        base_parameters = {"module": module_name, "method": method_name, "params": json.dumps(method_params)}
        if job == None:
            print("Creating job...")
            job = self.__create_job(name=name,
                                    notebook_path=self.UNIVERSAL_RUNNER_PATH,
                                    base_parameters=base_parameters,
                                    libraries=libraries,
                                    workers=workers,
                                    timeout_seconds=timeout_seconds,
                                    retry_on_timeout=retry_on_timeout,
                                    cron_settings=cron_settings,
                                    email_notifications=email_notifications)
        else:
            print("Updating job...")
            job = self.__update_job(name=name,
                                    job=job,
                                    notebook_path=self.UNIVERSAL_RUNNER_PATH,
                                    base_parameters=base_parameters,
                                    libraries=libraries,
                                    workers=workers,
                                    timeout_seconds=timeout_seconds,
                                    retry_on_timeout=retry_on_timeout,
                                    cron_settings=cron_settings,
                                    email_notifications=email_notifications)


    def run_job(self, job_id):
        data = {"job_id": job_id}

        return self.client.perform_query('POST', '/jobs/run-now', data=data)


    def __list_jobs(self):
        return self.client.perform_query('GET', '/jobs/list', data={})


    def __create_job(self,
                     name,
                     notebook_path,
                     base_parameters,
                     libraries,
                     workers=DEFAULT_WORKERS,
                     timeout_seconds=0,
                     retry_on_timeout=False,
                     cron_settings=None,
                     email_notifications=None):

        # It appears at the moment that attaching a library in dbfs to a cluster can only be done through the API

        data = {
            "name": name,
            "new_cluster": {
                "spark_version": self.SPARK_VERSION,
                "node_type_id": "compute-optimized",
                "aws_attributes": {
                    "availability": "ON_DEMAND"
                },
                "num_workers": workers
            },
            "notebook_task": {
                "notebook_path": notebook_path,
                "base_parameters": base_parameters
            },
            "libraries": libraries,
            "max_concurrent_runs": self.MAX_CONCURRENT_RUNS,
            "retry_on_timeout" : retry_on_timeout,
            "timeout_seconds": timeout_seconds
        }

        if cron_settings:
            data["schedule"] = cron_settings

        if email_notifications:
            data["email_notifications"] = email_notifications

        return self.client.perform_query('POST', '/jobs/create', data=data)


    def __update_job(self,
                     name,
                     job,
                     notebook_path,
                     base_parameters,
                     libraries,
                     workers=DEFAULT_WORKERS,
                     timeout_seconds=0,
                     retry_on_timeout=False,
                     cron_settings=None,
                     email_notifications=None):
        selected_job_id = job['job_id']
        job_settings = job['settings']

        job_settings['libraries'] = libraries
        job_settings['max_concurrent_runs'] = self.MAX_CONCURRENT_RUNS
        job_settings['notebook_task']['notebook_path'] = notebook_path
        job_settings['notebook_task']['base_parameters'] = base_parameters
        job_settings['new_cluster']['num_workers'] = workers
        job_settings['timeout_seconds'] = timeout_seconds
        job_settings['retry_on_timeout'] = retry_on_timeout

        if cron_settings == None and 'schedule' in job_settings:
            del job_settings['schedule']
        elif cron_settings:
            job_settings['schedule'] = cron_settings

        if email_notifications == None and 'email_notifications' in job_settings:
            del job_settings['email_notifications']
        elif email_notifications:
            job_settings['email_notifications'] = email_notifications

        data = {
            "job_id": selected_job_id,
            "new_settings": job_settings
        }

        return self.client.perform_query('POST', '/jobs/reset', data=data)
