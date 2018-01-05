import time
import os
from datatools.api_client import ApiClient

class DatabricksError(Exception):

    """

    Define an error wrapper for errors having to do with databricks

    """

    def __init__(self, message):
        super(DatabricksError, self).__init__(message)

class DatabricksLibraryInstaller():

    """

    This class abstracts away the complexities of installing code libraries on databricks clusters

    """

    def __init__(self, databricks_username=None, databricks_password=None, databricks_host=None, cluster_id=None ):

        databricks_username = databricks_username or os.environ['DATABRICKS_USERNAME']
        databricks_password = databricks_password or os.environ['DATABRICKS_PASSWORD']
        databricks_host = databricks_host or os.environ['DATABRICKS_HOST']
        cluster_id = cluster_id or os.environ['DATABRICKS_CLUSTER']

        self.cluster_id = cluster_id
        self.client = ApiClient(user=databricks_username,
                                password=databricks_password, host=databricks_host)

    def _restart_cluster(self):
        data = {}
        if self.cluster_id is not None:
            data['cluster_id'] = self.cluster_id
        return self.client.perform_query('POST', '/clusters/restart', data=data)

    def _install_library(self, library):
        data = {
            "cluster_id": self.cluster_id,
            "libraries": [
                {
                    "egg": library.dbfs_path
                }
            ]
        }
        return self.client.perform_query('POST', '/libraries/install', data=data)

    def _uninstall_library(self, library):
        data = {
            "cluster_id": self.cluster_id,
            "libraries": [
                {
                    "egg": library.dbfs_path
                }
            ]
        }
        return self.client.perform_query('POST', '/libraries/uninstall', data=data)


    def check_library_state(self, lib=None):
        results = self.client.perform_query('GET', '/libraries/cluster-status?cluster_id='+self.cluster_id, data={})['library_statuses']

        if lib == None:
            return results
        else:
            for library in results:
                if 'egg' in library['library']:
                    if library['library']['egg'] == lib.dbfs_path:
                        return library['status']
        return None


    def check_cluster_state(self):
        status = self.client.perform_query('GET', '/clusters/list', data={})

        for cluster in status['clusters']:
            if cluster['cluster_id'] == self.cluster_id:
                return cluster['state']
        return None



    # This method will package and install a library on databricks. If a cluster reboot is required
    # it will perform the cluster reboot and will block until the reboot has succeeded
    # Params:
    # path -- path to the library to package and upload to databricks
    # library_namespace -- all libraries are stored in the /mnt/hired-ds/libraries sub-folder. Libraries are further namespaced
    #    and the branch is appended to the library path
    # reinstall -- if true then uninstall library and restart cluster if library is currently installed

    def uninstall_library(self, library, output=True):
        self._uninstall_library(library)
        self._restart_cluster()

        # wait for cluster to restart
        cluster_state = self.check_cluster_state()
        elapsed_time = 0
        while cluster_state != 'RUNNING':
            cluster_state = self.check_cluster_state()
            if output:
                print "Cluster state is {0} -- waiting for cluster to restart {1}".format(cluster_state, str(elapsed_time))
            time.sleep(5)
            elapsed_time += 5

        return True


    def install_library(self, library, reinstall=True, output=True):
        library_state = self.check_library_state(library)
        if library_state == 'INSTALLED' or library_state == 'FAILED':
            if reinstall == True:
                if output:
                    print("Library was previously installed on cluster. Uninstalling library.")
                self.uninstall_library(library, output)
            else:
                return False

        if output:
            print("Installing library on cluster")

        library.package_and_upload()
        self._install_library(library)

        library_state = self.check_library_state(library)
        elapsed_time = 0
        while library_state != 'INSTALLED':
            time.sleep(5)
            elapsed_time += 5
            library_state = self.check_library_state(library)
            if output:
                print "Library state is {0} -- waiting for library install {1}".format(library_state, str(elapsed_time))

        return True


class DatabricksJobRunner():

    """

    This class abstracts away the complexities of running code libraries on databricks clusters

    Note: At the moment this relies on a job being configured in the databricks UI and associated
    with a cluster with an existing library already present on it.

    """

    # Maximum number of concurrent runs of a job, as set by Databricks, is 1000 :
    # https://community.cloud.databricks.com/doc/api/
    MAX_CONCURRENT_RUNS = 1000

    def __init__(self, databricks_username=None, databricks_password=None, databricks_host=None):

        databricks_username = databricks_username or os.environ['DATABRICKS_USERNAME']
        databricks_password = databricks_password or os.environ['DATABRICKS_PASSWORD']
        databricks_host = databricks_host or os.environ['DATABRICKS_HOST']

        self.client = ApiClient(user=databricks_username,
                                password=databricks_password, host=databricks_host)

    def list_jobs(self):
        return self.client.perform_query('GET', '/jobs/list', data={})

    def check_run_progress(self, run_id, full=False):
        data = {"run_id": run_id}
        result = self.client.perform_query('GET', '/jobs/runs/get', data=data)
        if full:
            return result
        else:
            return result['state']['life_cycle_state']

    def get_job_by_name(self, name):
        jobs = self.list_jobs()
        if 'jobs' in jobs:
            jobs = jobs['jobs']

            for job in jobs:
                if job['settings']['name'] == name:
                    return job

        return None

    def update_job_libraries(self, job_name, libraries):
        job = self.get_job_by_name(job_name)

        if job is None:
            raise DatabricksError("Could not find job with name {0}".format(job_name))

        selected_job_id = job['job_id']
        job_settings = job['settings']

        job_settings['libraries'] = libraries
        # Since most of our jobs are set to max concurrent retries as 1, setting them to 1000
        # should be done alongside updating libraries so that with the next run, every job gets
        # a concurrency of 1000.
        job_settings['max_concurrent_runs'] = self.MAX_CONCURRENT_RUNS
        data = {
            "job_id": selected_job_id,
            "new_settings": job_settings
        }

        try:
            return self.client.perform_query('POST', '/jobs/reset', data=data)
        except:
            raise DatabricksError(
                """
                    There was an error while trying to reset the job.
                    Headers: {headers}
                    URL: {url}
                    Data: {data}
                """.format(headers=self.client.auth,
                           url=self.client.url + "/jobs/reset",
                           data=data))

    def create_job(self, name, notebook_path, libraries, workers=4, max_retries=1):

        # It appears at the moment that attaching a library in dbfs to a cluster can only be done
        # through the API

        data = {
            "name": name,
            "new_cluster": {
                "spark_version": "1.6.1-ubuntu15.10-hadoop1",
                "node_type_id": "compute-optimized",
                "aws_attributes": {
                    "availability": "ON_DEMAND"
                },
                "num_workers": workers
            },
            "notebook_task": {
                "notebook_path": notebook_path
            },
            "libraries": libraries,
            "max_concurrent_runs": self.MAX_CONCURRENT_RUNS,
            "email_notifications": {
                "on_start": [],
                "on_success": [],
                "on_failure": []
            },
            "max_retries": max_retries
        }

        return self.client.perform_query('POST', '/jobs/create', data=data)

    def create_or_update_job(self, job_name, databricks_notebook, libraries, workers=6, max_retries=1):
        job = self.get_job_by_name(job_name)
        if job == None:
            print("Creating job...")
            job = self.create_job(job_name, databricks_notebook, libraries, workers=workers, max_retries=max_retries)
        else:
            print("Updating job libraries...")
            self.update_job_libraries(job_name, libraries)


    def run_job(self, job_id, notebook_params):
        data = {"job_id": job_id, "notebook_params": notebook_params}

        return self.client.perform_query('POST', '/jobs/run-now', data=data)
