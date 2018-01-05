from git import Repo
import os
import subprocess

class Library():

    """

    This class provides an abstraction around packaging and uploading libraries to S3 for use on
    databricks' cluster

    local_path = the full path to the code checked out from git that is stored on the local filesystem
    namespace = an identifier that will be used in paths eg: libraries/namespace/branch/library_name.egg
    branch = the code branch to package and upload. If not provided then default to whatever branch is current

    """

    def __init__(self, local_path, namespace, library_name, branch=None,
                 s3_path='s3://hired-ds/development/libraries', dbfs_path='dbfs:/mnt/hired-ds/development/libraries'):
        self.local_path = local_path
        self.egg_file_path = None
        self.namespace = namespace
        self.library_name = library_name
        self._dbfs_path = dbfs_path
        self._s3_path = s3_path

        if branch == None:
            self.branch = self._current_branch()
        else:
            self.branch = branch

    def _package(self):
        os.chdir(self.local_path)
        full_egg_path = None
        output_data = subprocess.check_output(['python', 'setup.py', 'bdist_egg'])
        output_file = output_data.split("creating 'dist/")[1].split(".egg")[0] + ".egg"
        full_egg_path = self.local_path + "/dist/" + output_file

        self.egg_file_path = full_egg_path

    def _current_branch(self):
        repo = Repo(self.local_path)
        branch = repo.active_branch
        branch_name = branch.name
        return branch_name

    def _upload_to_s3(self):
        if self.egg_file_path == None:
            self._package()
        self.egg_name = self.egg_file_path.rsplit('/', 1)[1]
        response = subprocess.check_output(['aws', 's3', 'cp', self.egg_file_path, self.s3_path])

    @property
    def dbfs_path(self):
        return self._dbfs_path + '/' + self.namespace + '/' + self.branch + '/' + self.library_name + '.egg'

    @property
    def s3_path(self):
        return self._s3_path + '/' + self.namespace + '/' + self.branch + '/' + self.library_name + '.egg'

    def package_and_upload(self):
        self._package()
        self._upload_to_s3()