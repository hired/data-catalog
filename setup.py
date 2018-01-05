from setuptools import setup, find_packages

setup(
    name="data-catalog",
    version="0.0.1",
    description='Catalog of Hired Domain models',
    author='Prad',
    author_email='pradheep.raju@hired.com',
    packages=['catalog'],
    package_data={'catalog': ['dao/queries/*.sql']},
    install_requires=['PyYAML',
                      'boto3==1.4.8',
                      'botocore==1.8.5',
                      'click==6.7',
                      'py4j==0.10.4',
                      'pyspark==2.2.0',
                      'python-dateutil==2.6.1',
                      's3transfer==0.1.12',
                      'six==1.11.0'],
    zip_safe=False
)
