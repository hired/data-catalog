# To build this package -- python setup.py bdist_egg

from setuptools import setup, find_packages

setup(
    name = "Data tools",
    version = "0.1",
    description='Common code and tools for packaging, deploying and running hired data tools',
    author='Walt Schlender',
    author_email='walt@hired.com',
    packages = find_packages(),
    data_files=[],
    dependency_links=['GitPython',
                      'requests',
                      'ndg-httpsclient',
                      'pyOpenSSL',
                      'pyasn1',
                      'requirements-parser'],
    scripts=['bin/dstools']
)
