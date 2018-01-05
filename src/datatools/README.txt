


Data Tools README
==================

This library extracts common tasks done across repos and code-bases.

This includes:

1. Packaging code into libraries programmatically
2. Running packaged code on a spark cluster
3. Checking out code from git


To set up data tools so that you can install and run libraries on databricks clusters you'll need to do the following:

Modify your environment variables to include the following -- It is recommended that you do this in your ~/.bash_profile
file in your home directory so that you only have to do this once.

export DATABRICKS_USERNAME='username'
export DATABRICKS_PASSWORD='password'
export DATABRICKS_HOST='https://dbricksid.cloud.databricks.com'
export DATABRICKS_CLUSTER = '0711-175915-poet2'
export DATABRICKS_DEVELOPER = 'yourname'

username and password are the username and password you log into the databricks site with.
The host name is the url of the login site for databricks.

To find the cluster_id you'll need to click into a cluster via the databricks UI. The cluster id is visible in the URL.
For example:

https://ourcould.cloud.databricks.com/#setting/clusters/0711-175915-poet2/configuration

Has a cluster id of: 0711-175915-poet2

Finally you'll want to source your .bash_profile -- source ~/.bash_profile

You will need to run this command in all terminal windows you wish to update the environment variables in


That's it. You should now be able to use our databricks library deployment tools.