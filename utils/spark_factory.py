from functools import lru_cache

from pyspark.sql import SparkSession
import boto3


@lru_cache(10)
def _load_aws_credentials():
    session = boto3.Session()
    credentials = session.get_credentials()
    if not (credentials.access_key or credentials.secret_key):
        raise AttributeError("Could not find AWS credentials.Please set up aws credentials, using "
                             "aws configure")
    return credentials


@lru_cache(10)
def spark_conf(master="local", application_name="DistributeThis"):
    credentials = _load_aws_credentials()
    spark = SparkSession.builder \
        .master(master) \
        .appName(application_name) \
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:2.7.3,org.postgresql:postgresql:42.1.1") \
        .getOrCreate()
    spark._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", credentials.access_key)
    spark._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey",
                                         credentials.secret_key)
    return spark


spark = spark_conf()

url = '***REMOVED***://localhost:5432/hired_dev?user=postgres&password=HIRED'
pushdown_query = """(WITH candidate_roles AS
(SELECT
   u.id,
   json_agg((r.id, r.name)) AS roles
 FROM users u LEFT OUTER JOIN candidate_role_selections rs
     ON u.id = rs.candidate_id
   LEFT OUTER JOIN roles r
     ON rs.role_id = r.id
 WHERE u.type = 'Candidate'
 GROUP BY u.id),
    places AS (
      SELECT
        u.id,
        json_agg(p1.*) AS places
      FROM users u LEFT OUTER JOIN places p1
          ON u.place_id = p1.id
      WHERE u.type = 'Candidate'
      GROUP BY u.id),
    selected_places AS
  (SELECT
     u.id,
    json_agg(ps.place_id) as selected_places
   FROM users u LEFT OUTER JOIN place_selections ps
       ON ps.candidate_id = u.id
   WHERE u.type = 'Candidate'
   GROUP BY u.id),
    skills AS (
      SELECT
        u.id,
        json_agg((s.name, pr.name)) AS skills
      FROM users u LEFT OUTER JOIN candidate_skill_selections ss
          ON ss.candidate_id = u.id
        LEFT OUTER JOIN skills s
          ON ss.skill_id = s.id
        LEFT OUTER JOIN primary_roles pr
          ON s.primary_role_id = pr.id
      WHERE u.type = 'Candidate'
      GROUP BY u.id)

SELECT
  u.id as candidate_id,
  cp.*,
  r.roles,
  s.skills,
  p.places,
  ps.selected_places
FROM (SELECT *
      FROM users
      WHERE type = 'Candidate' AND id = 952634) u
  LEFT OUTER JOIN candidate_profiles cp
    ON cp.id = u.profile_id
  LEFT JOIN candidate_roles r
    ON r.id = u.id
  LEFT JOIN places p
    ON p.id = u.id
  LEFT JOIN selected_places ps
    ON ps.id = u.id
  LEFT JOIN skills s
    ON r.id = u.id
) emp_alias"""
df = spark.read.jdbc(url=url, table=pushdown_query, properties={'driver': 'org.postgresql.Driver'})
df.show(10)
