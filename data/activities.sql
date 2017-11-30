 SELECT
  j.id as job_id,
  LOWER(j.name) as job_title
FROM jobs j
LEFT OUTER JOIN (
  SELECT
    CAST(o.job_id AS INT) as job_id
  FROM offers o
  LEFT OUTER JOIN (
    SELECT
      CAST(a.target_id AS INT) AS offer_id
    FROM activities a
    WHERE a.type = 'IntroducedToEmployer'
    AND a.created_at >= '2017-10-15'
  ) ii
  ON (o.id = ii.offer_id)
  WHERE (o.created_at >= '2017-10-15')
  OR (ii.offer_id IS NOT NULL)
  GROUP BY 1
  ) i
ON (CAST(j.id AS INT) = i.job_id)
LEFT OUTER JOIN (
  SELECT
    job_id
  FROM activities a
  WHERE a.created_at >= '2017-10-15'
  AND a.type = 'CandidatesListViewed'
  GROUP BY 1
  ) s
ON (j.id=s.job_id)
LEFT OUTER JOIN (
  SELECT
    job_id
  FROM activities a
  WHERE a.type = 'CandidateViewed'
  AND a.created_at >= '2017-10-15'
  GROUP BY 1
) v
ON (j.id = v.job_id)
WHERE ( i.job_id IS NOT NULL OR s.job_id IS NOT NULL OR v.job_id IS NOT NULL OR (j.created_at >= '2017-10-15'))
AND j.primary_role_id in (1,2,3,4,10,11,12,13)
AND j.company_id NOT IN (258, 257, 7480, 11342)
GROUP BY 1,2
ORDER BY 1