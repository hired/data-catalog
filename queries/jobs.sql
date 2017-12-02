(WITH
    latest_activities AS (
      SELECT
        a.type,
        cast(replace(json_extract_path(a.data, 'job_id')::TEXT, '"', '') AS INT) AS job_id,
        CAST(a.target_id AS INT)                                                   AS offer_id
      FROM activities a
      WHERE a.created_at >= current_date - INTERVAL '30' DAY AND
            json_extract_path(a.data, 'job_id') IS NOT NULL
  ),
    this_offers AS (
    (
      SELECT CAST(o.job_id AS INT) AS job_id
      FROM offers o
        JOIN (
               SELECT a.offer_id
               FROM latest_activities a
               WHERE a.type = 'IntroducedToEmployer'
             ) ii
          ON (o.id = ii.offer_id)
      WHERE (o.created_at >= current_date - INTERVAL '30' DAY)
    )
  )
SELECT
    j.*
FROM jobs j
  LEFT JOIN this_offers i
    ON (CAST(j.id AS INT) = i.job_id)
  LEFT JOIN (
              SELECT job_id
              FROM latest_activities a
              WHERE a.type = 'CandidatesListViewed' OR a.type = 'CandidateViewed'
            ) s
    ON (j.id = s.job_id)
WHERE
  j.created_at >= current_date - INTERVAL '30' DAY OR s.job_id IS NOT NULL
                                                      AND j.primary_role_id IN
                                                          (1, 2, 3, 4, 30, 11, 12, 13)
                                                      AND
                                                      j.company_id NOT IN (258, 257, 7480, 11342)
GROUP BY 1, 2
ORDER BY 1) active_jobs