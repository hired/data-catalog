(WITH
    required_skills AS ( SELECT
                           j.id AS job_id,
                           s.name
                         FROM jobs j
                           JOIN jobs_skills js ON js.job_id = j.id
                           JOIN skills s ON s.id = js.skill_id),
    optional_skills AS ( SELECT
                           j.id AS job_id,
                           s.name
                         FROM jobs j
                           JOIN jobs_optional_skills js ON js.job_id = j.id
                           JOIN skills s ON s.id = js.skill_id),
    latest_activities AS (
      SELECT
        a.type,
        cast(replace(json_extract_path(a.data, 'job_id') :: TEXT, '"', '') AS INT) AS job_id,
        CAST(a.target_id AS INT)                                                   AS offer_id
      FROM activities a
      WHERE a.created_at >= current_date - INTERVAL '30' DAY AND
            json_extract_path(a.data, 'job_id') IS NOT NULL),
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
      WHERE (o.created_at >= CURRENT_DATE - INTERVAL '30' DAY)
    )
  )
SELECT
  j.*,
  json_agg(rs.name) AS required_skills,
  json_agg(os.name) AS optional_skills,
  json_agg(p.*)
FROM jobs j
  LEFT JOIN required_skills rs
    ON rs.job_id = j.id
  LEFT JOIN places p ON
                       j.place_id = p.id
  LEFT JOIN optional_skills os
    ON os.job_id = j.id
  LEFT JOIN this_offers i
    ON (CAST(j.id AS INT) = i.job_id)
  LEFT JOIN (
              SELECT job_id
              FROM latest_activities a
              WHERE a.type = 'CandidatesListViewed' OR a.type = 'CandidateViewed'
            ) S
    ON (j.id = S.job_id)
WHERE
  j.created_at >= CURRENT_DATE - INTERVAL '30' DAY OR S.job_id IS NOT NULL
                                                      AND j.primary_role_id IN
                                                          (1, 2, 3, 4, 30, 11, 12, 13)
                                                      AND
                                                      j.company_id NOT IN (258, 257, 7480, 11342)
GROUP BY 1, 2
ORDER BY 1) as job_alias