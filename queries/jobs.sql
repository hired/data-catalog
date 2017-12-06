(WITH
    required_skills AS ( SELECT
                           j.id             AS job_id,
                           json_agg(s.name) AS name
                         FROM jobs j
                           JOIN jobs_skills js ON js.job_id = j.id
                           JOIN skills s ON s.id = js.skill_id
                         WHERE s.name IS NOT NULL
                         GROUP BY j.id),
    optional_skills AS ( SELECT
                           j.id             AS job_id,
                           json_agg(s.name) AS name
                         FROM jobs j
                           JOIN jobs_optional_skills js ON js.job_id = j.id
                           JOIN skills s ON s.id = js.skill_id
                         WHERE s.name IS NOT NULL
                         GROUP BY j.id),
    latest_activities AS (
      SELECT
        a.type,
        cast(replace(json_extract_path(a.data, 'job_id') :: TEXT, '"', '') AS INT) AS job_id,
        CAST(a.target_id AS INT)                                                   AS offer_id
      FROM activities a
      WHERE a.created_at >= current_date - INTERVAL '10' DAY AND
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
      WHERE (o.created_at >= CURRENT_DATE - INTERVAL '10' DAY)
    )
  )
SELECT
  j.*,
  rs.name        AS required_skills,
  os.name        AS optional_skills,
  to_json(p.*)   AS places,
  to_json(emp.employee) AS employer_ids
FROM jobs j
  LEFT JOIN required_skills rs
    ON rs.job_id = j.id
  LEFT JOIN (SELECT
               json_agg(u.*) as employee,
               ej.job_id AS job_id
             FROM employer_jobs ej
               JOIN users u ON u.id = ej.employer_id
             GROUP BY ej.job_id) emp
    ON emp.job_id = j.id
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
              GROUP BY job_id
            ) S
    ON (j.id = S.job_id)
WHERE
  j.created_at >= CURRENT_DATE - INTERVAL '10' DAY OR S.job_id IS NOT NULL
                                                      AND j.primary_role_id IN
                                                          (1, 2, 3, 4, 10, 11, 12, 13)
                                                      AND
                                                      j.company_id NOT IN (258, 257, 7480, 11342)
ORDER BY 1) as job_alias