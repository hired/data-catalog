(WITH the_skills AS (
    SELECT
      t.id,
      row_to_json(t) AS skill
    FROM
      (SELECT
         s.id,
         ns.name AS normalized_name,
         s.name  AS name
       FROM skills s
         LEFT JOIN (SELECT DISTINCT lower(name) AS name
                    FROM normalized_skills) ns ON ns.name = lower(s.name)) t),
    required_skills AS (SELECT
                          j.id              AS job_id,
                          json_agg(s.skill) AS skills
                        FROM the_skills s
                          JOIN jobs_skills js ON js.skill_id = s.id
                          JOIN jobs j ON j.id = js.job_id
                        GROUP BY j.id),
    optional_skills AS (SELECT
                          j.id              AS job_id,
                          json_agg(s.skill) AS skills
                        FROM the_skills s
                          JOIN jobs_optional_skills js ON js.skill_id = s.id
                          JOIN jobs j ON j.id = js.job_id
                        GROUP BY j.id),
    the_user AS (
      SELECT
        u.*,
        to_json(p.*) AS place,
        to_json(l.*) AS locale
      FROM users u
        LEFT JOIN places p ON u.place_id = p.id
        LEFT JOIN locales l ON u.locale_id = l.id
  ),
    latest_activities AS (
      SELECT
        a.type,
        CAST(REPLACE(json_extract_path(a.data, 'job_id') :: TEXT, '"', '') AS INT) AS job_id,
        CAST(a.target_id AS INT)                                                   AS offer_id
      FROM activities a
      WHERE a.created_at >= CURRENT_DATE - INTERVAL '20' DAY AND
            json_extract_path(a.data, 'job_id') IS NOT NULL ),
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
      WHERE (o.created_at >= CURRENT_DATE - INTERVAL '20' DAY)
    )
  )
SELECT row_to_json(t) AS payload
FROM (
       SELECT
         to_json(c.*)          AS company,
         to_json(bucket.*)     AS bucket,
         to_json(p.*)          AS place,
         to_json(emp.employee) AS employer,
         j.*,
         rs.skills             AS required_skills,
         os.skills             AS optional_skills
       FROM jobs j
         LEFT JOIN required_skills rs
           ON rs.job_id = j.id
         LEFT JOIN (SELECT
                      json_agg(u.*) AS employee,
                      ej.job_id     AS job_id
                    FROM employer_jobs ej
                      JOIN the_user u ON u.id = ej.employer_id
                    GROUP BY ej.job_id) emp
           ON emp.job_id = j.id
         LEFT JOIN places p ON
                              j.place_id = p.id
         LEFT JOIN companies c ON
                                 j.company_id = c.id
         LEFT JOIN roles bucket ON
                                  bucket.id = j.bucket_id
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
         j.created_at >= CURRENT_DATE - INTERVAL '20' DAY OR S.job_id IS NOT NULL
                                                             AND j.primary_role_id IN
                                                                 (1, 2, 3, 4, 20, 11, 12, 13)
                                                             AND
                                                             j.company_id NOT IN
                                                             (258, 257, 7480, 11342)) t
) as job_alias