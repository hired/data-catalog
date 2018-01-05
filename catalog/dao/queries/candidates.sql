(WITH candidate_roles AS
(SELECT
   u.id,
   json_agg(r.*) AS buckets
 FROM users u LEFT OUTER JOIN candidate_role_selections rs
     ON u.id = rs.candidate_id
   LEFT OUTER JOIN (select ri.id as bucket_id,ri.* as bucket from roles ri) r
     ON rs.role_id = r.id
 WHERE u.type = 'Candidate'
 GROUP BY u.id),
    candidate_places AS (
      SELECT
        u.id,
        p2.place  AS place,
        p2.market AS market
      FROM users u LEFT OUTER JOIN (SELECT
                                      p1.id           AS id,
                                      row_to_json(p1) AS place,
                                      row_to_json(m)  AS market
                                    FROM places p1
                                      JOIN markets m ON m.place_id = p1.id) p2
          ON p2.id = u.place_id
      WHERE u.type = 'Candidate'
  ),
    place_selections AS
  (SELECT
     u.id,
     json_agg(ps.*) AS place_selections
   FROM users u LEFT OUTER JOIN place_selections ps
       ON ps.candidate_id = u.id
   WHERE u.type = 'Candidate'
   GROUP BY u.id),
    candidate_skills AS (
      SELECT
        t.id,
        json_agg(t.skill_name) AS skills
      FROM (
             SELECT
               u.id,
               s.name AS skill_name,
               pr.name
             FROM users u
               LEFT OUTER JOIN candidate_skill_selections ss
                 ON ss.candidate_id = u.id
               LEFT OUTER JOIN skills S
                 ON ss.skill_id = S.id
               LEFT OUTER JOIN primary_roles pr
                 ON S.primary_role_id = pr.id
             WHERE u.type = 'Candidate'
             ORDER BY ss.created_at) t
      GROUP BY t.id
  )
SELECT row_to_json(t) as payload
FROM (
       SELECT
         employment.employments,
         education.educations,
         answer.answers,
         response.reponses,
         favorite.favorites,
         p.place,
         p.market,
         r.buckets,
         shortlistings.job_ids                AS shortlistings_job_ids,
         offer.offers,
         hidden_company.hidden_company_ids    AS candidate_hidden_company_ids,
         hidden_employers.employer_hidden_ids AS hidden_by_employer_ids,
         u.id                                 AS candidate_id,
         slug,
         name,
         email,
         signup_email,
         approved,
         signup_email,
         u.created_at,
         u.deleted_at,
         u.approved_at,
         u.rejected_at,
         row_to_json(cp)                      AS profile,
         membership.memberships,
         ps.place_selections,
         s.skills
       FROM (SELECT *
             FROM users
             WHERE type = 'Candidate') u
         LEFT JOIN (SELECT
                      json_agg(job_id) AS job_ids,
                      candidate_id
                    FROM shortlistings
                    GROUP BY candidate_id) AS shortlistings
           ON u.id = shortlistings.candidate_id
         LEFT JOIN (SELECT
                      json_agg(educations) AS educations,
                      candidate_id
                    FROM educations
                    GROUP BY candidate_id) AS education
           ON u.id = education.candidate_id
         LEFT JOIN (SELECT
                      json_agg(matching_answers) AS answers,
                      candidate_id
                    FROM matching_answers
                    GROUP BY candidate_id) AS answer
           ON u.id = answer.candidate_id
         LEFT JOIN (
                     SELECT
                       t.candidate_id,
                       json_agg(t) AS reponses
                     FROM (
                            SELECT
                              q.candidate_id,
                              q.message,
                              q.status,
                              q.type,
                              r.message,
                              r.status
                            FROM candidate_questions q
                              JOIN question_responses r
                                ON q.id = r.question_id) t
                     GROUP BY t.candidate_id
                   ) AS response
           ON u.id = response.candidate_id
         LEFT JOIN (SELECT
                      json_agg(employments) AS employments,
                      user_id               AS candidate_id
                    FROM employments
                    GROUP BY candidate_id) AS employment
           ON u.id = employment.candidate_id
         LEFT JOIN (SELECT
                      json_agg(company_id) AS hidden_company_ids,
                      candidate_id
                    FROM candidate_hidden_companies
                    GROUP BY candidate_id) AS hidden_company
           ON u.id = hidden_company.candidate_id
         LEFT JOIN (SELECT
                      json_agg(employer_id) AS employer_hidden_ids,
                      candidate_id
                    FROM employer_hides
                    GROUP BY candidate_id) AS hidden_employers
           ON u.id = hidden_employers.candidate_id
         LEFT JOIN (SELECT
                      json_agg(auction_memberships) AS memberships,
                      candidate_id
                    FROM auction_memberships
                    GROUP BY candidate_id) AS membership
           ON u.id = membership.candidate_id
         LEFT JOIN (SELECT
                      candidate_id,
                      json_agg(offers.*) AS offers
                    FROM offers
                    GROUP BY candidate_id) offer ON
                                                   offer.candidate_id = u.id
         LEFT JOIN (SELECT
                      json_agg(t) as favorites,
                      t.candidate_id
                    FROM (SELECT
                            candidate_id,
                            employer_id,
                            company_id
                          FROM company_favorites) t
                    GROUP BY candidate_id) favorite ON
                                                      favorite.candidate_id = u.id
         LEFT OUTER JOIN candidate_profiles cp
           ON cp.id = u.profile_id
         LEFT JOIN candidate_roles r
           ON r.id = u.id
         LEFT JOIN candidate_places p
           ON p.id = u.id
         LEFT JOIN place_selections ps
           ON ps.id = u.id
         LEFT JOIN candidate_skills S
           ON S.id = u.id
       ) t
) as candidates


