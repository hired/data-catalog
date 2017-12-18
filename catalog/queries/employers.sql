(WITH places AS (
    SELECT
      u.id,
      json_agg(p1.*) AS places
    FROM users u LEFT JOIN places p1
        ON u.place_id = p1.id
    WHERE u.type = 'Employer'
    GROUP BY u.id)
SELECT row_to_json(t) AS payload
FROM (
  SELECT
    u.id               AS employer_id,
    to_json(c.*)       AS company,
    to_json(profile.*) AS profiles,
    hidden_candidate.hidden_candidates,
    candidate_question.responses,
    p.places,
    o.offer_ids
  FROM (SELECT u.*
        FROM users u
          JOIN companies c ON u.company_id = c.id
        WHERE u.type = 'Employer'
       ) u
    LEFT JOIN places p
      ON p.id = u.id
    LEFT JOIN employer_profiles profile
      ON profile.id = u.profile_id
    LEFT JOIN (SELECT
                 json_agg(candidate_id) AS hidden_candidates,
                 employer_id
               FROM employer_hides
               GROUP BY employer_id) AS hidden_candidate
      ON hidden_candidate.employer_id = u.id
    LEFT JOIN (SELECT
                 t.sender_id,
                 json_agg(t) AS responses
               FROM (
                      SELECT
                        q.sender_id,
                        q.candidate_id,
                        q.message,
                        q.status,
                        q.type,
                        r.message,
                        r.status
                      FROM candidate_questions q
                        JOIN question_responses r
                          ON q.id = r.question_id) t
               GROUP BY t.sender_id) AS candidate_question
      ON candidate_question.sender_id = u.id
    LEFT JOIN companies c
      ON c.id = u.company_id
    LEFT JOIN (SELECT
                 json_agg(id) AS offer_ids,
                 employer_id  AS employer_id
               FROM offers
               GROUP BY employer_id) o
      ON o.employer_id = u.id) t) as employers







