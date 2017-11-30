WITH places AS (
    SELECT
      u.id,
      json_agg(p1.*) AS places
    FROM users u LEFT JOIN places p1
        ON u.place_id = p1.id
    WHERE u.type = 'Employer'
    GROUP BY u.id),
    offers AS (
      SELECT
        u.id           AS candidate_id,
        json_agg(p1.*) AS offers
      FROM users u LEFT JOIN offers p1
          ON u.id = p1.employer_id
      GROUP BY u.id)
SELECT
  u.id AS employer_id,
  p.places,o.offers
FROM (SELECT u.*
      FROM users u
        JOIN companies c ON u.company_id = c.id
      WHERE c.name = 'Amazon') u
  LEFT JOIN places p
    ON p.id = u.id
  LEFT JOIN companies c
    ON c.id = u.company_id
  LEFT JOIN offers o
    ON o.candidate_id = u.id;



