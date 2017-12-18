WITH candidate_roles AS
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