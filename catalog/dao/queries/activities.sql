(SELECT row_to_json(t) AS payload
FROM (
       SELECT
         a.*,
         json_extract_path(a.data, 'job_id') AS job_id
       FROM activities a
       WHERE a.created_at >= current_date - INTERVAL '30' DAY AND
             json_extract_path(a.data, 'job_id') IS NOT NULL AND
             a.type IN
             ('CandidatesListViewed', 'CandidateViewed', 'BidOnCandidate', 'IntroducedToEmployer')) t

) as activities