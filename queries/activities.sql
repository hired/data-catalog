(SELECT a.*, json_extract_path(a.data, 'job_id') as job_id
      from activitie s WHERE a.created_at >= current_date - INTERVAL '30' DAY AND
            json_extract_path(a.data, 'job_id') IS NOT NULL AND
            a.type in ('CandidatesListViewed','CandidateViewed','BidOnCandidate','IntroducedToEmployer'))
            as activities