( select row_to_json(t) as payload from (SELECT
  o.id as offer_id,
  o.*,
  to_json(c.*) as company,
  to_json(membership.*) as membership
FROM offers o
  JOIN companies c ON c.id = o.company_id
  JOIN auction_memberships membership on membership.id = o.auction_membership_id) t
) as offers


