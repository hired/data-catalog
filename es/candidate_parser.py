import json
from datetime import date

import dateutil.parser
from dateutil.relativedelta import relativedelta

from catalog.utils.spark_factory import spark_conf


class Candidate(object):
    def __init__(self
                 , slug
                 , name
                 , email
                 , signup_email
                 , us_dod_clearance
                 , educations
                 , created_at
                 , approved_at
                 , rejected_at
                 , deleted_at
                 , skills
                 , remote
                 , preferred_contract_rate
                 , min_total_comp
                 , candidate_hidden_company_ids
                 , hidden_by_employer_ids
                 , primary_role_experience
                 , favorites
                 , desired_job_types
                 , fulltime_contract_preference
                 , shortlistings_job_ids
                 , approved
                 , employments
                 , offers
                 , answers
                 , reponses
                 , place
                 , market
                 , roles
                 , candidate_id
                 , profile
                 , memberships
                 , selected_places):
        self.recent_offers_count = len([offer for offer in offers if
                                        dateutil.parser.parse(offer.get("created_at")).date()
                                        > (
                                            date.today() - relativedelta(months=+3))])
        self.job_type_preference = job_type_preference(desired_job_types,
                                                       fulltime_contract_preference)
        self.favorited_by_company_ids = favorites
        self.numeric_years_of_experience = primary_role_experience
        self.selected_places = selected_places
        self.work_summary = "{} at {}".format(employments[-1].get("position"),
                                              employments[-1].get("company"))
        self.education_summary = "{},{}".format(educations[-1].get("university"),
                                                educations[-1].get("degree"))
        self.memberships = memberships
        self.profile = profile
        self.candidate_id = candidate_id
        self.roles = roles
        self.rejected = not approved
        self.open_to_consulting = is_open_to_consulting(answers)
        self.market = market
        self.place = place
        self.coords = {"lat": place.get("latitude", 0), "lng": place.get("longitude", 0)}
        self.coords_array = [self.coords["lng"], self.coords["lat"]]
        self.reponses = reponses
        self.answers = answers
        self.offers = offers
        self.employments = employments
        self.approved = approved
        self.shortlistings_job_ids = shortlistings_job_ids
        self.hidden_by_employer_ids = hidden_by_employer_ids
        self.candidate_hidden_company_ids = candidate_hidden_company_ids
        self.min_total_comp = min_total_comp
        self.preferred_contract_rate = preferred_contract_rate
        self.remote = remote
        self.skills = skills
        self.skills_list = ",".join(skills)
        self.top_skills = skills[:5]
        self.deleted_at = deleted_at
        self.rejected_at = rejected_at
        self.approved_at = approved_at
        self.created_at = created_at
        self.educations = educations
        self.us_dod_clearance_display_name = us_dod_clearance
        self.signup_email = signup_email
        self.email = email
        self.name = name
        self.slug = slug


def is_open_to_consulting(answers):
    if answers:
        return True if [answer for answer in answers if
                        answer.get(
                            "question_key") == "consulting" and answer.get(
                            "answer_key") == "y"] else False
    return False


def job_type_preference(preferred_job_type, fulltime_contract_preference):
    if [job_type for job_type in preferred_job_type if job_type.endswith("only")]:
        return preferred_job_type
    elif len(
            preferred_job_type) == 2 and "full_time" in preferred_job_type and "contract" in preferred_job_type:
        if fulltime_contract_preference == "full_time":
            return "full_time_preferred"
        elif fulltime_contract_preference == "contract":
            return "contract_preferred"
        else:
            return "both_equally"
    return "both_equally"


def tap(elem):
    return elem


def favorited_by_companyids(elem):
    if elem:
        return [i.get("company_id") for i in elem]
    return []


def yoe_to_i(yoe):
    elems = yoe.replace("+", "").split("-")
    try:
        if len(elems) == 2:
            return int(elems[1])
        else:
            return int(elems[0])
    except ValueError:
        0


mapping = {"candidate_id": tap, "slug": tap, "name": tap, "email": tap, "signup_email": tap,
           "us_dod_clearance": tap,
           "educations": tap,
           "desired_job_types": tap,
           "fulltime_contract_preference": tap,
           "created_at": tap,
           "approved_at": tap, "rejected_at": tap, "deleted_at": tap, "skills": tap, "remote": tap,
           "preferred_contract_rate": tap,
           "min_total_comp": tap, "numeric_years_of_experience": tap,
           "candidate_hidden_company_ids": tap,
           "hidden_by_employer_ids": tap, "favorited_by_company_ids": tap,
           "favorited_by_employer_ids": tap,
           "recent_job_offers": tap, "job_type_preference": tap, "open_to_consulting": tap,
           "shortlistings_job_ids": tap,
           "approved": tap, "rejected": tap, "employments": tap, "offers": tap, "answers": tap,
           "reponses": tap, "favorites": tap,
           "place": tap, "market": tap,
           "roles": tap, "candidate_id": tap, "profile": tap, "memberships": tap,
           "selected_places": tap,
           "primary_role_experience": yoe_to_i,
           "favorites": favorited_by_companyids}

spark = spark_conf()
from catalog.dao import write_catalog

candidates = spark.read.parquet(write_catalog.DATA_CATALOG_SINK + "/candidates")
jsons = candidates.select(candidates.row_to_json).rdd.collect()
for j in jsons:
    print(j)


class ComplexEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Candidate):
            return obj.__dict__
        return json.JSONEncoder.default(self, obj)

# with open("sample1.json", mode='r', encoding='utf-8') as json_payload:
#     user_payload = json.loads(json_payload.read())
#     user_profile = user_payload.get("profile")
#     k = {key: mapping.get(key)(value) for key, value in user_profile.items() if
#          key in mapping}
#     k1 = {key: mapping.get(key)(value) for key, value in user_payload.items() if
#           key in mapping}
#     k.update(k1)
#     candidate = Candidate(**k)
#     print(json.dumps(candidate.__dict__, cls=ComplexEncoder))
#
#     es_conf = {"es.nodes" : "ela-n1","es.port" : "9200","es.nodes.client.only" : "true","es.resource" : "sensor_counts/metrics"}
