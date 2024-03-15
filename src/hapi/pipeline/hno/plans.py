from datetime import datetime

from hdx.utilities.retriever import Retrieve


def get_plans(hpc_url: str, retriever: Retrieve, today: datetime):
    year = today.year
    json = retriever.download_json(
        f"{hpc_url}fts/flow/plan/overview/progress/{year}"
    )
    plan_infos = []
    for plan in json["data"]["plans"]:
        planid = plan["id"]
        if plan["planType"]["name"] != "Humanitarian response plan":
            continue
        countries = plan["countries"]
        if len(countries) != 1:
            continue
        countryiso3 = countries[0]["iso3"]
        plan_infos.append({"iso3": countryiso3, "planid": planid})
    return plan_infos
