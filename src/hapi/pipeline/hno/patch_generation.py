import logging
from copy import copy
from typing import Dict

from hdx.utilities.dateparse import parse_date

from hapi.pipeline.hno.hapi_api import get_admin_lookup
from hapi.pipeline.hno.hapi_patch import HAPIPatch

logger = logging.getLogger(__name__)


class PatchGeneration:
    def __init__(self, hapi_repo: str, year: int):
        self.hapi_repo = hapi_repo
        self.reference_period_start = f"{year}-01-01T00:00:00"
        self.reference_period_end = f"{year}-12-31T23:59:59"
        self.adminlookup = get_admin_lookup(
            parse_date(self.reference_period_start)
        )

    def get_admin2_ref(self, countryiso3: str, row: Dict) -> str:
        adm2 = row["Admin 2 PCode"]
        if adm2:
            admin2_ref = adm2  # lookup needed here
        else:
            adm1 = row["Admin 1 PCode"]
            if adm1:
                admin2_ref = adm1  # lookup needed here
            else:
                admin2_ref = countryiso3  # lookup needed here
        return admin2_ref

    def generate_hapi_patch(self, datasets: Dict, rows: Dict) -> None:
        with HAPIPatch(self.hapi_repo) as hapi_patch:
            values = []
            for countryiso3 in datasets:
                dataset = datasets[countryiso3]
                resource = dataset.get_resource()
                resource_id = resource["id"]
                for row in rows[countryiso3]:
                    admin2_ref = self.get_admin2_ref(countryiso3, row)
                    population_group_code = row["Population Group"]
                    sector_code = row["Sector"]
                    gender_code = row["Gender"]
                    age_range_code = row["Age Group"]
                    disabled_marker = row["Disabled"]
                    base_row = [
                        resource_id,
                        admin2_ref,
                        None,
                        population_group_code,
                        sector_code,
                        gender_code,
                        age_range_code,
                        disabled_marker,
                        None,
                        self.reference_period_start,
                        self.reference_period_end,
                    ]

                    def create_row(in_col, status):
                        out_row = copy(base_row)
                        population = row[in_col]
                        if population != "":
                            out_row[2] = status
                            out_row[8] = population
                            values.append(out_row)

                    create_row("Population", "population")
                    create_row("Affected", "affected")
                    create_row("In Need", "inneed")
                    create_row("Targeted", "targeted")
                    create_row("Reached", "reached")

                patch = {
                    "description": f"Updating HNO for resource: {resource_id} and country: {countryiso3}",
                    # automatically generated “State”: “canceled” / “active” / “failed”
                    "sequence": hapi_patch.get_sequence_number(),
                    # to make sure the patches are applied in a specific order
                    "database_schema_version": "0.6.2",
                    # This is based on https://github.com/OCHA-DAP/hapi-sqlalchemy-schema.
                    "changes": [
                        {
                            "type": "DELETE",
                            "entity": "DBHumanitarianNeeds",
                            "conditions": [
                                {
                                    "column": "resource_id",
                                    "operator": "equals",
                                    "value": resource,
                                },
                                {
                                    "column": "reference_period_start",
                                    "operator": "equals",
                                    "value": self.reference_period_start,
                                },
                            ],
                        },
                        {
                            "type": "INSERT",
                            "entity": "DBHumanitarianNeeds",
                            # Based on https://github.com/OCHA-DAP/hapi-sqlalchemy-schema
                            "headers": [
                                "resource_ref",
                                "admin2_ref",
                                "population_status_code",
                                "population_group_code",
                                "sector_code",
                                "gender_code",
                                "age_range_code",
                                "disabled_marker",
                                "population",
                                "reference_period_start",
                                "reference_period_end",
                                {
                                    "name": "source_data",
                                    "value": "not yet implemented",
                                    # option to specify a value that applies to all rows
                                },
                            ],
                            "values": values,
                        },
                    ],
                }
                hapi_patch.create(f"hno_{countryiso3}", patch)
