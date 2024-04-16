import logging
from copy import copy
from datetime import datetime, timezone
from typing import Dict

from hapi.pipeline.hno.hapi_api import (
    get_admin_lookup,
    get_reference_period,
    get_resource_ref,
)
from hapi.pipeline.hno.hapi_patch import HAPIPatch

logger = logging.getLogger(__name__)


class PatchGeneration:
    def __init__(self, hapi_repo: str, year: int):
        self.hapi_repo = hapi_repo
        reference_period_start = datetime(year, 1, 1, tzinfo=timezone.utc)
        self.adminlookup = get_admin_lookup(reference_period_start)

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
                resource_id, resource_ref = get_resource_ref(dataset)
                for row in rows[countryiso3]:
                    admin2_ref = self.get_admin2_ref(countryiso3, row)
                    population_group_code = row["Population Group"]
                    sector_code = row["Sector"]
                    gender_code = row["Gender"]
                    age_range_code = row["Age Group"]
                    disabled_marker = row["Disabled"]
                    reference_period_start, reference_period_end = (
                        get_reference_period(dataset)
                    )
                    base_row = [
                        resource_ref,
                        admin2_ref,
                        None,
                        population_group_code,
                        sector_code,
                        gender_code,
                        age_range_code,
                        disabled_marker,
                        None,
                        reference_period_start,
                        reference_period_end,
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
                    "description": f"Updating HNO for {resource_id}",
                    # automatically generated “State”: “canceled” / “active” / “failed”
                    "sequence": hapi_patch.get_sequence_number(),
                    # to make sure the patches are applied in a specific order
                    "database_schema_version": "0.6.2",
                    # This is based on https://github.com/OCHA-DAP/hapi-sqlalchemy-schema.
                    "changes": [
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
                        }
                    ],
                }
                hapi_patch.create("hno", patch)
