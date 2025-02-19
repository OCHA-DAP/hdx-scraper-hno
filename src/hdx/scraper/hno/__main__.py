"""Entry point to start HAPI HNO pipeline"""

import logging
from os import getenv
from os.path import expanduser, join
from typing import Optional

from hdx.api.configuration import Configuration
from hdx.api.utilities.hdx_error_handler import HDXErrorHandler
from hdx.data.dataset import Dataset
from hdx.data.user import User
from hdx.facades.infer_arguments import facade
from hdx.scraper.framework.utilities.reader import Read
from hdx.scraper.hno._version import __version__
from hdx.scraper.hno.dataset_generator import DatasetGenerator
from hdx.scraper.hno.hapi_dataset_generator import HAPIDatasetGenerator
from hdx.scraper.hno.hapi_output import HAPIOutput
from hdx.scraper.hno.monitor_json import MonitorJSON
from hdx.scraper.hno.plan import Plan
from hdx.scraper.hno.progress_json import ProgressJSON
from hdx.utilities.dateparse import now_utc
from hdx.utilities.easy_logging import setup_logging
from hdx.utilities.path import (
    script_dir_plus_file,
    wheretostart_tempdir_batch,
)

setup_logging()
logger = logging.getLogger(__name__)

lookup = "hdx-scraper-hno"
updated_by_script = "HDX Scraper: HPC HNO"

generate_country_resources = True
generate_global_dataset = True
generate_hapi_dataset = True


def main(
    save: bool = False,
    use_saved: bool = False,
    hpc_basic_auth: str = "",
    hpc_bearer_token: str = "",
    countryiso3s: str = "",
    pcodes: str = "",
    year: Optional[int] = None,
    err_to_hdx: bool = False,
    save_test_data: bool = False,
) -> None:
    """Generate datasets and create them in HDX

    Args:
        save (bool): Save downloaded data. Defaults to False.
        use_saved (bool): Use saved data. Defaults to False.
        hpc_basic_auth (str): Basic auth string. Defaults to "".
        hpc_bearer_token (str): Bearer token. Defaults to "".
        countryiso3s (str): Countries to process. Defaults to "" (all countries).
        pcodes (str): P-codes to process. Defaults to "" (all p-codes).
        year (Optional[int]): Year to process. Defaults to None.
        err_to_hdx (bool): Whether to write any errors to HDX metadata. Defaults to False.
        save_test_data (bool): Whether to save test data. Defaults to False.
    Returns:
        None
    """
    logger.info(f"##### {lookup} version {__version__} ####")
    if not User.check_current_user_organization_access(
        "49f12a06-1605-4f98-89f1-eaec37a0fdfe", "create_dataset"
    ):
        raise PermissionError(
            "API Token does not give access to OCHA HPC-Tools organisation!"
        )
    with HDXErrorHandler(write_to_hdx=err_to_hdx) as error_handler:
        with wheretostart_tempdir_batch(lookup) as info:
            folder = info["folder"]
            batch = info["batch"]
            configuration = Configuration.read()
            if not year:
                year = getenv("YEAR")
            today = now_utc()
            if not year:
                year = today.year
            year = int(year)
            saved_dir = "saved_data"
            if not hpc_basic_auth:
                hpc_basic_auth = getenv("HPC_BASIC_AUTH")
            if not hpc_bearer_token:
                hpc_bearer_token = getenv("HPC_BEARER_TOKEN")
            Read.create_readers(
                folder,
                "saved_data",
                folder,
                save,
                use_saved,
                hdx_auth=configuration.get_api_key(),
                basic_auths={"hpc_basic": hpc_basic_auth},
                bearer_tokens={"hpc_bearer": hpc_bearer_token},
                today=today,
                rate_limit={"calls": 1, "period": 1},
            )
            if countryiso3s:
                countryiso3s = countryiso3s.split(",")
            else:
                countryiso3s = None
            if pcodes:
                pcodes = pcodes.split(",")
            else:
                pcodes = None
            plan = Plan(
                configuration, year, error_handler, countryiso3s, pcodes
            )
            dataset_generator = DatasetGenerator(configuration, year)
            hapi_output = HAPIOutput(
                configuration,
                year,
                error_handler,
                dataset_generator.global_name,
            )
            hapi_output.setup_admins()
            progress_json = ProgressJSON(year, saved_dir, save_test_data)
            plan_ids_countries = plan.get_plan_ids_and_countries(progress_json)

            countries_with_data = []
            for plan_id_country in plan_ids_countries:
                countryiso3 = plan_id_country["iso3"]
                plan_id = plan_id_country["id"]
                monitor_json = MonitorJSON(saved_dir, save_test_data)
                published, rows = plan.process(
                    countryiso3, plan_id, monitor_json
                )
                if not rows:
                    continue
                hapi_output.process(countryiso3, rows)
                hapi_output.add_negative_rounded_errors(countryiso3)
                countries_with_data.append(countryiso3)
                if not generate_country_resources:
                    continue
                dataset = dataset_generator.get_country_dataset(countryiso3)
                highest_admin = plan.get_highest_admin(countryiso3)
                if not dataset:
                    logger.warning(
                        f"No dataset found for {countryiso3}, generating!"
                    )
                    dataset = dataset_generator.generate_country_dataset(
                        countryiso3, folder, rows, highest_admin
                    )
                    dataset.update_from_yaml(
                        script_dir_plus_file(
                            join("config", "hdx_dataset_static.yaml"), main
                        )
                    )
                    resource = dataset.get_resource(0)
                    dataset.create_in_hdx(
                        match_resource_order=True,
                        remove_additional_resources=False,
                        hxl_update=False,
                        updated_by_script=updated_by_script,
                        batch=batch,
                    )
                else:
                    resource = dataset_generator.add_country_resource(
                        dataset, countryiso3, rows, folder, highest_admin
                    )
                    if not resource:
                        continue
                    resource.set_date_data_updated(published)
                    dataset.set_quickchart_resource(resource)
                    dataset.update_in_hdx(
                        operation="patch",
                        match_resource_order=True,
                        remove_additional_resources=False,
                        hxl_update=False,
                        updated_by_script=updated_by_script,
                        batch=batch,
                    )

                if highest_admin == 0:
                    filename = "hdx_country_resource_view_static_adm0.yaml"
                    if next(iter(rows.values())).get("In Need", "") == "":
                        filename = (
                            "hdx_country_resource_view_static_adm0_no_pin.yaml"
                        )
                elif highest_admin == 1:
                    filename = "hdx_country_resource_view_static_adm1.yaml"
                else:
                    filename = "hdx_country_resource_view_static.yaml"
                dataset.generate_quickcharts(
                    resource,
                    script_dir_plus_file(join("config", filename), main),
                )

            if generate_global_dataset:
                global_rows = plan.get_global_rows()
                global_highest_admin = plan.get_global_highest_admin()
                dataset = Dataset.read_from_hdx(
                    dataset_generator.slugified_name
                )
                if dataset:
                    resource = dataset_generator.add_global_resource(
                        dataset,
                        global_rows,
                        folder,
                        global_highest_admin,
                    )
                else:
                    dataset, resource = (
                        dataset_generator.generate_global_dataset(
                            folder,
                            global_rows,
                            countries_with_data,
                            global_highest_admin,
                        )
                    )
                if dataset:
                    dataset.update_from_yaml(
                        script_dir_plus_file(
                            join("config", "hdx_dataset_static.yaml"), main
                        )
                    )
                    if global_highest_admin == 0:
                        filename = "hdx_resource_view_static_adm0.yaml"
                    else:
                        filename = "hdx_resource_view_static.yaml"
                    dataset.generate_quickcharts(
                        0,
                        script_dir_plus_file(join("config", filename), main),
                    )
                    dataset.create_in_hdx(
                        match_resource_order=True,
                        remove_additional_resources=False,
                        hxl_update=False,
                        updated_by_script=updated_by_script,
                        batch=batch,
                    )

                    # We need the global dataset id and resource id
                    if generate_hapi_dataset:
                        resource_id = resource.get("id")
                        if not resource_id:
                            name = resource["name"]
                            for resource in dataset.get_resources():
                                if resource["name"] == name:
                                    resource_id = resource["id"]
                                    break
                        if resource_id:
                            global_rows = hapi_output.get_global_rows()
                            hapi_dataset_generator = HAPIDatasetGenerator(
                                configuration,
                                year,
                                global_rows,
                                countries_with_data,
                            )
                            dataset = (
                                hapi_dataset_generator.generate_needs_dataset(
                                    folder,
                                    countries_with_data,
                                    dataset["id"],
                                    resource_id,
                                )
                            )
                            if dataset:
                                dataset.update_from_yaml(
                                    script_dir_plus_file(
                                        join(
                                            "config", "hdx_dataset_static.yaml"
                                        ),
                                        main,
                                    )
                                )
                                if global_highest_admin == 0:
                                    filename = (
                                        "hdx_resource_view_static_adm0.yaml"
                                    )
                                else:
                                    filename = "hdx_resource_view_static.yaml"
                                dataset.generate_quickcharts(
                                    0,
                                    script_dir_plus_file(
                                        join("config", filename), main
                                    ),
                                )
                                dataset.create_in_hdx(
                                    remove_additional_resources=False,
                                    hxl_update=False,
                                    updated_by_script=updated_by_script,
                                    batch=batch,
                                )
                                resources = sorted(
                                    dataset.get_resources(),
                                    key=lambda r: r["name"],
                                    reverse=True,
                                )
                                dataset.reorder_resources(
                                    [r["id"] for r in resources],
                                    hxl_update=False,
                                )

    logger.info("HDX Scraper HNO pipeline completed!")


if __name__ == "__main__":
    facade(
        main,
        hdx_site="feature",
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=lookup,
        project_config_yaml=script_dir_plus_file(
            join("config", "project_configuration.yaml"), main
        ),
    )
