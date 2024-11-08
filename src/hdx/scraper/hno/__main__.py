"""Entry point to start HAPI HNO pipeline"""

import logging
from os.path import expanduser, join

from hdx.api.configuration import Configuration
from hdx.data.user import User
from hdx.facades.infer_arguments import facade
from hdx.scraper.hno._version import __version__
from hdx.scraper.hno.dataset_generator import DatasetGenerator
from hdx.scraper.hno.monitor_json import MonitorJSON
from hdx.scraper.hno.plan import Plan
from hdx.scraper.hno.progress_json import ProgressJSON
from hdx.utilities.dateparse import now_utc
from hdx.utilities.downloader import Download
from hdx.utilities.easy_logging import setup_logging
from hdx.utilities.path import (
    progress_storing_folder,
    script_dir_plus_file,
    wheretostart_tempdir_batch,
)
from hdx.utilities.retriever import Retrieve

setup_logging()
logger = logging.getLogger(__name__)

lookup = "hdx-scraper-hno"
updated_by_script = "HDX Scraper: HPC HNO"

generate_country_resources = True
generate_global_dataset = True


def main(
    save: bool = False,
    use_saved: bool = False,
    countryiso3s: str = "",
    pcodes: str = "",
    save_test_data: bool = False,
) -> None:
    """Generate datasets and create them in HDX

    Args:
        save (bool): Save downloaded data. Defaults to False.
        use_saved (bool): Use saved data. Defaults to False.
        countryiso3s (str): Countries to process. Defaults to "" (all countries).
        pcodes (str): P-codes to process. Defaults to "" (all p-codes).
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
    with wheretostart_tempdir_batch(lookup) as info:
        folder = info["folder"]
        batch = info["batch"]
        configuration = Configuration.read()
        today = now_utc()
        year = today.year
        saved_dir = "saved_data"
        with Download(
            extra_params_yaml=join(expanduser("~"), ".extraparams.yaml"),
            extra_params_lookup=lookup,
            use_auth="basic_auth",
        ) as downloader:
            retriever = Retrieve(
                downloader, folder, saved_dir, folder, save, use_saved
            )
            plan = Plan(configuration, year, countryiso3s, pcodes)
            dataset_generator = DatasetGenerator(configuration, year)
            progress_json = ProgressJSON(year, saved_dir, save_test_data)
            plan_ids_countries = plan.get_plan_ids_and_countries(
                retriever, progress_json
            )
            plan.setup_admins(retriever)

        with Download(
            extra_params_yaml=join(expanduser("~"), ".extraparams.yaml"),
            extra_params_lookup=lookup,
            use_auth="bearer_token",
            rate_limit={"calls": 1, "period": 1},
        ) as downloader:
            retriever = Retrieve(
                downloader,
                folder,
                saved_dir,
                folder,
                save,
                use_saved,
                delete=False,
            )
            countries_with_data = []
            for _, plan_id_country in progress_storing_folder(
                info, plan_ids_countries, "iso3"
            ):
                countryiso3 = plan_id_country["iso3"]
                plan_id = plan_id_country["id"]
                monitor_json = MonitorJSON(saved_dir, save_test_data)
                published, rows = plan.process(
                    retriever, countryiso3, plan_id, monitor_json
                )
                if not rows:
                    continue
                countries_with_data.append(countryiso3)
                if not generate_country_resources:
                    continue
                dataset = dataset_generator.get_country_dataset(countryiso3)
                if not dataset:
                    logger.error(f"No dataset found for {countryiso3}!")
                    continue
                highest_admin = plan.get_highest_admin(countryiso3)
                resource = dataset_generator.add_country_resource(
                    dataset, countryiso3, rows, folder, year, highest_admin
                )
                if not resource:
                    continue
                resource.set_date_data_updated(published)
                dataset.preview_resource()
                dataset.update_in_hdx(
                    operation="patch",
                    match_resource_order=True,
                    remove_additional_resources=False,
                    hxl_update=False,
                    updated_by_script=updated_by_script,
                    batch=batch,
                )
                dataset.generate_quickcharts(
                    resource,
                    script_dir_plus_file(
                        join(
                            "config",
                            "hdx_country_resource_view_static.yaml",
                        ),
                        main,
                    ),
                )

            if generate_global_dataset:
                global_rows = plan.get_global_rows()
                global_highest_admin = plan.get_global_highest_admin()
                dataset = dataset_generator.generate_global_dataset(
                    folder,
                    global_rows,
                    countries_with_data,
                    year,
                    global_highest_admin,
                )
                if dataset:
                    # *** For old resource ***
                    resourcedata = {
                        "name": "Global HPC HNO 2024 Deprecated",
                        "description": "Deprecated global HNO resource (will be removed soon)",
                    }
                    rows = plan.get_old_global_rows()
                    hxltags = configuration["old_hxltags"]
                    success, results = dataset.generate_resource_from_iterable(
                        list(hxltags.keys()),
                        (rows[key] for key in sorted(rows)),
                        hxltags,
                        folder,
                        "hpc_hno_2024_deprecated.csv",
                        resourcedata,
                    )
                    # ***                  ***

                    dataset.update_from_yaml(
                        script_dir_plus_file(
                            join("config", "hdx_dataset_static.yaml"), main
                        )
                    )
                    dataset.generate_quickcharts(
                        0,
                        script_dir_plus_file(
                            join("config", "hdx_resource_view_static.yaml"),
                            main,
                        ),
                    )
                    dataset.create_in_hdx(
                        remove_additional_resources=False,
                        hxl_update=False,
                        updated_by_script=updated_by_script,
                        batch=batch,
                    )
                    # *** For old resource ***
                    # resources = dataset.get_resources()
                    # resource_ids = [
                    #     x["id"]
                    #     for x in sorted(
                    #         resources, key=lambda x: x["name"], reverse=True
                    #     )
                    # ]
                    # dataset.reorder_resources(resource_ids)
                    # ***                  ***

    logger.info("HDX Scraper HNO pipeline completed!")


if __name__ == "__main__":
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=lookup,
        project_config_yaml=script_dir_plus_file(
            join("config", "project_configuration.yaml"), main
        ),
    )
