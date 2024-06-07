"""Entry point to start HAPI HNO pipeline"""

import logging
from os.path import expanduser, join

from hdx.api.configuration import Configuration
from hdx.data.user import User
from hdx.facades.infer_arguments import facade
from hdx.scraper.hno._version import __version__
from hdx.scraper.hno.monitor_json import MonitorJSON
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

from src.hdx.scraper.hno.plan import Plan

setup_logging()
logger = logging.getLogger(__name__)

lookup = "hdx-scraper-hno"
updated_by_script = "HDX Scraper: HPC HNO"


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
    if not User.check_current_user_organization_access("hdx", "create_dataset"):
        raise PermissionError(
            "API Token does not give access to HDX organisation!"
        )
    with wheretostart_tempdir_batch(lookup) as info:
        folder = info["folder"]
        batch = info["batch"]
        configuration = Configuration.read()
        today = now_utc()
        year = today.year
        saved_dir = "saved_data"
        plan = Plan(configuration, year, countryiso3s, pcodes)
        with Download(
            extra_params_yaml=join(expanduser("~"), ".extraparams.yaml"),
            extra_params_lookup=lookup,
            use_auth="basic_auth",
        ) as downloader:
            retriever = Retrieve(
                downloader, folder, saved_dir, folder, save, use_saved
            )
            progress_json = ProgressJSON(year, saved_dir, save_test_data)
            plan_ids_countries = plan.get_plan_ids_and_countries(
                retriever, progress_json
            )

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
            for _, plan_id_country in progress_storing_folder(
                info, plan_ids_countries, "iso3"
            ):
                countryiso3 = plan_id_country["iso3"]
                plan_id = plan_id_country["id"]
                monitor_json = MonitorJSON(saved_dir, save_test_data)
                rows = plan.process(
                    retriever, countryiso3, plan_id, monitor_json
                )
                dataset = plan.generate_dataset(countryiso3, rows, folder)
                if dataset:
                    dataset.update_from_yaml(
                        script_dir_plus_file("hdx_dataset_static.yaml", main)
                    )
                    dataset.create_in_hdx(
                        remove_additional_resources=True,
                        hxl_update=False,
                        updated_by_script=updated_by_script,
                        batch=batch,
                    )

    logger.info("HDX Scraper HNO pipeline completed!")


if __name__ == "__main__":
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=lookup,
        project_config_yaml=script_dir_plus_file(
            "project_configuration.yaml", main
        ),
    )
