"""Entry point to start HAPI HNO pipeline"""

import logging
from os.path import expanduser, join

from hdx.api.configuration import Configuration
from hdx.facades.infer_arguments import facade
from hdx.utilities.dateparse import now_utc
from hdx.utilities.downloader import Download
from hdx.utilities.easy_logging import setup_logging
from hdx.utilities.path import (
    progress_storing_folder,
    script_dir_plus_file,
    wheretostart_tempdir_batch,
)
from hdx.utilities.retriever import Retrieve

from src.hapi.pipeline.hno.plan import Plan

from hapi.pipeline.hno._version import __version__

setup_logging()
logger = logging.getLogger(__name__)


lookup = "hapi-pipeline-hno"
updated_by_script = "HAPI Pipeline: HNO"


def main(
    save: bool = False, use_saved: bool = False, countryiso3s: str = ""
) -> None:
    """Generate datasets and create them in HDX

    Args:
        save (bool): Save downloaded data. Defaults to False.
        use_saved (bool): Use saved data. Defaults to False.
        countryiso3s (str): Countries to process. Defaults to "" (all countries).
    Returns:
        None
    """
    logger.info(f"##### {lookup} version {__version__} ####")
    with wheretostart_tempdir_batch(lookup) as info:
        folder = info["folder"]
        batch = info["batch"]
        configuration = Configuration.read()
        plan = Plan(configuration, now_utc(), countryiso3s)
        with Download(
            extra_params_yaml=join(expanduser("~"), ".extraparams.yaml"),
            extra_params_lookup=lookup,
            use_auth="basic_auth",
        ) as downloader:
            retriever = Retrieve(
                downloader, folder, "saved_data", folder, save, use_saved
            )
            plan_ids_countries = plan.get_plan_ids_and_countries(retriever)

        with Download(
            extra_params_yaml=join(expanduser("~"), ".extraparams.yaml"),
            extra_params_lookup=lookup,
            use_auth="bearer_token",
            rate_limit={"calls": 1, "period": 1},
        ) as downloader:
            retriever = Retrieve(
                downloader,
                folder,
                "saved_data",
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
                rows = plan.process(retriever, countryiso3, plan_id)
                dataset = plan.generate_dataset(countryiso3, rows, folder)
                if dataset:
                    dataset.update_from_yaml(
                        script_dir_plus_file("hdx_dataset_static.yaml", main)
                    )
                    dataset.create_in_hdx(
                        remove_additional_resources=True,
                        hxl_update=False,
                        updated_by_script=updated_by_script,
                        batch=info["batch"],
                    )

    logger.info("HAPI HNO pipeline completed!")


if __name__ == "__main__":
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=lookup,
        project_config_yaml=script_dir_plus_file(
            "project_configuration.yaml", main
        ),
    )
