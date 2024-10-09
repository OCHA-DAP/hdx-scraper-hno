from pytest_check import check

from hdx.scraper.hno.dataset_generator import DatasetGenerator


class TestDatasetGenerator:
    def test_move_resource(self):
        input_resource_names = [
            "afg_hpc_needs_2024.xlsx",
            "afg_hpc_needs_2023.xlsx",
            "afg_hpc_needs_2022.xlsx",
            "afg_hpc_needs_2021.xlsx",
            "afg_hpc_needs_api_2024.csv",
        ]
        resources = [{"name": x} for x in input_resource_names]
        resource = DatasetGenerator.move_resource(resources, "AFG", 2024)
        check.equal(resource["name"], "afg_hpc_needs_api_2024.csv")
        resource_names = [x["name"] for x in resources]
        check.equal(
            resource_names,
            [
                "afg_hpc_needs_api_2024.csv",
                "afg_hpc_needs_2024.xlsx",
                "afg_hpc_needs_2023.xlsx",
                "afg_hpc_needs_2022.xlsx",
                "afg_hpc_needs_2021.xlsx",
            ],
        )
        resource = DatasetGenerator.move_resource(resources, "AFG", 2024)
        check.equal(resource["name"], "afg_hpc_needs_api_2024.csv")
        resource_names = [x["name"] for x in resources]
        check.equal(
            resource_names,
            [
                "afg_hpc_needs_api_2024.csv",
                "afg_hpc_needs_2024.xlsx",
                "afg_hpc_needs_2023.xlsx",
                "afg_hpc_needs_2022.xlsx",
                "afg_hpc_needs_2021.xlsx",
            ],
        )

        input_resource_names = [
            "afg_hpc_needs_2024.xlsx",
            "afg_hpc_needs_2023.xlsx",
            "afg_hpc_needs_2022.xlsx",
            "afg_hpc_needs_2021.xlsx",
            "afg_hpc_needs_api_2021.csv",
        ]
        resources = [{"name": x} for x in input_resource_names]
        _ = DatasetGenerator.move_resource(resources, "AFG", 2021)
        resource_names = [x["name"] for x in resources]
        check.equal(
            resource_names,
            [
                "afg_hpc_needs_2024.xlsx",
                "afg_hpc_needs_2023.xlsx",
                "afg_hpc_needs_2022.xlsx",
                "afg_hpc_needs_api_2021.csv",
                "afg_hpc_needs_2021.xlsx",
            ],
        )
        _ = DatasetGenerator.move_resource(resources, "AFG", 2021)
        resource_names = [x["name"] for x in resources]
        check.equal(
            resource_names,
            [
                "afg_hpc_needs_2024.xlsx",
                "afg_hpc_needs_2023.xlsx",
                "afg_hpc_needs_2022.xlsx",
                "afg_hpc_needs_api_2021.csv",
                "afg_hpc_needs_2021.xlsx",
            ],
        )

        input_resource_names = [
            "afg_hpc_needs_2023.xlsx",
            "afg_hpc_needs_2022.xlsx",
            "afg_hpc_needs_2021.xlsx",
            "afg_hpc_needs_api_2024.csv",
        ]
        resources = [{"name": x} for x in input_resource_names]
        _ = DatasetGenerator.move_resource(resources, "AFG", 2024)
        resource_names = [x["name"] for x in resources]
        check.equal(
            resource_names,
            [
                "afg_hpc_needs_api_2024.csv",
                "afg_hpc_needs_2023.xlsx",
                "afg_hpc_needs_2022.xlsx",
                "afg_hpc_needs_2021.xlsx",
            ],
        )
        _ = DatasetGenerator.move_resource(resources, "AFG", 2024)
        resource_names = [x["name"] for x in resources]
        check.equal(
            resource_names,
            [
                "afg_hpc_needs_api_2024.csv",
                "afg_hpc_needs_2023.xlsx",
                "afg_hpc_needs_2022.xlsx",
                "afg_hpc_needs_2021.xlsx",
            ],
        )
        input_resource_names = [
            "afg_hpc_needs_2024.xlsx",
            "afg_hpc_needs_2023.xlsx",
            "afg_hpc_needs_api_2024.csv",
            "afg_hpc_needs_2022.xlsx",
            "afg_hpc_needs_2021.xlsx",
        ]
        resources = [{"name": x} for x in input_resource_names]
        resource = DatasetGenerator.move_resource(resources, "AFG", 2024)
        check.equal(resource["name"], "afg_hpc_needs_api_2024.csv")
        resource_names = [x["name"] for x in resources]
        check.equal(
            resource_names,
            [
                "afg_hpc_needs_api_2024.csv",
                "afg_hpc_needs_2024.xlsx",
                "afg_hpc_needs_2023.xlsx",
                "afg_hpc_needs_2022.xlsx",
                "afg_hpc_needs_2021.xlsx",
            ],
        )
