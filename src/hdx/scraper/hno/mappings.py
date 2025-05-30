from os.path import join

from hdx.scraper.framework.utilities.reader import Read
from hdx.utilities.path import script_dir_plus_file


def get_category_mapping():
    path = script_dir_plus_file(
            join("config", "category_mapping.csv"), get_category_mapping)
    header, iterator = Read.get_reader().get_tabular_rows(path, dict_form=True)
    category_mapping = {}
    for row in iterator:
        category_mapping[row["categoryName"].lower()] = (row["categoryName"], row["gender"], row["age"], row["disability"], row["status"])
    return category_mapping