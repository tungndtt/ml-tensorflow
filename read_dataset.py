import re
from pandas import read_excel, isna


def read_data():
    df = read_excel(io="example_table.xlsx", sheet_name="Sheet1")
    patterns = [
        re.compile(r"Fertilizer Application (\d+) Fertilizer$"),
        re.compile(r"Soil Tillage Application (\d+) Type$"),
        re.compile(r"Crop Protection Application (\d+) Type$"),
    ]
    category_fields = ["fertilizer", "type", "type"]
    rename_patterns = [
        [
            ("Fertilizer Application %s Share plant-available nitrogen", "nitrogen"),
            ("Fertilizer Application %s Amount ", "amount")
        ],
        None,
        [("Crop Protection Application %s Amount", "amount")],
    ]
    categories = [
        "fertilizer_applications",
        "soil_tillage_applications",
        "crop_protection_applications"
    ]
    for _, row in df.iterrows():
        data = {
            "fertilizer_applications": [],
            "soil_tillage_applications": [],
            "crop_protection_applications": []
        }
        # extract applications information
        for column, value in row.items():
            for (
                pattern, field, rename_pattern, category
            ) in zip(patterns, category_fields, rename_patterns, categories):
                matching = pattern.match(column)
                if matching:
                    index = int(matching.group(1))
                    if not isna(value):
                        if rename_pattern:
                            record = {}
                            for target_pattern, rename_field in rename_pattern:
                                field_value = row[target_pattern % index]
                                if type(field_value) == str:
                                    field_values = field_value.split("/")
                                    values = value.split("/")
                                    for i, fv in enumerate(field_values):
                                        record[field] = values[i]
                                        record[rename_field] = (
                                            float(fv.strip())
                                        )
                                else:
                                    record[field] = value
                                    record[rename_field] = (
                                        field_value
                                        if field_value and not isna(field_value)
                                        else 0
                                    )
                            data[category].append(record)
                        else:
                            record = {}
                            record[field] = value
                            data[category].append(record)
        # extract other information
        for column, field in [
            ("Maincrop", "maincrop"),
            ("Variety", "variety"),
            ("Soil Type", "soil_type"),
            ("Düngebedarfsermittlung", "max_allowed_fertilizer"),
            ("Seed density kg/ha", "seed_density"),
            ("Nitrat", "nitrate"),
            ("Phosphor", "phosphor"),
            ("Kalium", "potassium"),
            ("PH Value", "ph"),
            ("RKS Value", "rks"),
            ("Weight Harvest", "harvest_weight")
        ]:
            value = row[column]
            data[field] = (
                value
                if value and not isna(value) and value != "-"
                else None
            )
        value = row["Intercrop"]
        data["intercrop"] = value if value != "-" else None
        yield data
