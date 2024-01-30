import uuid
import re
from pandas import read_excel, isna
from psycopg2 import connect
from psycopg2.extras import Json


def read_data():
    df = read_excel(
        io="D:/livesen-map/recommendation/experiment-scripts/example_table.xlsx",
        sheet_name="Sheet1"
    )
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


def extract(data):
    cols, vals = [], []
    for col in [
        "maincrop", "intercrop",
        "soil_type", "variety",
        "seed_density",
        "max_allowed_fertilizer",
        "nitrate", "phosphor", "potassium", "ph", "rks",
        "harvest_weight"
    ]:
        if col in data and data[col] is not None:
            cols.append(col)
            vals.append(data[col])
    for col in [
        "fertilizer_applications",
        "soil_tillage_applications",
        "crop_protection_applications",
    ]:
        if col in data and data[col] is not None:
            cols.append(col)
            vals.append(Json(data[col]))
    return cols, vals


if __name__ == "__main__":
    import os
    import sys
    sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
    from config import STORAGE
    db_params = {
        "dbname": STORAGE.dbname,
        "user": STORAGE.user,
        "password": STORAGE.password,
        "host": STORAGE.host,
        "port": STORAGE.port,
    }
    conn = connect(**db_params)
    cursor = conn.cursor()
    status = True
    try:
        dummy_region = """
        POLYGON((
            -107.99560546875 41.17865397233169,
            -110.45654296875001 33.5047590692261,
            -95.16357421874999 34.30714385628805,
            -107.99560546875 41.17865397233169
        ))
        """
        cursor.execute(
            "INSERT INTO field(id, user_id, name, region) VALUES (%s, %s, %s, %s)",
            (1, 1, str(uuid.uuid4()), dummy_region,)
        )
        id = 0
        for data in read_data():
            cols, vals = extract(data)
            inserted_season = None
            insert_cols = ", ".join(cols)
            insert_vals = ", ".join(["%s" for _ in range(len(vals))])
            insert_cmd = f"""
            INSERT INTO season(user_id, field_id, season_id, {insert_cols})
            VALUES (%s, %s, %s, {insert_vals})
            RETURNING *
            """
            cursor.execute(insert_cmd, (1, 1, f"season-{id}", *vals,))
            id += 1
    except Exception as error:
        status = False
        print(error)
    finally:
        if status:
            conn.commit()
        else:
            conn.rollback()
        cursor.close()
        conn.close()
