from pandas import read_excel, isna
import re


df = read_excel(io="example_table.xlsx", sheet_name="Sheet1")
patterns = [
    re.compile(r"Fertilizer Application (\d+) Fertilizer$"),
    re.compile(r"Soil Tillage Application (\d+) Type$"),
    re.compile(r"Crop Protection Application (\d+) Type$"),
    re.compile(r"Maincrop$"),
    re.compile(r"Variety$"),
    re.compile(r"Soil Type$"),
]
dicts = [dict(), dict(), dict(), dict(), dict(), dict()]
categories = [
    "fertilizer", "soil_tillage", "crop_protection", "crop", "variety", "soil"
]
for column in df:
    for pattern, data in zip(patterns, dicts):
        if pattern.match(column):
            for value in df[column]:
                if not isna(value):
                    for v in value.split("/"):
                        v = v.strip().lower()
                        if v not in data:
                            data[v] = 0
                        data[v] += 1
for data, category in zip(dicts, categories):
    with open(f"category/{category}.csv", "w", encoding="utf-8") as f:
        f.write(",".join([k for k, v in data.items() if v > 3]))
