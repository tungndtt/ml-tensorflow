from keras.models import Sequential, save_model, load_model
from keras.layers import Dense, BatchNormalization
from keras.callbacks import EarlyStopping
import numpy
from psycopg2 import connect


fields = [
    "maincrop", "intercrop",
    "soil_type", "variety",
    "seed_density",
    "max_allowed_fertilizer",
    "fertilizer_applications",
    "soil_tillage_applications",
    "crop_protection_applications",
    "nitrate", "phosphor", "potassium", "ph", "rks",
    "harvest_weight"
]


def parse_record(record: tuple):
    if record is None:
        return None
    return {col: record[i] for i, col in enumerate(fields)}


def read_data():
    import os
    import sys
    sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
    from config import STORAGE
    from preprocess import encode_data
    X, y = [], []
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
        columns = ",".join(fields)
        cursor.execute(f"SELECT {columns} FROM season")
        for record in cursor:
            sample, label = encode_data(parse_record(record))
            X.append(sample)
            y.append(label)
    except Exception as error:
        status = False
        print(error)
    finally:
        cursor.close()
        conn.close()
    return (X, y) if status else (None, None)


def build_model(X, y):
    model = Sequential()
    model.add(BatchNormalization())
    model.add(Dense(512, activation="relu"))
    model.add(BatchNormalization())
    model.add(Dense(256, activation="relu"))
    model.add(BatchNormalization())
    model.add(Dense(128, activation="relu"))
    model.add(BatchNormalization())
    model.add(Dense(64, activation="relu"))
    model.add(BatchNormalization())
    model.add(Dense(32, activation="relu"))
    model.add(BatchNormalization())
    model.add(Dense(1))
    model.compile(optimizer="adam", loss="mean_squared_error")
    early_stopping = EarlyStopping(monitor="val_loss",
                                   patience=100,
                                   restore_best_weights=True)
    model.fit(numpy.array(X), numpy.array(y),
              epochs=2000,
              validation_split=0.1,
              callbacks=[])
    save_model(model, "D:/livesen-map/recommendation/data/model/model.keras")
    model.save_weights(
        "D:/livesen-map/recommendation/data/model/weights.h5"
    )


if __name__ == "__main__":
    X, y = read_data()
    if X:
        # construct model
        build_model(X, y)
        # inference
        # model = load_model(
        #     "D:/livesen-map/recommendation/data/model/model.keras")
        # model.load_weights(
        #     "D:/livesen-map/recommendation/data/model/weights.h5"
        # )
        # for i in range(10):
        #     print(model.predict([X[i]])[0][0])
