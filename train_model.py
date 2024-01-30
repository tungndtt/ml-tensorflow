import numpy
from keras.models import Sequential, save_model, load_model
from keras.layers import Dense, BatchNormalization
from keras.callbacks import EarlyStopping
from read_dataset import read_data
from preprocess_data import encode_data


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

model_path = "model/model.keras"
weights_path = "model/weights.h5"


def parse_record(record: tuple):
    if record is None:
        return None
    return {col: record[i] for i, col in enumerate(fields)}


def dataset():
    X, y = [], []
    for data in read_data():
        sample, label = encode_data(data)
        X.append(sample)
        y.append(label)
    return X, y


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
    save_model(model, model_path)
    model.save_weights(weights_path)


def predict_model(X):
    model = load_model(model_path)
    model.load_weights(weights_path)
    for i in range(10):
        print(model.predict([X[i]])[0][0])


if __name__ == "__main__":
    X, y = read_data()
    if X:
        build_model(X, y)
        # predict_model(X)
