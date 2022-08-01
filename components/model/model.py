from sklearn import datasets
from sklearn.ensemble import RandomForestClassifier
import pickle


class Model:
    def __init__(self, id) -> None:
        self._id = id
        self._model_engine = None
        self._initialized = False

    def check_status(self):
        return self._initialized

    def get_model(id: str):
        model = Model(id)

        # check if id exists in model vault and load
        

        return model

    def initialize(self, model):
        self._model_engine = model
        self._initialized = True

    def fit_model(self, X, y, **kwargs):
        model = self.get_model(self)
        model.fit(X, y, **kwargs)

        return 1




# iris_data = datasets.load_iris()
# X = iris_data.data
# y = iris_data.target

# clf = RandomForestClassifier()
# clf.fit(X, y)

# with open(r'xgb_model_iris.pickle', 'wb') as f:
#     pickle.dump(clf, f)
