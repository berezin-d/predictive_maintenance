from sklearn import datasets
from xgboost import XGBClassifier
import pickle


iris_data = datasets.load_iris()
X = iris_data.data
y = iris_data.target

clf = XGBClassifier()
clf.fit(X, y)

with open(r'xgb_model_iris.pickle', 'wb') as f:
    pickle.dump(clf, f)
