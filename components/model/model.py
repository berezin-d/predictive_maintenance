from sklearn import datasets
from sklearn.ensemble import RandomForestClassifier
import pickle


iris_data = datasets.load_iris()
X = iris_data.data
y = iris_data.target

clf = RandomForestClassifier()
clf.fit(X, y)

with open(r'xgb_model_iris.pickle', 'wb') as f:
    pickle.dump(clf, f)
