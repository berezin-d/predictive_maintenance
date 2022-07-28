import dash
from dash import dcc
from dash import html
from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc
import numpy as np
import pickle


app = dash.Dash(__name__)
app.title = 'ML model deployment'
server = app.server

with open('xgb_model_iris.pickle', 'rb') as f:
    clf = pickle.load(f)


app.layout = html.Div([
    dbc.Row([html.H3(children='Predict Iris Species')]),
    dbc.Row([
        dbc.Col(html.Label(children='Sepal Length (CM):'), width={"order": "first"}),
        dbc.Col(dcc.Slider(min=4, max=8, value=5, id='sepal_length')),
    ]),
    dbc.Row([
        dbc.Col(html.Label(children='Sepal Width (CM):'), width={"order": "first"}),
        dbc.Col(dcc.Slider(min=2, max=5, value=3, id='sepal_width')),
    ]),
    dbc.Row([
        dbc.Col(html.Label(children='Petal Length (CM):'), width={"order": "first"}),
        dbc.Col(dcc.Slider(min=1, max=8, value=5, id='petal_length')),
    ]),
    dbc.Row([
        dbc.Col(html.Label(children='Petal Width (CM):'), width={"order": "first"}),
        dbc.Col(dcc.Slider(min=0, max=5, value=3, id='petal_width')),
    ]),
    dbc.Row([
        dbc.Button('Submit', id='submit-val', n_clicks=0, color='primary')
    ]),
    html.Br(),
    dbc.Row([html.Div(id='prediction_output')]),
], style={'padding': '0px 0px 0px 150px', 'width': '50%'})


@app.callback(
    Output('prediction_output', 'children'),
    Input('submit-val', 'n_clicks'),
    State('sepal_length', 'value'),
    State('sepal_width', 'value'),
    State('petal_length', 'value'),
    State('petal_width', 'value'),
)
def update_output(n_clicks, sepal_length, sepal_width, petal_length, petal_width):
    x = np.array([[float(sepal_length), float(sepal_width), float(petal_length), float(petal_width)]])
    prediction = clf.predict(x)[0]
    if prediction == 0:
        output = 'Iris-Sentosa'
    elif prediction == 1:
        output = 'Iris-Versicolor'
    else:
        output = 'Iris-Virginica'
    
    return output

if __name__ == '__main__':
    app.run_server(debug=True)
