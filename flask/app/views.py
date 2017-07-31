# Create views for the html page
from app import app
import json
import time
from flask import jsonify, render_template, request
import pandas as pd

@app.route('/')
@app.route('/index')
def index():
    return render_template('index.html')

@app.route('/maps')
def maps():
    return render_template('index.html')

@app.route('/batch')
def batch():
    return render_template('batch.html')

@app.route('/downloaddata')
def downloaddata():
    return render_template('downloaddata.html')

@app.route('/aboutme')
def aboutme():
    return render_template('aboutme.html')

@app.route('/realtime/<day_of_year>')
def realtime(day_of_year):
	dataframe = pd.read_csv('/Users/shiv/.bin/time-series-analysis-nyc-taxi/flask/out' + day_of_year +'.csv')
	return dataframe.to_json(orient='records')
