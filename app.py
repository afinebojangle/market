from flask import Flask, render_template, request
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://bojangles:apppas0!@marketdata.chew6qxftqgr.us-east-1.rds.amazonaws.com:5432/marketdata'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_ECHO'] = True
db = SQLAlchemy(app)


if __name__ == '__main__':
    app.debug = True
    app.run()
