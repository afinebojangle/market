from flask import Flask, render_template, request
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://rayford:rhtpas0!@localhost:5432/market_data'
db = SQLAlchemy(app)


if __name__ == '__main__':
    app.debug = True
    app.run()
