import os
from flask import Flask, flash, redirect, render_template, request, session, url_for
from cs50 import SQL
import redis
import random
import logging
from timeit import default_timer as timer

UPLOAD_FOLDER = 'static/'

r = redis.Redis(host='localhost', port=6379, db=0)

# configure application
app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# ensure responses aren't cached
if app.config["DEBUG"]:
    @app.after_request
    def after_request(response):
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        response.headers["Expires"] = 0
        response.headers["Pragma"] = "no-cache"
        return response

# logger = logging.getLogger('cs50')
# logger.propagate = False

logger = logging.getLogger('cs50')
logger.disabled = True

# configure CS50 Library to use SQLite database
db = SQL("sqlite:///csv.db")

# On IBM Cloud Cloud Foundry, get the port number from the environment variable PORT
# When running this app on the local machine, default the port to 8000
port = int(os.getenv('PORT', 8000))

# Main Index page
@app.route("/")
def index():

    # Extracting the entire SQlite table and then displaying it.
    rows = db.execute("SELECT * FROM earthquakes WHERE 1")

    return render_template("index.html", rows=rows)

# For searching by magnitude
@app.route("/searchmag", methods=["GET", "POST"])
def searchmag():

    # if user reached route via POST (as by submitting a form via POST)
    if request.method == "POST":
        range1 = request.form.get("range1")
        range2 = request.form.get("range2")
        range3 = request.form.get("range3")
        range4 = request.form.get("range4")

        magnitude1 = random.randint(float(range1), float(range2))
        magnitude2 = random.randint(float(range3), float(range4))

        sql = "SELECT COUNT (*) FROM earthquakes WHERE mag BETWEEN {} AND {}".format(magnitude1, magnitude2)

        start = timer()

        # Check if results in cache
        count =  r.get(sql)

        # https://d0.awsstatic.com/whitepapers/Database/database-caching-strategies-using-redis.pdf
        # If results not in cache    
        if count is None:

            # Query the database and add the results to cache
            rows = db.execute(sql)
            r.set(sql, str(rows[0]))

            end = timer()
            time = end - start
            return render_template("searchmagr.html", magnitude1=magnitude1, magnitude2=magnitude2, rows=rows, time=time)
        # Else,     
        else:
            end = timer()
            time = end - start
            return render_template("searchmagr.html", magnitude1=magnitude1, magnitude2=magnitude2, rows=int(count), time=time)

    # if user reached route via GET (as in by clicking on a link or via redirect)
    else:
        return render_template("searchmag.html")

# if __name__ == '__main__':
#    app.run(host='0.0.0.0', port=port, debug=True, threaded=True)