import os
import csv
import math
import logging
from flask import Flask, flash, redirect, render_template, request, session, url_for
from cs50 import SQL
from timezonefinder import TimezoneFinder
from pytz import timezone
import pytz
from datetime import datetime
import dateutil.parser

UPLOAD_FOLDER = 'static/'

# configure application
app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

tf = TimezoneFinder(in_memory=True)

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

# Deprecated function
# For parsing the CSV file
@app.route("/parse")
def parse():

    #CREATE TABLE 'earthquakes' ('time' datetime, 'latitude' double precision, 'longitude' double precision, 'depth' double precision, 'mag' real, 'magType' text, 'nst' integer, 'gap' double precision, 'dmin' double precision, 'rms' double precision, 'net' text, 'id' text, 'updated' datetime, 'place' text, 'type' text, 'horizontalError' double precision, 'depthError' double precision, 'magError' double precision, 'magNst' integer, 'status' text, 'locationSource' text, 'magSource' text)

    # Parsing the csv file
    csvfile = open('all_month.csv')
    testReader = csv.reader(csvfile)

    # Populating the SQLite table with data from csv file
    for row in testReader:
        db.execute("INSERT INTO earthquakes (time, latitude, longitude, depth, mag, magType, nst, gap, dmin, rms, net, id3, updated, place, type3, horizontalError, depthError, magError, magNst, status, locationSource, magSource) VALUES (:time, :latitude, :longitude, :depth, :mag, :magType, :nst, :gap, :dmin, :rms, :net, :id3, :updated, :place, :type3, :horizontalError, :depthError, :magError, :magNst, :status, :locationSource, :magSource)", time=row[0], latitude=row[1], longitude=row[2], depth=row[3], mag=row[4], magType=row[5], nst=row[6], gap=row[7], dmin=row[8], rms=row[9], net=row[10], id3=row[11], updated=row[12], place=row[13], type3=row[14], horizontalError=row[15], depthError=row[16], magError=row[17], magNst=row[18], status=row[19], locationSource=row[20], magSource=row[21])
    csvfile.close()

    return redirect(url_for("index"))

# Deprecated function
# For converting degrees to radians
@app.route("/convert")
def convert():

    rows = db.execute("SELECT latitude, longitude, id FROM earthquakes WHERE 1")

    for row in rows:
        latR = math.radians(row['latitude'])
        longR = math.radians(row['longitude'])
        id3 = row['id']
        db.execute("UPDATE earthquakes SET latR = :latR, longR = :longR WHERE id = :id3", latR=latR, longR=longR, id3=id3)

    return redirect(url_for("index"))

# Deprecated function.
# For updating the csv file.
@app.route("/updatecsv")
def updatecsv():

    # Parsing the csv file
    csvfile = open('all_month.csv')
    testReader = csv.reader(csvfile)

    csvRows = []

    # Updating the new rows
    for row in testReader:
        if testReader.line_num == 1:
            continue    # skip first row
        latR = math.radians(float(row[1]))
        longR = math.radians(float(row[2]))
        coslatR = math.cos(latR)
        sinlatR = math.sin(latR)
        coslongR = math.cos(longR)
        sinlongR = math.sin(longR)
        row.append(latR)
        row.append(longR)
        row.append(coslatR)
        row.append(sinlatR)
        row.append(coslongR)
        row.append(sinlongR)
        csvRows.append(row)

    csvfile.close()

    csvfile = open('all_month_2.csv','w',newline='')

    testWriter = csv.writer(csvfile)
    for row in csvRows:
        testWriter.writerow(row)

    csvfile.close()

    return redirect(url_for("index"))

# For searching by magnitude
@app.route("/searchmag", methods=["GET", "POST"])
def searchmag():

    # if user reached route via POST (as by submitting a form via POST)
    if request.method == "POST":
        magnitude = request.form.get("magnitude")

        rows = db.execute("SELECT * FROM earthquakes WHERE mag >= :magnitude", magnitude=magnitude)

        return render_template("searchmagr.html", rows=rows)

    # if user reached route via GET (as in by clicking on a link or via redirect)
    else:
        return render_template("searchmag.html")

# For searching by magnitude and date
@app.route("/searchmagdate", methods=["GET", "POST"])
def searchmagdate():

    # if user reached route via POST (as by submitting a form via POST)
    if request.method == "POST":
        magnitude1 = request.form.get("magnitude1")
        magnitude2 = request.form.get("magnitude2")
        start = request.form.get("start") + 'T00:00:00.000Z'
        end = request.form.get("end") + 'T23:59:59.999Z'

        rows = db.execute("SELECT * FROM earthquakes WHERE mag >= :magnitude1 AND mag <= :magnitude2 AND time >= :start AND time <= :end", magnitude1=magnitude1, magnitude2=magnitude2, start=start, end=end)

        return render_template("searchmagr.html", rows=rows)

    # if user reached route via GET (as in by clicking on a link or via redirect)
    else:
        return render_template("searchmagdate.html")

# For searching by location
@app.route("/searchlocation", methods=["GET", "POST"])
def searchlocation():

    # if user reached route via POST (as by submitting a form via POST)
    if request.method == "POST":

        # http://janmatuschek.de/LatitudeLongitudeBoundingCoordinates
        # https://stackoverflow.com/questions/3126830/query-to-get-records-based-on-radius-in-sqlite
        # http://www.movable-type.co.uk/scripts/latlong.html

        # Using Spherical Law of Cosines
        # Getting info from HTML form
        latitude = request.form.get("latitude")
        longitude = request.form.get("longitude")
        radius = request.form.get("radius")

        # Converting to radian and then sin and cos of that
        latitudeR = math.radians(float(latitude))
        cos_latitudeR = math.cos(latitudeR)
        sin_latitudeR = math.sin(latitudeR)

        longitudeR = math.radians(float(longitude))
        cos_longitudeR = math.cos(longitudeR)
        sin_longitudeR = math.sin(longitudeR)

        cos_radius = math.cos(float(radius) / 6371)

        rows2 = db.execute("SELECT * FROM earthquakes WHERE 1")
        rows = []

        for row in rows2:
            latR = math.radians(float(row['latitude']))
            longR = math.radians(float(row['longitude']))
            coslatR = math.cos(latR)
            sinlatR = math.sin(latR)
            coslongR = math.cos(longR)
            sinlongR = math.sin(longR)
            if (sin_latitudeR * sinlatR + cos_latitudeR * coslatR * (coslongR * cos_longitudeR + sinlongR * sin_longitudeR) > cos_radius):
                rows.append(row)

        return render_template("searchmagr.html", rows=rows)

    # if user reached route via GET (as in by clicking on a link or via redirect)
    else:
        return render_template("searchlocation.html")

@app.route("/searchtime", methods=["GET", "POST"])
def searchtime():

    # if user reached route via POST (as by submitting a form via POST)
    if request.method == "POST":

        magnitude = request.form.get("magnitude")
        start = (dateutil.parser.parse(request.form.get("start"))).time()
        end = (dateutil.parser.parse(request.form.get("end"))).time()

        rows2 = db.execute("SELECT * FROM earthquakes WHERE mag >= :magnitude", magnitude=magnitude)
        rows = []

        for row in rows2:
            # https://pypi.org/project/timezonefinder/
            # Calculates the offset from UTC based on the location
            local_timezone_string = tf.timezone_at(lat=row['latitude'], lng=row['longitude'])
            if local_timezone_string is None:
                local_timezone_string = tf.certain_timezone_at(lat=row['latitude'], lng=row['longitude'])

                if local_timezone_string is None:
                    local_timezone_string = tf.closest_timezone_at(lat=row['latitude'], lng=row['longitude'])

                    if local_timezone_string is None:
                        local_timezone_string = tf.closest_timezone_at(lat=row['latitude'], lng=row['longitude'], delta_degree=3)

                        if local_timezone_string is None:
                            local_timezone_string = tf.closest_timezone_at(lat=row['latitude'], lng=row['longitude'], delta_degree=6)

                            if local_timezone_string is None:
                                local_timezone_string = tf.closest_timezone_at(lat=row['latitude'], lng=row['longitude'], delta_degree=15)

            #print(local_timezone_string, row['place'])

            if local_timezone_string is not None:
                local_timezone = timezone(local_timezone_string)

                row_time = dateutil.parser.parse(row['time'])
                local_time = row_time.astimezone(local_timezone).time()

                if local_time >= start and local_time <= end:
                    rows.append(row)

        return render_template("searchmagr.html", rows=rows)

    # if user reached route via GET (as in by clicking on a link or via redirect)
    else:
        return render_template("searchtime.html")

@app.route("/cluster", methods=["GET", "POST"])
def cluster():

    # if user reached route via POST (as by submitting a form via POST)
    if request.method == "POST":

        latitude1 = request.form.get("latitude1")
        longitude1 = request.form.get("longitude1")

        latitude2 = request.form.get("latitude2")
        longitude2 = request.form.get("longitude2")

        cellsize = request.form.get("cellsize")
        magnitude = request.form.get("magnitude")

        # Converting to radian and then sin and cos for 1st set of latitude and longitude
        latitudeR = math.radians(float(latitude1))
        cos_latitudeR = math.cos(latitudeR)
        sin_latitudeR = math.sin(latitudeR)

        longitudeR = math.radians(float(longitude1))
        cos_longitudeR = math.cos(longitudeR)
        sin_longitudeR = math.sin(longitudeR)

        latitudeR2 = math.radians(float(latitude2))
        cos_latitudeR2 = math.cos(latitudeR2)
        sin_latitudeR2 = math.sin(latitudeR2)

        longitudeR2 = math.radians(float(longitude2))
        cos_longitudeR2 = math.cos(longitudeR2)
        sin_longitudeR2 = math.sin(longitudeR2)


        radius = request.form.get("radius") # Comment this if distance is not given
        # If Distance(radius) is not given then calculate with this,
        # radius = math.acos(sin_latitudeR * sin_latitudeR2 + cos_latitudeR * cos_latitudeR2 * math.cos(longitudeR - longitudeR2))

        cos_radius = math.cos(float(radius) / 6371)

        rows2 = db.execute("SELECT * FROM earthquakes WHERE mag >= :magnitude", magnitude=magnitude)
        rows = []
        total_lat = 0
        total_long = 0

        for row in rows2:
            latR = math.radians(float(row['latitude']))
            longR = math.radians(float(row['longitude']))
            coslatR = math.cos(latR)
            sinlatR = math.sin(latR)
            coslongR = math.cos(longR)
            sinlongR = math.sin(longR)
            if (sin_latitudeR * sinlatR + cos_latitudeR * coslatR * (coslongR * cos_longitudeR + sinlongR * sin_longitudeR) > cos_radius) and (sin_latitudeR2 * sinlatR + cos_latitudeR2 * coslatR * (coslongR * cos_longitudeR2 + sinlongR * sin_longitudeR2) > cos_radius):
                rows.append(row)
                total_lat += float(row['latitude'])
                total_long += float(row['longitude'])

        mean_lat = total_lat / len(rows)
        mean_long = total_long / len(rows)

        # Converting to radian and then sin and cos of that
        mean_latR = math.radians(mean_lat)
        cos_mean_latR = math.cos(mean_latR)
        sin_mean_latR = math.sin(mean_latR)

        mean_longR = math.radians(mean_long)
        cos_mean_longR = math.cos(mean_longR)
        sin_mean_longR = math.sin(mean_longR)

        cos_cellsize = math.cos(float(cellsize) / 6371)

        rows3 = []
        for row in rows:
            latR = math.radians(float(row['latitude']))
            longR = math.radians(float(row['longitude']))
            coslatR = math.cos(latR)
            sinlatR = math.sin(latR)
            coslongR = math.cos(longR)
            sinlongR = math.sin(longR)
            if (sin_mean_latR * sinlatR + cos_mean_latR * coslatR * (coslongR * cos_mean_longR + sinlongR * sin_mean_longR) > cos_cellsize):
                rows3.append(row)

        return render_template("clusterer.html", rows=rows3, count=len(rows), mean_lat=mean_lat, mean_long=mean_long)
     # if user reached route via GET (as in by clicking on a link or via redirect)
    else:
        return render_template("cluster.html")

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=port, debug=True, threaded=True)