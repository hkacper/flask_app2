import os
from cerberus import Validator
from flask import Flask, abort, jsonify, render_template, request
from sqlalchemy import create_engine, func
from sqlalchemy.orm import scoped_session, sessionmaker

import models2 as models
from models2 import Base

DATABASE_URL = os.environ['DATABASE_URL']
engine = create_engine(DATABASE_URL)
# export DATABASE_URL="postgres://qosxgmgvgqkscx:5bd1c12f805a4297c0ee81ded08a7ed581f95a88048226d892c2d2e1fd882817@ec2-54-195-252-243.eu-west-1.compute.amazonaws.com:5432/dflbpq85c6k03c"
# engine = create_engine("postgresql://postgres:postgres@localhost:5432/chinook")


# Cerberus schemas
artist_schema = {'name': {'type': 'string'}}
v = Validator(artist_schema)

db_session = scoped_session(
    sessionmaker(autocommit=False, autoflush=False, bind=engine)
)

Base.query = db_session.query_property()

app = Flask(__name__)


@app.teardown_appcontext
def shutdown_session(exception=None):
    db_session.remove()


@app.route("/counter", methods=["GET"])
def counter():
    db_session.query(models.Counter).filter(models.Counter.counter_id == 1).update(
        {"counter_val": (models.Counter.counter_val + 1)})
    db_session.commit()
    counter = db_session.query(models.Counter).filter(
        models.Counter.counter_id == 1)
    return str(counter[0].counter_val)


@app.route("/artists", methods=["POST"])
def artists():
    if request.method == "POST":
        data = request.get_json()
        if v.validate(data):
            artist = models.Artist(name=data['name'])
            db_session.add(artist)
            db_session.commit()
            return jsonify(artist.as_dict())
        else:
            return "Wrong json format", 400
    abort(405)


@app.route("/count_songs", methods=["GET"])
def songs():
    # TODO sql optimization
    # SELECT artist.name, count(track) FROM track
    # JOIN album ON album.album_id = track.album_id
    # JOIN artist ON artist.artist_id = album.artist_id
    # WHERE artist.name = 'AC/DC' OR artist.name = 'Accept'
    # GROUP BY artist.name
    if request.method == "GET":
        param = request.args.get('artist')
        if param is None:
            return '', 404
        param = param.split(',')
        a = db_session.query(models.Track, models.Album, models.Artist)\
            .filter(models.Track.album_id == models.Album.album_id)\
            .filter(models.Album.artist_id == models.Artist.artist_id)\
            .filter(models.Artist.name.in_((param))).all()
        # FIXME pls xD
        lul = dict()
        for xd in a:
            if xd[2].name in lul.keys():
                lul[xd[2].name] += 1
            else:
                lul[xd[2].name] = 1
        if len(lul) > 0:
            return jsonify(lul)
        else:
            return '', 404
    abort(405)


@app.route("/longest_tracks", methods=["GET"])
def get_longest_tracks():
    tracks = db_session.query(models.Track).order_by(
        models.Track.milliseconds.desc()).limit(10)
    l = list()
    for t in tracks:
        l.append(t.as_dict())
    return jsonify(l)


@app.route("/longest_tracks_by_artist", methods=["GET"])
def get_longest_by_artist():
    artist = request.args.get('artist')
    if artist is None:
        return '', 404
    tracks_all = db_session.query(models.Track, models.Album, models.Artist)\
        .filter(models.Track.album_id == models.Album.album_id)\
        .filter(models.Album.artist_id == models.Artist.artist_id)\
        .filter(models.Artist.name == artist)\
        .order_by(models.Track.milliseconds.desc()).limit(10).all()
    l = list()
    for t in tracks_all:
        l.append(t[0].as_dict())
    if len(l) > 0:
        return jsonify(l)
    else:
        return '', 404


if __name__ == "__main__":
    app.run(debug=True)
