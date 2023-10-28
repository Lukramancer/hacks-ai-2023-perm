from flask import Flask, request, jsonify

from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker

from datetime import datetime

from src.tables import Event, Base, District

from env import DATABASE

app = Flask(__name__)


engine = create_engine(DATABASE)
db_session_maker = sessionmaker(bind=engine)
db_session = db_session_maker()

Base.metadata.create_all(engine)


@app.get("/get-events")
def get_events():
    date = request.args.get("date")
    if date is None:
        return "Error: date is not specified"

    try:
        date = datetime.strptime(date, "%d.%m.%Y").date()
    except ValueError:
        return "Error: date is not correct"

    types = request.args.get("types")
    if types is not None:
        types = types.split(',')
        events = db_session.query(Event).filter((func.DATE(Event.time) == date) & Event.event.in_(types)).all()
    else:
        events = db_session.query(Event).filter(func.DATE(Event.time) == date).all()

    return jsonify(events)


@app.get("/get-districts")
def get_districts():
    return jsonify(db_session.query(District).all())


@app.get("/get-events-types")
def get_events_types():
    events_types = set[str](map(lambda query_result_row: query_result_row[0], db_session.query(Event.event).distinct().all()))
    return jsonify(list(events_types))


if __name__ == "__main__":
    app.run()
