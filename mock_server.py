import flask
from flask import Flask, Response
import json


app = flask.Flask(__name__)
app.config["DEBUG"] = True

registered_services = {
    "kite_streaming_service": "http://localhost:8080/health",
    "bse_result_polling_service": "http://localhost:8081/health",
    "health_monitor_service": "http://localhost:8082/health"
}


@app.route('/api/v1/facility/<facilityId>/transfer/slot_config/update', method=['GET'])
def update_slot_config(facilityId):
    print("received facilityId " + facilityId)
    response = app.response_class(response = json.dumps({'status': "done"}), status=200, mimetype='application/json')
    return response


@app.route('/run_checks', methods=['GET'])
def run_health_checks():
    return "<h1>Distant Reading Archive</h1><p>This site is a prototype API for distant reading of science fiction novels.</p>"

if __name__ == '__main__':
    app.run()
