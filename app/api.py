from flask import Flask
from flask import request, jsonify
from db import create_weather,get_weather_report_id,update_weather, \
    get_weather_report_average_sensor_id,get_weather_report_average_tempture,get_weather_report_all,\
    create_weather_reports_batch   

api = Flask(__name__)
api.config["DEBUG"] = True


@api.route('/api/v1/weather/sensor/create', methods=['POST'])
def api_weather_create():
    request_json = request.get_json(silent=True)
    try:
        if request_json and 'ID' in request_json and  isinstance(request_json['ID'], int) and request_json and 'sensor_id' in request_json \
        and isinstance(request_json['sensor_id'], int):
                weather = create_weather(request_json["ID"],request_json["sensor_id"],request_json["temperature"],request_json["humidity"],
                request_json["city"],request_json["country"],request_json["air_pollution"])
                return jsonify(weather)
        else:
                raise ValueError("JSON is invalid, or missing a property")
    except ValueError as ValErr:
        print(ValErr)
        return jsonify(dict(Status="Error", Message="Missing dependencies in the json body")), 400

    

@api.route('/api/v1/weather/sensor/id', methods=['GET'])
def api_get_weather_sensor():
    request_json = request.get_json(silent=True)
    try:
        if request_json and 'ID' in request_json and isinstance(request_json['ID'], int):
            weather = get_weather_report_id(request_json["ID"])
            return jsonify(weather)
        else:
            raise ValueError("JSON is invalid, or missing a property ID")
    except ValueError as ValErr:
        print(ValErr)
        return jsonify(dict(Status="Error", Message="Missing dependencies in the json body")), 400
    

@api.route('/api/v1/weather/sensor/update', methods=['PUT'])
def api_update_add_metric():
    request_json = request.get_json(silent=True)
    try:
        if request_json and 'ID' in request_json and isinstance(request_json['ID'], int):
                weather = update_weather(request_json["ID"],request_json["new_metric"],request_json["value"])
                return jsonify(weather)
        else:
            raise ValueError("JSON is invalid, or missing a property")
    except ValueError as ValErr:
        print(ValErr)
        return jsonify(dict(Status="Error", Message="Missing dependencies in the json body")), 400


@api.route('/api/v1/weather/sensor/all', methods=['GET'])
def api_get_weather_report_all():
    weather = get_weather_report_all()
    return jsonify(weather)


@api.route('/api/v1/weather/sensor/batch', methods=['POST'])
def api_batch_weather_report_create():
    weather = create_weather_reports_batch()
    return jsonify(weather)


@api.route('/api/v1/weather/sensor/average/city', methods=['GET'])
def api_get_weather_city():
    request_json = request.get_json(silent=True)
    try:
        if request_json and 'city' in request_json and isinstance(request_json['city'], str):
            weather = get_weather_report_average_tempture(request_json["city"])
            return jsonify(dict(Average_Tempature=weather[1], Average_Humitdiy=weather[0]))
        else:
            raise ValueError("JSON is invalid, or missing a property")
    except ValueError as ValErr:
        print(ValErr)
        return jsonify(dict(Status="Error", Message="Missing dependencies in the json body")), 400


@api.route('/api/v1/weather/sensor/average/id', methods=['GET'])
def api_get_weather_sensor_by_id():
    request_json = request.get_json(silent=True)
    try:
        if request_json and 'sensor_id' in request_json and isinstance(request_json['sensor_id'], int):
            if request_json and 'start_date' in request_json and isinstance(request_json['start_date'], str) and request_json and 'end_date' in request_json \
                and isinstance(request_json['end_date'], str):
                weather = get_weather_report_average_sensor_id(request_json["sensor_id"],request_json["start_date"],request_json["end_date"])
                return jsonify(dict(Average_Tempature=weather[1], Average_Humitdiy=weather[0]))
            else:
                weather = get_weather_report_average_sensor_id(request_json["sensor_id"],"","")
                return jsonify(dict(Average_Tempature=weather[1], Average_Humitdiy=weather[0]))
        else:
            raise ValueError("JSON is invalid, or missing a property")
    except ValueError as ValErr:
        print(ValErr)
        return jsonify(dict(Status="Error", Message="Missing dependencies in the json body")), 400
    
@api.errorhandler(404)
def page_not_found(e):
    return "<h1>404</h1><p>The resource could not be found.</p>", 404

@api.errorhandler(500)
def internal_server_error(e):
    return "<h1>500</h1><p>Internal Server Error</p>", 500

if __name__ == '__main__':
    api.run(debug=True, host='0.0.0.0', port=5000)