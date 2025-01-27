from flask import Flask, request, jsonify
import sqlite3
import pandas as pd

app = Flask(__name__)

# Database query function
def query_database(query):
    conn = sqlite3.connect('delhi_weather.db')
    result = pd.read_sql_query(query, conn)
    conn.close()
    return result

# Endpoint to retrieve delhi weather details for a specific month and year
@app.route('/delhi_weather', methods=['GET'])
def get_weather():
    month = request.args.get('month')  # Get month from query params
    year = request.args.get('year')  # Get year from query params
    weather_parameter = request.args.get('weather_parameter')  # Get weather_parameter from query params (like: temperature, humidity)

    if not month or not year or not weather_parameter:
        return jsonify({"error": "Missing required query parameters: month, year, or weather_parameter"}), 400

    try:
        query = f"""
        SELECT datetime_utc, {weather_parameter} 
        FROM delhi_weather 
        WHERE month = {month} AND year = {year}
        """
        data = query_database(query)
        return jsonify(data.to_dict(orient='records'))  # Return the result as a JSON list
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Endpoint to retrieve high, median, and low temperature stats for a given year
@app.route('/delhi_weather/stats', methods=['GET'])
def get_stats():
    year = request.args.get('year')  # Get year from query params

    if not year:
        return jsonify({"error": "Missing required query parameter: year"}), 400

    try:
        query = f"""
        SELECT month, 
               MAX(_tempm) AS high, 
               AVG(_tempm) AS median, 
               MIN(_tempm) AS low
        FROM delhi_weather 
        WHERE year = {year}
        GROUP BY month
        """
        data = query_database(query)
        return jsonify(data.to_dict(orient='records'))  # Return the result as a JSON list
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
