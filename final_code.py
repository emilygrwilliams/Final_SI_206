# Avery Schiff, Keely Nykerk, Emily Williams

import requests
import sqlite3
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

# url for the api
calendarific_base_url = "https://calendarific.com/api/v2/holidays"
tmdb_base_url = "https://api.themoviedb.org/3"
tomorrow_io_base_url = "https://api.tomorrow.io/v4/weather/forecast"

# api Keys
calendarific_api_key = "BIijlOUbCxMFspLKxsigqETCVsTyvZLe"
tmdb_api_key = "e8b8d5f8e00eb7585b9abbb08ecae48f"
tomorrow_io_api_key = "6VPq9bMwfUqVDOxlCHBIwzNBcIICxEdL"

# database setup
db_path = "movies.db"

if not os.path.exists(db_path):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    # create tables
    c.execute('''CREATE TABLE movies (
                    id INTEGER PRIMARY KEY,
                    title TEXT UNIQUE,
                    release_date TEXT,
                    popularity REAL,
                    box_office REAL
                 )''')

    c.execute('''CREATE TABLE weather (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    movie_id INTEGER,
                    temperature REAL,
                    description TEXT,
                    wind_speed REAL,
                    FOREIGN KEY(movie_id) REFERENCES movies(id)
                 )''')

    c.execute('''CREATE TABLE holidays (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    movie_id INTEGER,
                    holiday_name TEXT,
                    date TEXT,
                    FOREIGN KEY(movie_id) REFERENCES movies(id)
                 )''')

    conn.commit()
    conn.close()

# connect to the database
def get_db_connection():
    return sqlite3.connect(db_path)

# fetch movie data from TMDB
def fetch_movie_data(query):
    url = f"{tmdb_base_url}/search/movie?api_key={tmdb_api_key}&query={query}"
    response = requests.get(url)
    return response.json()

# fetch weather data from Tomorrow.io
def fetch_weather_data(latitude, longitude):
    url = f"{tomorrow_io_base_url}?location={latitude},{longitude}&apikey={tomorrow_io_api_key}"
    response = requests.get(url)
    return response.json()

# fetch holiday data from Calendarific
def fetch_holiday_data(country, year):
    url = f"{calendarific_base_url}?api_key={calendarific_api_key}&country={country}&year={year}"
    response = requests.get(url)
    return response.json()

# ex usage
#movie_data = fetch_movie_data("Inception")
#weather_data = fetch_weather_data(42.3478, -71.0466)  # ex coordinates
#holiday_data = fetch_holiday_data("US", 2024)

#print("Movie Data:", movie_data)
#print("Weather Data:", weather_data)
#print("Holiday Data:", holiday_data)

# filter movies if vote_count > 100 and get release_date and vote_average
def filter_movie_data(movies):
    filtered_movies = []
    for movie in movies:
        if movie.get("vote_count", 0) > 100:
            filtered_movies.append({
                "title": movie["title"],
                "release_date": movie.get("release_date", "N/A"),
                "vote_average": movie.get("vote_average", "N/A"),
                "vote_count": movie.get("vote_count", 0)
            })
    return filtered_movies

# ex usage
#query = "wizard"
#movie_data = fetch_movie_data(query)

# filter movies
#filtered_movie_data = filter_movie_data(movie_data.get("results", []))

# display filtered movie data
#for movie in filtered_movie_data:
#    print(f"Title: {movie['title']}")
#    print(f"Release Date: {movie['release_date']}")
#    print(f"Vote Average: {movie['vote_average']}")
#    print(f"Vote Count: {movie['vote_count']}")
#    print("-" * 30)

# preprocess weather data to categorize conditions
def preprocess_weather_data(weather_data):
    pass

# generate choropleth-like map for weather conditions by region using matplotlib
def generate_weather_map(region_coordinates, weather_data):
    pass

# generate bar graph comparing box office revenue with nearby holidays
def generate_bar_graph(box_office_data, holiday_data):
    pass

# generate scatter plot correlating box office revenue with weather
def generate_scatter_plot(movie_data, weather_data, holiday_data):
    pass


# example weather and holiday data for visualization

# preprocess weather data to categorize conditions


# generate the choropleth

# generate the bar graph comparing box office revenue with nearby holidays

# generate the scatter plot correlating box office revenue with weather
