#pip install meteostat
import requests
import sqlite3
import os
import datetime
from bs4 import BeautifulSoup
import re
from meteostat import Point, Daily
import pandas as pd

# url and api keys
calendarific_base_url = "https://calendarific.com/api/v2/holidays"
tmdb_base_url = "https://api.themoviedb.org/3"
marvel_url = 'https://www.marvel.com/comics/characters'

calendarific_api_key = "BIijlOUbCxMFspLKxsigqETCVsTyvZLe"
tmdb_api_key = "e8b8d5f8e00eb7585b9abbb08ecae48f"

# db setup
db_path = "movies.db"
if not os.path.exists(db_path):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    # create tables
    c.execute('''CREATE TABLE movies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT UNIQUE,
                    release_date TEXT,
                    popularity REAL,
                    box_office REAL
                 )''')

    c.execute('''CREATE TABLE cities (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    location TEXT NOT NULL,
                    latitude REAL,
                    longitude REAL
                )''')

    c.execute('''CREATE TABLE weather (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    city_id INTEGER,
                    date TEXT,
                    temperature REAL,
                    precipitation REAL,
                    FOREIGN KEY(city_id) REFERENCES cities(id)
                )''')

    c.execute('''CREATE TABLE holidays (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    holiday_name TEXT,
                    date TEXT
                 )''')

    conn.commit()
    conn.close()

# database connection
def get_db_connection():
    return sqlite3.connect(db_path)

# fetch Marvel characters
def marvel_list(url):
    response = requests.get(url)
    if response.status_code != 200:
        print("Failed to fetch Marvel characters.")
        return []

    soup = BeautifulSoup(response.text, 'html.parser')
    character_set = set()
    characters = soup.select('li.ListAZ__List_Item a')
    for character in characters:
        name = character.text.strip()
        cleaned_name = re.sub(r'\s*\(.*?\)', '', name)
        character_set.add(cleaned_name)

    return sorted(character_set)

# fetch movie data from TMDB
def fetch_movie_data(query):
    url = f"{tmdb_base_url}/search/movie?api_key={tmdb_api_key}&query={query}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e: #help from AI
        print(f"Error fetching movie data for query '{query}': {e}")
        return {}

# save movie data to database
def save_movie_data(movies):
    conn = get_db_connection()
    c = conn.cursor()
    for movie in movies:
        release_year = movie['release_date'][:4] if movie['release_date'] != "N/A" else None #help from AI
        if release_year == '2023' or release_year == '2024' and movie['vote_count'] > 100:  # only save 2023 and 2024 movies with vote count > 100
            c.execute('''INSERT OR IGNORE INTO movies (title, release_date, popularity, box_office) 
                         VALUES (?, ?, ?, ?)''', 
                      (movie['title'], movie['release_date'], movie['vote_average'], movie['vote_count']))
    conn.commit()
    conn.close()


# fetch holiday data from Calendarific
def fetch_holiday_data(country, year):
    url = f"{calendarific_base_url}?api_key={calendarific_api_key}&country={country}&year={year}&type=national,local"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e: #help from AI
        print(f"Error fetching holiday data: {e}")
        return {}

# save holiday data to the database
def save_holiday_data(holidays):
    conn = get_db_connection()
    c = conn.cursor()

    for holiday in holidays:
        holiday_name = holiday.get('name', 'Unknown')
        holiday_date = holiday.get('date', {}).get('iso', None)
        if holiday_date:
            c.execute('''SELECT id FROM holidays WHERE holiday_name = ? AND date = ?''', 
                      (holiday_name, holiday_date))
            existing = c.fetchone()
            if not existing:
                c.execute('''INSERT INTO holidays (holiday_name, date) 
                             VALUES (?, ?)''', 
                          (holiday_name, holiday_date))
    conn.commit()
    conn.close()


# save city data to the database
def save_city_data(city_name, latitude, longitude):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT id FROM cities WHERE location = ?", (city_name,))
    city = c.fetchone()
    if city is None:
        c.execute("INSERT INTO cities (location, latitude, longitude) VALUES (?, ?, ?)", 
                  (city_name, latitude, longitude))
        conn.commit()
    conn.close()

# fetch historical weather data using Meteostat
def fetch_weather_data(latitude, longitude, start_date, end_date):
    location = Point(latitude, longitude)
    data = Daily(location, start_date, end_date)
    df = data.fetch()

    return df.reset_index()

# save weather data to database
def save_weather_data(weather_data, city_name):
    conn = get_db_connection()
    c = conn.cursor()

    c.execute("SELECT id FROM cities WHERE location = ?", (city_name,))
    city = c.fetchone()
    if city is None:
        raise ValueError(f"City {city_name} not found in database.")
    city_id = city[0]

    for _, row in weather_data.iterrows():
        date_str = row['time'].strftime('%Y-%m-%d')
        temperature = row['tavg'] 
        precipitation = row.get('prcp', 0)

        c.execute('''INSERT INTO weather (city_id, date, temperature, precipitation) 
                     VALUES (?, ?, ?, ?)''',
                  (city_id, date_str, temperature, precipitation))

    conn.commit()
    conn.close()


# main function
if __name__ == "__main__":
    # movie data
    queries = marvel_list(marvel_url)
    all_filtered_movies = []

    for query in queries:
        movies_data = fetch_movie_data(query)
        if 'results' in movies_data:
            filtered_movies = [
                {
                    "title": movie["title"],
                    "release_date": movie.get("release_date", "N/A"),
                    "vote_average": movie.get("vote_average", 0),
                    "vote_count": movie.get("vote_count", 0)
                } 
                for movie in movies_data['results'] if movie.get("vote_count", 0) > 50
            ]
            all_filtered_movies.extend(filtered_movies)
            save_movie_data(filtered_movies)

    # city data
    city_examples = [
        {"name": "San Francisco", "latitude": 37.7749, "longitude": -122.4194},
        {"name": "New York", "latitude": 40.7128, "longitude": -74.0060}
    ]
    for city in city_examples:
        save_city_data(city["name"], city["latitude"], city["longitude"])

    # holiday data
    holidays_data = fetch_holiday_data("US", datetime.datetime.now().year)
    if 'response' in holidays_data and 'holidays' in holidays_data['response']:
        holidays = holidays_data['response']['holidays']
        save_holiday_data(holidays)

    # weather data
    start_date = datetime.datetime(2023, 1, 1)
    end_date = datetime.datetime(2024, 12, 31)

    for city in city_examples:
        df_weather = fetch_weather_data(city["latitude"], city["longitude"], start_date, end_date)
        save_weather_data(df_weather, city["name"])
