import requests
import sqlite3
import os
import datetime
from bs4 import BeautifulSoup
import re

# API and URL constants
calendarific_base_url = "https://calendarific.com/api/v2/holidays"
tmdb_base_url = "https://api.themoviedb.org/3"
tomorrow_io_base_url = "https://api.tomorrow.io/v4/weather/timelines"
marvel_url = 'https://www.marvel.com/comics/characters'

# API keys
calendarific_api_key = "BIijlOUbCxMFspLKxsigqETCVsTyvZLe"
tmdb_api_key = "e8b8d5f8e00eb7585b9abbb08ecae48f"
tomorrow_io_api_key = "6VPq9bMwfUqVDOxlCHBIwzNBcIICxEdL"

# Database setup
db_path = "movies.db"
if not os.path.exists(db_path):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    # Create tables
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
                    temperature REAL,
                    precipitation_type TEXT,
                    weather_code TEXT,
                    FOREIGN KEY(city_id) REFERENCES cities(id)
                )''')

    c.execute('''CREATE TABLE holidays (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    holiday_name TEXT,
                    date TEXT
                 )''')

    conn.commit()
    conn.close()

# Get database connection
def get_db_connection():
    return sqlite3.connect(db_path)

# Fetch Marvel characters
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

# Fetch movie data from TMDB
def fetch_movie_data(query):
    url = f"{tmdb_base_url}/search/movie?api_key={tmdb_api_key}&query={query}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching movie data for query '{query}': {e}")
        return {}

# Save movie data to database
def save_movie_data(movies):
    conn = get_db_connection()
    c = conn.cursor()
    for movie in movies:
        c.execute('''INSERT OR IGNORE INTO movies (title, release_date, popularity, box_office) 
                     VALUES (?, ?, ?, ?)''', 
                  (movie['title'], movie['release_date'], movie['vote_average'], movie['vote_count']))
    conn.commit()
    conn.close()

# Fetch weather data from Tomorrow.io
def fetch_weather_data(latitude, longitude):
    url = f"{tomorrow_io_base_url}?apikey={tomorrow_io_api_key}&location={latitude},{longitude}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching weather data: {e}")
        return {}

# Save city data to the database
def save_city_data(city_name, latitude, longitude):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT id FROM cities WHERE location = ?", (city_name,))
    city = c.fetchone()
    if city is None:
        c.execute("INSERT INTO cities (location, latitude, longitude) VALUES (?, ?, ?)", 
                  (city_name, latitude, longitude))
        conn.commit()
        print(f"Saved city: {city_name}")
    conn.close()

# Save weather data to database
def save_weather_data(weather, city_name, latitude, longitude):
    conn = get_db_connection()
    c = conn.cursor()

    # Get or insert city
    c.execute("SELECT id FROM cities WHERE location = ?", (city_name,))
    city = c.fetchone()
    if city is None:
        c.execute("INSERT INTO cities (location, latitude, longitude) VALUES (?, ?, ?)", 
                  (city_name, latitude, longitude))
        city_id = c.lastrowid
    else:
        city_id = city[0]

    # Insert weather data
    c.execute('''INSERT INTO weather (city_id, temperature, precipitation_type, weather_code) 
                 VALUES (?, ?, ?, ?)''',
              (city_id, weather.get('temperature'), weather.get('precipitationType'), weather.get('weatherCode')))
    conn.commit()
    conn.close()

# Fetch holiday data from Calendarific
def fetch_holiday_data(country, year):
    url = f"{calendarific_base_url}?api_key={calendarific_api_key}&country={country}&year={year}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching holiday data: {e}")
        return {}

# Save holiday data to the database
def save_holiday_data(holidays):
    conn = get_db_connection()
    c = conn.cursor()
    for holiday in holidays:
        c.execute('''INSERT INTO holidays (holiday_name, date) 
                     VALUES (?, ?)''', 
                  (holiday['name'], holiday['date']))
    conn.commit()
    conn.close()

# Main function
if __name__ == "__main__":
    # Fetch and save movie data
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

    # Fetch and save city data
    city_examples = [
        {"name": "San Francisco", "latitude": 37.7749, "longitude": -122.4194},
        {"name": "New York", "latitude": 40.7128, "longitude": -74.0060}
    ]
    for city in city_examples:
        save_city_data(city["name"], city["latitude"], city["longitude"])

    # Fetch and save holiday data
    holidays_data = fetch_holiday_data("US", datetime.datetime.now().year)
    if 'response' in holidays_data and 'holidays' in holidays_data['response']:
        save_holiday_data(holidays_data['response']['holidays'])

    # Fetch and save weather data
    for city in city_examples:
        weather_data = fetch_weather_data(city["latitude"], city["longitude"])
       
