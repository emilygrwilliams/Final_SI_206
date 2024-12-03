import requests
import sqlite3
import os
import datetime
from bs4 import BeautifulSoup
import re

# url for the api and beautiful soup
calendarific_base_url = "https://calendarific.com/api/v2/holidays"
tmdb_base_url = "https://api.themoviedb.org/3"
tomorrow_io_base_url = "https://api.tomorrow.io/v4/weather/forecast"
marvel_url = 'https://www.marvel.com/comics/characters'

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

# create a list of marvel characters
def marvel_list(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    character_set = set()

    characters = soup.select('li.ListAZ__List_Item a')
    for character in characters:
        name = character.text.strip()
        cleaned_name = re.sub(r'\s*\(.*?\)', '', name)
        character_set.add(cleaned_name)

    character_list = sorted(character_set)
    return(character_list)

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

# filter movies if vote_count > 50 and get release_date and vote_average
def filter_movie_data(movies):
    filtered_movies = []
    for movie in movies:
        if movie.get("vote_count", 0) > 50:
            filtered_movies.append({
                "title": movie["title"],
                "release_date": movie.get("release_date", "N/A"),
                "vote_average": movie.get("vote_average", "N/A"),
                "vote_count": movie.get("vote_count", 0)
            })
    return filtered_movies

# function to save movie data to database
def save_movie_data(movies):
    conn = get_db_connection()
    c = conn.cursor()
    for movie in movies:
        c.execute('''INSERT OR IGNORE INTO movies (title, release_date, popularity, box_office) 
                     VALUES (?, ?, ?, ?)''', 
                  (movie['title'], movie['release_date'], movie['vote_average'], movie['vote_count']))
    conn.commit()
    conn.close()

# function to save holiday data to database
def save_holiday_data(holidays):
    conn = get_db_connection()
    c = conn.cursor()
    for holiday in holidays:
        c.execute('''INSERT INTO holidays (movie_id, holiday_name, date) 
                     VALUES (?, ?, ?)''', 
                  (holiday['movie_id'], holiday['holiday_name'], holiday['date']))
    conn.commit()
    conn.close()

# function to save weather data to database
def save_weather_data(weather, movie_id):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('''INSERT INTO weather (movie_id, temperature, description, wind_speed) 
                 VALUES (?, ?, ?, ?)''',
              (movie_id, weather['temperature'], weather['description'], weather['wind_speed']))
    conn.commit()
    conn.close()

# check if the movie's release date is within 15 days of a holiday (use datetime)
def check_nearby_holiday(movie_release_date):
    pass

# function to add holiday proximity information to movies
def add_holiday_proximity_to_movies():
    pass

# run the functions to collect and store data
if __name__ == "__main__":
    queries = marvel_list(marvel_url)
    all_filtered_movies = []

    for query in queries:
        movies_data = fetch_movie_data(query)
        if 'results' in movies_data:
            filtered_movies = filter_movie_data(movies_data['results'])
            all_filtered_movies.extend(filtered_movies)
            save_movie_data(filtered_movies)
        else:
            print(f"No results found for query: {query}")

    # fetch holiday data and save it

    # add holiday proximity information to movies.db

    # output for verification
    print(f"Saved {len(all_filtered_movies)} movies data.")

    
    # all rows in the movies table
    conn = sqlite3.connect("movies.db")
    c = conn.cursor()
    c.execute("SELECT * FROM movies")
    rows = c.fetchall()
    for row in rows:
        print(row)
    conn.close()

    # all rows from the holidays table
    conn = sqlite3.connect("movies.db")
    c = conn.cursor()
    c.execute("SELECT * FROM weather")
    rows = c.fetchall()
    print("Contents of the weather table:")
    for row in rows:
        print(row)  
    conn.close()

