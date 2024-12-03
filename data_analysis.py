import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

# connect to the database
def get_db_connection():
    return sqlite3.connect("movies.db")

# function to read and prepare data
def analyze_data():
    pass

# function to find movies near holidays
def movies_near_holidays():
    conn = sqlite3.connect("movies.db")
    c = conn.cursor()
    
    query = '''
    SELECT 
        m.title, 
        m.release_date, 
        h.holiday_name, 
        h.date AS holiday_date,
        ABS(julianday(m.release_date) - julianday(h.date)) AS days_difference
    FROM 
        movies m
    JOIN 
        holidays h
    ON 
        ABS(julianday(m.release_date) - julianday(h.date)) <= 30
    ORDER BY 
        m.release_date, days_difference;
    '''
    
    c.execute(query)
    results = c.fetchall()
    
    #needs to be changed
    for row in results:
        print(f"Movie: {row[0]} | Release Date: {row[1]} | Holiday: {row[2]} | Holiday Date: {row[3]} | Days Difference: {row[4]}")
    
    conn.close()

# function to create a bar plot
def create_bar_plot(df):
    pass

# function to create a scatter plot
def create_scatter_plot(df):
    pass

# function to create a choropleth map placeholder
def create_choropleth_map():
    pass

if __name__ == "__main__":
    df = analyze_data()
    create_bar_plot(df)
    create_scatter_plot(df)
    create_choropleth_map()
