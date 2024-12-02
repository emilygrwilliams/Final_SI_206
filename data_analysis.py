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
    conn = get_db_connection()

    # join data from movies, weather, and holidays
    query = '''
        SELECT 
            m.title, 
            m.box_office, 
            m.release_date, 
            w.temperature, 
            w.description AS weather_description, 
            h.holiday_name
        FROM movies m
        LEFT JOIN weather w ON m.id = w.movie_id
        LEFT JOIN holidays h ON m.id = h.movie_id
    '''
    
    df = pd.read_sql_query(query, conn)
    conn.close()

    # add holiday_present column
    df['holiday_present'] = df['holiday_name'].notnull()
    
    # save report to csv
    df.to_csv('movies_report.csv', index=False)
    print("Report saved to movies_report.csv")
    
    return df

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
