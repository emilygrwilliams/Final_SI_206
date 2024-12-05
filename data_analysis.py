import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import matplotlib.dates as mdates #help from AI

# connect to the database
def get_db_connection():
    return sqlite3.connect("movies.db")

# function to read and prepare data
def analyze_data():
    conn = get_db_connection()
    
    query = '''
    SELECT 
        m.title AS movie_title,
        m.release_date, 
        m.popularity, 
        m.box_office, 
        c.location AS city_name, 
        w.date AS weather_date, 
        w.temperature, 
        w.precipitation,
        h.holiday_name, 
        h.date AS holiday_date,
        ABS(julianday(m.release_date) - julianday(h.date)) AS days_difference
    FROM 
        movies m
    LEFT JOIN 
        holidays h ON ABS(julianday(m.release_date) - julianday(h.date)) <= 5
    LEFT JOIN 
        weather w ON julianday(m.release_date) = julianday(w.date)
    LEFT JOIN 
        cities c ON c.id = w.city_id
    ORDER BY 
        m.release_date;
    '''
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

# function to create a bar plot showing holiday nearby vs average popularity
def create_bar_plot(df):
    df['holiday_nearby'] = df['days_difference'].notna()
    avg_popularity = df.groupby('holiday_nearby')['popularity'].mean().reset_index() #help from AI
    avg_popularity['holiday_nearby'] = avg_popularity['holiday_nearby'].replace({True: 'Holiday Nearby', False: 'No Holiday Nearby'}) #help from AI
    
    plt.figure(figsize=(8, 6))
    sns.barplot(data=avg_popularity, x='holiday_nearby', y='popularity')
    plt.title('Average Popularity Based on Nearby Holidays')
    plt.xlabel('Holiday Proximity')
    plt.ylabel('Average Popularity')
    plt.tight_layout()
    plt.show()

# function to create a scatter plot of temp vs popularity
def create_scatter_plot(df):
    plt.figure(figsize=(8, 5))
    sns.scatterplot(data=df, x='temperature', y='popularity', hue='precipitation', palette='coolwarm')
    plt.title('Temperature vs Movie Popularity')
    plt.xlabel('Temperature (°C)')
    plt.ylabel('Popularity')
    plt.legend(title='Precipitation')
    plt.show()

# function to create a line graph comparing temperatures in both cities
def create_line_graph(df):
    df['weather_date'] = pd.to_datetime(df['weather_date'])

    plt.figure(figsize=(12, 6))
    sns.lineplot(data=df, x='weather_date', y='temperature', hue='city_name')
    plt.title('Temperature Trends in Cities')
    plt.xlabel('Month')
    plt.ylabel('Temperature (°C)')
    plt.xlim(pd.Timestamp('2021-01-01'), pd.Timestamp('2023-12-31')) #help from AI
    plt.gca().xaxis.set_major_locator(mdates.MonthLocator()) #help from AI
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%b %Y')) #help from AI
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()


# function to create a pie chart comparing vote counts by temperature
def create_pie_chart(df):
    df['temp_category'] = df['temperature'].apply(lambda x: '≤ 15°C' if x <= 15 else '> 15°C') #help from AI
    
    temp_votes = df.groupby('temp_category')['popularity'].sum() #help from AI
    
    plt.figure(figsize=(8, 8))
    plt.pie(temp_votes, labels=temp_votes.index, autopct='%1.1f%%', startangle=140, colors=['skyblue', 'orange']) #help from AI
    plt.title('Popularity Based on Temperature Category')
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    df = analyze_data()
    
    create_bar_plot(df)
    create_scatter_plot(df)
    create_line_graph(df)
    create_pie_chart(df)
