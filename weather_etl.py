import pandas as pd
import requests
import sqlite3

# Declare df in the global scope
df = pd.DataFrame()

def extract():
    global df
    api_key = '28cb89ad9a833b215e469da7075e47a4'
    url = "https://history.openweathermap.org/data/2.5/aggregated/year?id=633679&appid=" + api_key

    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()['result']

        # Extract relevant features
        columns = ['dt',
                   'temp_median', 'temp_min', 'temp_max',
                   'humidity_median', 'humidity_min', 'humidity_max',
                   'precipitation_median', 'precipitation_min', 'precipitation_max',
                   'wind_speed_median', 'wind_speed_min', 'wind_speed_max']

        # Create a list to hold individual dataframes
        dfs = []

        for entry in data:
            # Check for leap year and handle February 29
            year = 2022  # Assuming data is from the year 2022
            month = entry['month']
            day = entry['day']
            if month == 2 and day == 29 and not pd.to_datetime(f"{year}-01-01").is_leap_year:
                continue  # Skip February 29 if not a leap year

            date = pd.to_datetime(f"{year}-{month}-{day}", errors='coerce')
            if pd.isnull(date):
                continue  # Skip invalid dates

            temp = entry['temp']
            humidity = entry['humidity']
            precipitation = entry['precipitation']
            wind_speed = entry['wind']

            row = {
                'dt': date,
                'temp_median': temp['median'],
                'temp_min': temp['record_min'],
                'temp_max': temp['record_max'],
                'humidity_median': humidity['median'],
                'humidity_min': humidity['min'],
                'humidity_max': humidity['max'],
                'precipitation_median': precipitation['median'],
                'precipitation_min': precipitation['min'],
                'precipitation_max': precipitation['max'],
                'wind_speed_median': wind_speed['median'],
                'wind_speed_min': wind_speed['min'],
                'wind_speed_max': wind_speed['max'],
            }

            dfs.append(pd.DataFrame([row]))

        # Concatenate the list of dataframes
        df = pd.concat(dfs, ignore_index=True)

        print("Historical data extracted and saved successfully.")
    else:
        print(f"Error: Unable to fetch data. Status code: {response.status_code}")

def transform():
    
    global df 
    
    if df is not None and not df.empty:
        
        # Delete rows with missing data
        df = df.dropna()

        # Delete duplicates
        df = df.drop_duplicates()

        # Extract month from 'dt' to create 'dt_month' column
        df['dt_month'] = df['dt'].dt.to_period("M")

        # Calculate monthly averages for temperature, humidity, and precipitation
        monthly_averages = df.groupby('dt_month').agg({
            'temp_median': 'mean',
            'humidity_median': 'mean',
            'precipitation_median': 'mean'
        }).reset_index()

        monthly_averages = monthly_averages.rename(columns={
            'temp_median': 'monthly_temp_avg',
            'humidity_median': 'monthly_humidity_avg',
            'precipitation_median': 'monthly_precipitation_avg'
        })

        # Merge with the original data
        df = pd.merge(df, monthly_averages, on='dt_month', how='left')

        # Add 'precipitation_type' column based on precipitation_max
        df['precipitation_type'] = pd.cut(
            df['precipitation_max'],
            bins=[-float('inf'), 1, 5, float('inf')],
            labels=['light', 'medium', 'heavy']
        )

        # Convert 'precipitation_type' to categorical
        df['precipitation_type'] = pd.Categorical(df['precipitation_type'], categories=['light', 'medium', 'heavy'],
                                                  ordered=True)

        # Calculate monthly mode for precipitation type
        monthly_mode = df.groupby('dt_month')['precipitation_type'].apply(
            lambda x: x.mode().iloc[0] if not x.mode().empty else None).reset_index()
        monthly_mode = monthly_mode.rename(columns={'precipitation_type': 'precipitation_monthly_mode'})

        # Merge precipitation mode calculations with the original data
        df = pd.merge(df, monthly_mode, on='dt_month', how='left')

        # Drop the extra 'dt_month' column used for merging
        df = df.drop(columns=['dt_month'])

        # Categorize wind strength based on wind speed
        df['wind_strength'] = pd.cut(
            df['wind_speed_max'],
            bins=[-float('inf'), 1.5, 3.3, 5.4, 7.9, 10.7, 13.8, 17.1, 20.7, 24.4, 28.4, 32.6, float('inf')],
            labels=['Calm', 'Light Air', 'Light Breeze', 'Gentle Breeze', 'Moderate Breeze', 'Fresh Breeze',
                    'Strong Breeze', 'Near Gale', 'Gale', 'Strong Gale', 'Storm', 'Violent Storm'],
            right=False  # Set to True if you want intervals to be right-closed
        )

        # Convert 'wind_strength' to categorical
        df['wind_strength'] = pd.Categorical(df['wind_strength'],
                                             categories=['Calm', 'Light Air', 'Light Breeze', 'Gentle Breeze',
                                                         'Moderate Breeze', 'Fresh Breeze', 'Strong Breeze',
                                                         'Near Gale', 'Gale', 'Strong Gale', 'Storm',
                                                         'Violent Storm'],
                                             ordered=True)
        
        df.to_csv('transformed_weatherdata.csv', index=False)

    else:
        print("Error: No data to transform.")

def load_to_sqlite():

    conn = sqlite3.connect('weatherdata.db')
    data_transformed = pd.read_csv('transformed_weatherdata.csv')
    data_transformed.to_sql('historical_weather_data', conn, if_exists='replace', index=False)

extract()
transform()
load_to_sqlite()
