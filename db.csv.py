import pandas as pd
import sqlite3

# Step 1: Loading the dataset
print("Step 1: Loading the dataset\n")
file_path = 'C:\\Users\\surendran.m\\Downloads\\testset (1).csv'
delhi_weather_data = pd.read_csv(file_path)

# Step 2: Cleaning the column names by cleaning or striping extra spaces
print("Step 2: Cleaning the column names by stripping extra spaces\n")
delhi_weather_data.columns = delhi_weather_data.columns.str.strip()

# Step 3: Removing unnecessary columns with entirely missing data or can be told as irrelevant
print("Step 3: Removing unnecessary columns with entirely missing or irrelevant data\n")
processed_data = delhi_weather_data.drop(columns=['_precipm', '_wgustm', '_windchillm'], errors='ignore')

# Step 4: Handling missing values by filling them with mean values (for numeric columns)
print("Step 4: Handling missing values by filling them with mean values (for numeric columns)\n")
processed_data['_tempm'].fillna(processed_data['_tempm'].mean(), inplace=True)
processed_data['_hum'].fillna(processed_data['_hum'].mean(), inplace=True)
processed_data['_pressurem'].fillna(processed_data['_pressurem'].mean(), inplace=True)
processed_data['_heatindexm'].fillna(processed_data['_heatindexm'].mean(), inplace=True)

# Step 5: Converting the `datetime_utc` to datetime format and extract year and month
print("Step 5: Converting the `datetime_utc` to datetime format and extract year and month\n")
processed_data['datetime_utc'] = pd.to_datetime(processed_data['datetime_utc'], format='%Y%m%d-%H:%M', errors='coerce')
processed_data['year'] = processed_data['datetime_utc'].dt.year
processed_data['month'] = processed_data['datetime_utc'].dt.month

# Step 6: Droping rows with invalid date and time conversion
print("Step 6: Droping rows with invalid datetime conversion\n")
processed_data.dropna(subset=['datetime_utc'], inplace=True)


# Step 7: Saving the cleaned data to SQLite database
print("Step 7: Saving the cleaned data to SQLite database\n")
def save_to_database(data, db_name='delhi_weather.db'):
    """
    Save a DataFrame to a SQLite database.

    Args:
        data (pd.DataFrame): The DataFrame to save.
        db_name (str): The name of the SQLite database file.
    """
    conext_to_sql = sqlite3.connect(db_name)  # Create/connect to SQLite database
    data.to_sql('delhi_weather', conext_to_sql, if_exists='replace', index=False)  # Saving DataFrame to the `delhi_weather` table
    conext_to_sql.close()  # Closing the connection

# Main Code Line
save_to_database(processed_data)

# Verifing and confirm the ddata
print("Data has been successfully saved into 'delhi_weather.db'.")
