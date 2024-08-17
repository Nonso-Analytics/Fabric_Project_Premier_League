# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "01bb8d39-906a-4275-b1ca-c4029a3b9306",
# META       "default_lakehouse_name": "LH_Tutorials",
# META       "default_lakehouse_workspace_id": "deb7aba3-4bff-49b5-b8e1-ee5005d6dde2",
# META       "known_lakehouses": [
# META         {
# META           "id": "01bb8d39-906a-4275-b1ca-c4029a3b9306"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### **Webscraping Premier League Data from FBREF**

# CELL ********************

import requests
from bs4 import BeautifulSoup
import pandas as pd

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

url = 'https://fbref.com/en/comps/9/2023-2024/2023-2024-Premier-League-Stats'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

data = requests.get(url)
data.text

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

soup = BeautifulSoup(data.text)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

standings_table = soup.select('table.stats_table')[0]
standings_table

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Extract table headers
headers = [th.get_text() for th in standings_table.find_all('th')]

# Extract table rows
rows = []
for tr in standings_table.find_all('tr'):
    cells = tr.find_all('td')
    cells = [cell.get_text(strip=True) for cell in cells]
    if cells:  # Only append rows that are not empty
        rows.append(cells)

# Convert to DataFrame
df = pd.DataFrame(rows, columns=headers[21:]) 
# Display the DataFrame
print(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Assuming 'df' is your DataFrame
new_headers = [
    'Squad', 'MP', 'W', 'D', 'L', 'GF', 'GA', 'GD', 
    'Pts', 'Pts/MP', 'xG', 'xGA', 'xGD', 'xGD/90', 
    'Attendance', 'Top Team Scorer', 'Goalkeeper', 'Notes'
]

# Rename the columns
df.columns = new_headers

# Rename the index and start it from 1
df.index = df.index + 1

# Display the updated DataFrame
print(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Remove spaces and special characters from column names
df.columns = df.columns.str.replace(' ', '_').str.replace('[^0-9a-zA-Z_]', '')

# Convert the Pandas DataFrame to a PySpark DataFrame
spark_df = spark.createDataFrame(df)

# Save the DataFrame as a Delta table
table_name = "premier_league_standings_2023_to_2024"
spark_df.write.mode("overwrite").format("delta").saveAsTable(table_name)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Getting Logo URL Data

# CELL ********************

# Initialize lists to hold the data
clubs = []
logos = []

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Find all the relevant rows (tr tags) in the table
for row in standings_table.find_all('tr'):
    # Find the club name and logo URL
    club = row.find('a')  # This might be within an <a> tag
    logo = row.find('img')  # The logo might be in an <img> tag

    if club and logo:
        clubs.append(club.text.strip())
        logos.append(logo['src'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create the DataFrame
df = pd.DataFrame({
    'Squad': clubs,
    'Logo URL': ['https:' + url if url.startswith('//') else url for url in logos]
})

# Ensure full URLs are displayed
pd.set_option('display.max_colwidth', None)

# Display the DataFrame
print(df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Remove spaces and special characters from column names
df.columns = df.columns.str.replace(' ', '_').str.replace('[^0-9a-zA-Z_]', '')

# Convert the Pandas DataFrame to a PySpark DataFrame
spark_df = spark.createDataFrame(df)

# Save the DataFrame as a Delta table
table_name = "premier_league_standings_2023_to_2024_logo_url"
spark_df.write.mode("overwrite").format("delta").saveAsTable(table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
