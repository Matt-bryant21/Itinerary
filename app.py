import psycopg2
from psycopg2 import sql
from tabulate import tabulate
from datetime import datetime
import pytz

# Replace these with your PostgreSQL connection details
dbname = "usa"
user = "matt"
password = "1234"
host = "localhost"

# Connection to PostgreSQL
conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host)
cursor = conn.cursor()

# Drop the table if it exists
drop_table_query = sql.SQL("DROP TABLE IF EXISTS {table_name};").format(
    table_name=sql.Identifier("vacay")
)
cursor.execute(drop_table_query)
conn.commit()

# Create the "vacay" table
create_table_query = """
CREATE TABLE "vacay" (
    "Day" TEXT,
    "Wollongong Time" TIMESTAMPTZ,
    "LA Time" TIMESTAMPTZ,
    "NY/Orlando Time" TIMESTAMPTZ,
    "Activity" VARCHAR(255)
);
"""
cursor.execute(create_table_query)
conn.commit()

# Data to be inserted
values = [
    ('Monday', '2023-11-20 11:10 AM', 'Qantas Flight QF11 Leaves Sydney'),
    ('Tuesday', '2023-11-21 6:00 AM', 'Qantas Flight QF11 Arrives Los Angeles'),
]

# Insert data into the "vacay" table with manual Wollongong Time entry
insert_data_query = """
INSERT INTO "vacay" ("Day", "Wollongong Time", "Activity")
VALUES (%s, %s::timestamptz, %s)
RETURNING "Wollongong Time";  -- Return the inserted Wollongong Time
"""

# Execute the insert statement for each row
for row in values:
    day, _, activity = row
    wollongong_time_str = input(f"Enter Wollongong Time for {day} in the format 'YYYY-MM-DD HH:MM AM/PM': ")
    cursor.execute(insert_data_query, (day, wollongong_time_str, activity))
    inserted_wollongong_time = cursor.fetchone()[0]

    # Update LA Time and NY/Orlando Time based on the manually entered Wollongong Time
    update_query = """
    UPDATE "vacay"
    SET
        "LA Time" = %s::timestamptz AT TIME ZONE 'America/Los_Angeles',
        "NY/Orlando Time" = %s::timestamptz AT TIME ZONE 'America/New_York'
    WHERE "Wollongong Time" = %s::timestamptz;
    """
    cursor.execute(update_query, (inserted_wollongong_time, inserted_wollongong_time, inserted_wollongong_time))

conn.commit()

# Select all data from the "vacay" table
select_query = "SELECT * FROM vacay"
cursor.execute(select_query)
rows = cursor.fetchall()

# Display the data using tabulate
headers = [desc[0] for desc in cursor.description]
table = tabulate(rows, headers=headers, tablefmt="psql")
print(table)

# Close the connection
cursor.close()
conn.close()
