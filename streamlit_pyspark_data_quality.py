import streamlit as st
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, date_sub
import pandas as pd

# Initialize SparkSession with Hive support
spark = SparkSession.builder \
    .appName("HiveTableCounts") \
    .enableHiveSupport() \
    .getOrCreate()

# Streamlit UI
st.title("Hive Table Record Counts for the Past 5 Days")
st.write("Select Hive tables to visualize their record counts over the past 5 days.")

# Input: Hive Database Name
database_name = st.text_input("Enter Hive Database Name:", value="default")

if database_name:
    # Fetch table names from the specified Hive database
    try:
        tables = spark.sql(f"SHOW TABLES IN {database_name}").select("tableName").rdd.flatMap(lambda x: x).collect()

        if tables:
            # Multi-select for choosing tables
            selected_tables = st.multiselect("Select tables to analyze:", tables)

            if selected_tables:
                data = []

                # Calculate record counts for each table for the past 5 days
                for table in selected_tables:
                    query = f"""
                        SELECT COUNT(*) AS count, CAST(event_date AS DATE) AS date
                        FROM {database_name}.{table}
                        WHERE event_date >= DATE_SUB(CURRENT_DATE(), 5)
                        GROUP BY event_date
                        ORDER BY event_date
                    """
                    table_data = spark.sql(query).collect()

                    for row in table_data:
                        data.append({"Table Name": table, "Date": row["date"], "Record Count": row["count"]})

                # Convert data to a Pandas DataFrame
                if data:
                    df = pd.DataFrame(data)

                    # Display DataFrame in Streamlit
                    st.write("### Record Counts for the Past 5 Days:")
                    st.dataframe(df)

                    # Graphical Representation
                    st.write("### Record Counts Line Chart:")
                    chart_data = df.pivot(index="Date", columns="Table Name", values="Record Count").fillna(0)
                    st.line_chart(chart_data)
                else:
                    st.warning("No data found for the selected tables.")
            else:
                st.info("Please select at least one table to analyze.")
        else:
            st.warning(f"No tables found in the database '{database_name}'.")
    except Exception as e:
        st.error(f"Error accessing Hive database: {str(e)}")
else:
    st.info("Please enter a Hive database name to proceed.")
