import duckdb
import matplotlib.pyplot as plt
import pandas as pd

# Make graph of remote work percentages over time using all parquet files in the current dir.
data = duckdb.query("""
SELECT
EXTRACT(YEAR FROM publication_date) AS year,
EXTRACT(MONTH FROM publication_date) AS month,
SUM(CASE WHEN is_remote = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS remote_percentage
FROM '*_remote.parquet'
WHERE year < 2025
GROUP BY year, month
ORDER BY year, month;
""").df()

plt.figure(figsize=(10, 6))  # Set the size of the graph.
data['date'] = pd.to_datetime(data[['year', 'month']].assign(DAY=1))
plt.plot(data['date'], data['remote_percentage'], marker='o')

plt.title('Andel annonser med möjlighet att arbeta på distans')
plt.xlabel('Tidpunkt')
plt.ylabel('Andel distansjobb (%)')

plt.grid(True)
plt.xticks(rotation=45)
plt.figtext(0.3, 0.01, "Datakälla: Arbetsförmedlingens öppna data",
            ha="right", fontsize=8, color="gray")
plt.tight_layout()

# Show the plot
plt.show()
