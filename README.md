# millennium-tap-query
Query the Millennium Simulation UWS/TAP client

This is a simple wrapper for the Python package `requests` to deal
with connections to the [Millennium TAP Web
Client](http://galformod.mpa-garching.mpg.de/millenniumtap/).  With
this tool you can perform basic or advanced queries to the Millennium
Simulation database and download the data products.

## Usage Example
```python
"""
Set up a new job, perform a simple query, and save the results
"""
import millennium_query
conn = MillenniumQuery('username','password')
conn.query('SELECT TOP 10 * FROM MPAGalaxies..DeLucia2006a')
conn.run() # Start job
conn.wait() # Wait until job is finished
conn.save('results.csv') # Stream results to file
conn.close() # close connection
```
