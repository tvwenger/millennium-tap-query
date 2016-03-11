# millennium-tap-query
Query the Millennium Simulation UWS/TAP client

This is a simple wrapper for the Python package `requests` to deal
with connections to the [Millennium TAP Web
Client](http://galformod.mpa-garching.mpg.de/millenniumtap/).  With
this tool you can perform basic or advanced queries to the Millennium
Simulation database and download the data products.

## Usage Examples
```python
"""
Set up a new job, perform a simple query, and save the results
"""
import millennium_query
conn = millennium_query.MillenniumQuery('username','password')
conn.query('SELECT TOP 10 * FROM MPAGalaxies..DeLucia2006a')
conn.run() # Start job
conn.wait() # Wait until job is finished
conn.save('results.csv') # Stream results to file
conn.close() # close connection
```

```python
"""
Connect to an already submitted job and save the results
"""
job_id = 'foobar'
conn = millennium_query.MillenniumQuery('username','password',job_id=job_id)
conn.wait() # Make sure job is done
conn.save('results.csv')
```

```python
"""
Acquire cookies, then run several jobs
"""
cookies = millennium_query.get_cookies('username','password')
jobs = []
for i in xrange(10):
    conn = millennium_query.MillenniumQuery('username','password',cookies=cookies)
    conn.query('SELECT TOP {0} * FROM MPAGalaxies..DeLucia2006a'.format(i))
    conn.run()
    jobs.append(conn)
for i,conn in enumerate(jobs):
    conn.wait()
    conn.save('results_{0}.csv'.format(i))
    conn.close()
```
