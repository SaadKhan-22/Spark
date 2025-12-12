# Spark
Contains Useful Spark Code
- Official PySpark docs are [here](https://spark.apache.org/docs/latest/api/python/index.html).
- Best Practices can be found [here](https://spark.apache.org/docs/latest/api/python/tutorial/pandas_on_spark/best_practices.html?highlight=specifiedwindowframe).
- One style guide is [this](https://github.com/palantir/pyspark-style-guide).




## Notes:
- ### 3 Ways Spark went against Conventional Wisdom:
  - **Adding SQL on top of Spark** gave the ability for in-memory, low-latency, general graphs of MapReduce operations, essentially giving the best of both worlds (i.e., Conventional DBs and MPPs). It also gave mid-query fault tolerance which could tolerate changes (to a degree) in the cluster's hardware. 
  - **One Size fits All** was a philosophy considered outdated at the time (consider that the Cloud was not as ubiquitous then as it now is). The trend was heading in the direction of specialised systems for different Data Analytics programming models. However, managing multiple systems came with the challenges of siginificant management overheads, difficulty in resource sharing between disparate platforms, and high latencies between the systems using different programming models which hindered performance severely. Based on its success in mapping the MapReduce model to SQL, Spark reused the approach for all these models (e.g., as iterative MapReduce jobs or combining many small ones) which solved many of the challenges mentioned just prior. The rationale for this was the fact that MR generalised extremely well to nearly every distributed compute workload (see [the Bulk Synchronous Parallel](https://en.wikipedia.org/wiki/Bulk_synchronous_parallel)) all of which it emulated as local computation + message passing between nodes.
