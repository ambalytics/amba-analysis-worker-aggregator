# amba-analysis-worker-aggregator

The Aggregator is implemented in python using Faust, a python stream
processing library. The Aggregator is based on the ideas of Kafka
streams. Even though Faust is built for stream processing, it is
designed for more manageable and smaller tasks. However, Faust is well
integrated into Kafka and allows an easy setup for asynchronous
multiprocessing, event-driven processing as well as time-driven
processing. Processing is achieved through agents which can run defined
by CRON Syntax, allowing the scheduling to be in line with the InfluxDB.

This Component is the primary ingesting source to the InfluxDB and is
responsible for the initial setup of the InfluxDB. All needed buckets,
as well as the needed tasks, need to be created. The Aggregator uses a
key supplied by the environment and used by the InfluxDB to allow
access. The Kafka connection consumes new processed discussion events
and writes their relevant data into the InfluxDB. Since the writing rate
is relatively low, the data is written synchronously as soon as
possible. To ensure the writing succeeds even if their network issues or
the InfluxDB is temporarily unavailable, retries are set up in case of
failure.

The main task of the Aggregator is the trend calculation. Its setup is
together with the InfluxDB needed to ensure correct trending
calculations and schedules. All trend calculations are run as CRON
agents, meaning they will always run at a specific time. The times are
coordinated with the InfluxDB tasks. An overview of all tasks and their
running time can be seen in Figure [1]. Compared
to timer agents, this allows restarts without resetting the timer and
keeping the calculation at regular intervals. Running multiple trending
queries on the InfluxDB increases the system requirements to ensure
timely calculations before running out of time, resulting in read
time-outs. Further, it allows optimization at which time what trend is
calculated to avoid collisions of the calculations. A queue is used to
avoid such collisions and ensure the calculations are not run directly.
Instead, all calculations run sequential, reducing the load on the
database. Trends are always started with an offset similar to the
long-running task setup in InfluxDB, ensuring better resource
management.

![cron_influx_heatmap](https://user-images.githubusercontent.com/84507772/142741680-2b716549-c67c-48f6-aec7-e7aeef761e72.png)
[1]: Heatmap Cron Jobs InfluxDB/Trend Calculation

Before the trends are calculated, the influx is queried to retrieve only
publications that have enough events. A minimum of events needs to exist
to be considered helpful to the trend calculation. The minimum
significantly reduces the series cardinality and ensures that prediction
and trend algorithms have enough valid data to produce sensible results.
Using these DOIs first, a query run in the influx calculates most of the
trending values. After that, it retrieves needed data to calculate the
Theil-sen’s Slope Estimator value using *pyMannKendall* . To reduce the
load on the influx and reduce single query times, the DOIs are split in
a list of 200, ensuring the cardinality stays low and the system load
relatively low. The added overhead timewise due to multiple requests is
not relevant.

The InfluxDB does most of our calculations, reducing the need to
transfer data and allowing an optimal and efficient calculation. These
calculations are split into two categories. One uses windowed data
meaning aggregating multiple events within a time window, the durations
of which can be seen in Table [\[tab:aggregator_scores\]][1]. If no
events are available, a 0 window is created to ensure evenly spaced data
needed for Holt Winter’s prediction and moving averages. The other
categories used the event data directly to calculate the mean values and
the trending score, the sum of all exponentially weighted scores of a
publication, with each weighted score defined as:

![grafik](https://user-images.githubusercontent.com/84507772/142741942-70c84c2e-06e3-42d5-bdc7-ab13846a45c4.png)

Where time is a timestamp in nanoseconds and gravity is a negative value
selected for a duration of trend calculation. The negative gravities
used ensure a that newer events, with less time differences, will be
exponentially higher weighted than older and are defined as seen in
Table [1]. Depending on the gravity, the time
duration most relevant can be adjusted. However, all events will always
be used to calculate the score, since the weight is always bigger than
0.

| Duration | Gravity            | Window Duration |
|:---------|:-------------------|:----------------|
| 6 Hours  | -0.00036           | 6 Minutes       |
| 24 Hours | -0.000025          | 24 Minutes      |
| 7 Days   | -0.00000357142857  | 168 Minutes     |
| 30 Days  | -0.000000833333333 | 12 Hours        |
| 365 Days | -0.000000833333333 | 146 Hours       |

Trending Score Configuration Values

Once all trends are calculated, all old trending rows are removed from
the PostgreSQL. Then the new ones are stored. Additionally, calculation
results are stored with the time the calculation occurred that allows
analyzing the development of trends over a period of time. Finally, new
trends wilt trigger an update to refresh the materialized view of
trending COVID-19 papers. Lastly, the Aggregator uses an agent to
retrieve the top three relevant publications every morning, with
COVID-19 as a top entity. The tweet content is generated by mixing fixed
strings and the publication data. The title of the publications is
shortened to follow tweet restrictions. A URL to the corresponding web
page is added too. The tweet is then sent to the Twitter API using the
bot credentials read from the environment.
