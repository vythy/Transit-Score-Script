Script to calculate `trips_per_hour` across MBTA rapid transit stations on a specific day.

The scheduled calendar day chosen was `2026-04-08` (Wednesday). Chosen since weekday service schedule is fairly consistent and so we use `2026-04-08` as a represnetative weekday.

`trips_per_hour` is the total number of rapid transit trains serving a station per hour and is used as a
"transit score" to determine transit activity at a station.

Default measured hours are from `7-10` (morning commute peak) to reflect our group's goal of determining TOD HOUSING development priority. 

To run locally `python transit_score.py` with `MBTA_GTFS` dataset in same directory ([text](https://www.mbta.com/developers/gtfs))