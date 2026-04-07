Script to calculate `trips_per_hour` across MBTA rapid transit stations on a specific day using public MBTA GTFS data.

The scheduled calendar day chosen was `2026-04-08` (Wednesday). Chosen since weekday service schedule is fairly consistent and so we use `2026-04-08` as a represnetative weekday.

`trips_per_hour` is the total number of rapid transit trains serving a station per hour and is used as a
"transit score" to determine transit activity at a station.

Default measured hours are from `7-10` (morning commute peak) to reflect our group's goal of determining TOD HOUSING development priority. 

To run locally `python transit_score.py` with the ([MBTA GTFS](https://www.mbta.com/developers/gtfs)) dataset in the same directory 