import pandas as pd

START_HOUR = 7
END_HOUR = 10
GTFS_FOLDER = "./MBTA_GTFS"
ANALYSIS_DATE = "20260408"   # Wednesday


def load_gtfs(gtfs_folder=GTFS_FOLDER):
    stops = pd.read_csv(f"{gtfs_folder}/stops.txt", dtype=str)
    stop_times = pd.read_csv(f"{gtfs_folder}/stop_times.txt", dtype=str)
    trips = pd.read_csv(f"{gtfs_folder}/trips.txt", dtype=str)
    routes = pd.read_csv(f"{gtfs_folder}/routes.txt", dtype=str)
    calendar = pd.read_csv(f"{gtfs_folder}/calendar.txt", dtype=str)
    calendar_dates = pd.read_csv(f"{gtfs_folder}/calendar_dates.txt", dtype=str)
    return stops, stop_times, trips, routes, calendar, calendar_dates


def classify_line(row):
    long_name = str(row.get("route_long_name", "")).lower()
    short_name = str(row.get("route_short_name", "")).lower()

    if "red" in long_name:
        return "Red"
    if "orange" in long_name:
        return "Orange"
    if "blue" in long_name:
        return "Blue"
    if "green" in long_name:
        return "Green"
    if "mattapan" in long_name or "mattapan" in short_name:
        return "Mattapan"
    return "Other"


def active_service_ids_for_date(calendar, calendar_dates, date_str):
    dt = pd.to_datetime(date_str, format="%Y%m%d")
    weekday_name = dt.day_name().lower()

    base = calendar[
        (calendar["start_date"] <= date_str) &
        (calendar["end_date"] >= date_str) &
        (calendar[weekday_name] == "1")
    ]["service_id"]

    active = set(base)

    exceptions = calendar_dates[calendar_dates["date"] == date_str]
    for _, row in exceptions.iterrows():
        sid = row["service_id"]
        ex_type = row["exception_type"]
        if ex_type == "1":
            active.add(sid)
        elif ex_type == "2":
            active.discard(sid)

    return active


def pick_dominant_service_ids_by_line(rapid_trips, rapid_routes):
    """
    Among active rapid-transit trips, pick the biggest service_id bucket per line.
    This prevents overlapping weekday schedule families from being stacked.
    """
    tmp = rapid_trips.merge(
        rapid_routes[["route_id", "line"]],
        on="route_id",
        how="left"
    )

    counts = (
        tmp.groupby(["line", "service_id"])
        .size()
        .reset_index(name="trip_count")
        .sort_values(["line", "trip_count"], ascending=[True, False])
    )

    chosen = counts.drop_duplicates("line")[["line", "service_id"]]

    print("\nChosen dominant service_id per line:")
    print(chosen.to_string(index=False))

    keep_ids = set(chosen["service_id"])
    return keep_ids


def compute_rapid_transit_station_totals(
    gtfs_folder=GTFS_FOLDER,
    analysis_date=ANALYSIS_DATE,
    start_hour=START_HOUR,
    end_hour=END_HOUR,
    output_csv="mbta_rapid_transit_station_totals.csv"
):
    stops, stop_times, trips, routes, calendar, calendar_dates = load_gtfs(gtfs_folder)

    routes["route_type_num"] = pd.to_numeric(routes["route_type"], errors="coerce")

    # rapid transit only
    rapid_routes = routes[routes["route_type_num"].isin([0, 1])].copy()
    rapid_routes["line"] = rapid_routes.apply(classify_line, axis=1)
    rapid_routes = rapid_routes[
        rapid_routes["line"].isin(["Red", "Orange", "Blue", "Green", "Mattapan"])
    ].copy()

    active_ids = active_service_ids_for_date(calendar, calendar_dates, analysis_date)
    if not active_ids:
        raise ValueError(f"No active service_ids found for {analysis_date}")

    # first pass: active rapid-transit trips on that date
    rapid_trips_active = trips[
        trips["route_id"].isin(rapid_routes["route_id"]) &
        trips["service_id"].isin(active_ids)
    ].copy()

    if rapid_trips_active.empty:
        raise ValueError("No active rapid-transit trips found on that date.")

    # key fix: keep only dominant service_id per line
    chosen_service_ids = pick_dominant_service_ids_by_line(rapid_trips_active, rapid_routes)

    rapid_trips = rapid_trips_active[
        rapid_trips_active["service_id"].isin(chosen_service_ids)
    ].copy()

    rapid_stop_times = stop_times[
        stop_times["trip_id"].isin(rapid_trips["trip_id"])
    ].copy()

    rapid_stop_times["hour"] = rapid_stop_times["departure_time"].apply(
        lambda x: int(str(x).split(":")[0])
    )

    windowed = rapid_stop_times[
        (rapid_stop_times["hour"] >= start_hour) &
        (rapid_stop_times["hour"] < end_hour)
    ].copy()

    if windowed.empty:
        raise ValueError("No rapid transit stop events found in that time window.")

    windowed = windowed.merge(
        stops[["stop_id", "parent_station", "stop_name"]],
        on="stop_id",
        how="left"
    )

    windowed = windowed.merge(
        rapid_trips[["trip_id", "route_id"]],
        on="trip_id",
        how="left"
    )

    windowed = windowed.merge(
        rapid_routes[["route_id", "line"]],
        on="route_id",
        how="left"
    )

    # collapse platforms into parent station where available
    windowed["station_id"] = windowed["parent_station"].fillna(windowed["stop_id"])

    # parent-station name lookup
    station_lookup = stops[["stop_id", "stop_name"]].drop_duplicates().rename(
        columns={"stop_id": "station_id", "stop_name": "station_name"}
    )

    fallback_names = (
        windowed.groupby(["station_id", "stop_name"])
        .size()
        .reset_index(name="n")
        .sort_values(["station_id", "n"], ascending=[True, False])
        .drop_duplicates("station_id")
        [["station_id", "stop_name"]]
        .rename(columns={"stop_name": "fallback_station_name"})
    )

    num_hours = end_hour - start_hour

    counts_total = (
        windowed.groupby("station_id")
        .size()
        .reset_index(name="num_trips")
    )
    counts_total["trips_per_hour"] = counts_total["num_trips"] / num_hours

    lines_served = (
        windowed.groupby("station_id")["line"]
        .agg(lambda x: ", ".join(sorted(set(v for v in x if pd.notna(v)))))
        .reset_index(name="lines_served")
    )

    result = counts_total.merge(station_lookup, on="station_id", how="left")
    result = result.merge(fallback_names, on="station_id", how="left")
    result["station_name"] = result["station_name"].fillna(result["fallback_station_name"])
    result = result.drop(columns=["fallback_station_name"])
    result = result.merge(lines_served, on="station_id", how="left")

    result["analysis_date"] = analysis_date
    result["time_window"] = f"{start_hour:02d}:00-{end_hour:02d}:00"

    result = result[
        [
            "station_id",
            "station_name",
            "analysis_date",
            "time_window",
            "num_trips",
            "trips_per_hour",
            "lines_served"
        ]
    ].sort_values("trips_per_hour", ascending=False)

    result.to_csv(output_csv, index=False)

    print("\nActive service_ids on date:", len(active_ids))
    print("Rapid trips after active-date filter:", len(rapid_trips_active))
    print("Rapid trips after dominant-service filter:", len(rapid_trips))
    print("Windowed stop events:", len(windowed))
    print("\nPreview:")
    print(result.head(30))

    return result


compute_rapid_transit_station_totals()