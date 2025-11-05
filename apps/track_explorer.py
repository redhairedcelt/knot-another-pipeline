# app.py
import datetime as dt
import hashlib
import numpy as np
import pandas as pd
import pydeck as pdk
import streamlit as st
from pathlib import Path

# ----------------------------
# PAGE CONFIG
# ----------------------------
st.set_page_config(page_title="Track Explorer", layout="wide")
st.title("Track Explorer — Streamlit + PyDeck")

# Default data directory under data/interim/tracks_to_explore; fallback to empty string.
_interim_dir = Path(__file__).resolve().parent.parent / "data" / "interim" / "tracks_to_explore"
if _interim_dir.exists():
    csv_candidates = sorted(_interim_dir.glob("*.csv"))
    DEFAULT_PATH = str(csv_candidates[0]) if csv_candidates else ""
else:
    DEFAULT_PATH = ""

# ----------------------------
# HELPERS
# ----------------------------
@st.cache_data(show_spinner=False)
def load_csv(path: str) -> pd.DataFrame:
    return pd.read_csv(path)

def try_detect_columns(df: pd.DataFrame):
    """Guess lat/lon/timestamp/track-id columns by common names."""
    lower = {c.lower(): c for c in df.columns}
    lat = lower.get("lat") or lower.get("latitude")
    lon = lower.get("lon") or lower.get("lng") or lower.get("longitude")
    ts  = lower.get("timestamp") or lower.get("time") or lower.get("datetime")
    tid = (
        lower.get("mmsi")
        or lower.get("cab_name")
        or lower.get("vessel_id")
        or lower.get("vehicle_id")
        or lower.get("id")
        or lower.get("cab_id")
        or lower.get("medallion")
    )
    return lat, lon, ts, tid

def to_epoch_ms(series):
    # Robust timestamp parsing → epoch ms (nullable Int64)
    s = pd.to_datetime(series, errors="coerce", utc=True)
    return (s.view("int64") // 10**6).astype("Int64")

def make_paths(df, max_tracks=200):
    """Build PathLayer input: [{path: [[lon,lat], ...]}, ...]"""
    paths = (
        df.sort_values(["track_id", "timestamp"])
          .groupby("track_id", sort=False)
          .apply(lambda g: g[["lon", "lat"]].dropna().values.tolist())
          .reset_index(name="path")
    )
    paths["npts"] = paths["path"].map(len)
    paths = paths[paths["npts"] > 2].drop(columns=["npts"]).head(max_tracks)
    return paths

def make_trips(df, max_tracks=200):
    """
    TripsLayer expects rows with 'path': [[lon, lat, ts_ms], ...].
    """
    def pack_trip(g):
        g = g.sort_values("timestamp")
        arr = np.column_stack([g["lon"].values, g["lat"].values, g["ts_ms"].values])
        arr = arr[~np.isnan(arr).any(axis=1)]
        return arr.tolist()

    trips = (
        df.dropna(subset=["lon", "lat", "ts_ms"])
          .sort_values(["track_id", "timestamp"])
          .groupby("track_id", sort=False)
          .apply(pack_trip)
          .reset_index(name="path")
    )
    trips["npts"] = trips["path"].map(len)
    trips = trips[trips["npts"] > 2].drop(columns=["npts"]).head(max_tracks)
    return trips

def ts_to_py_naive(ts_in):
    """
    Convert a pandas.Timestamp or datetime to a naive Python datetime
    (keeps UTC wall time, strips tzinfo) for Streamlit's slider.
    """
    if isinstance(ts_in, pd.Timestamp):
        dt_py = ts_in.to_pydatetime()
    else:
        dt_py = ts_in
    if isinstance(dt_py, dt.datetime) and dt_py.tzinfo is not None:
        dt_py = dt_py.astimezone(dt.timezone.utc).replace(tzinfo=None)
    return dt_py

# ----------------------------
# SIDEBAR: DATA INPUT
# ----------------------------
st.sidebar.header("Data")
path = st.sidebar.text_input("CSV path", DEFAULT_PATH)

uploaded = st.sidebar.file_uploader("…or upload CSV", type=["csv"])
if uploaded is not None:
    df_raw = pd.read_csv(uploaded)
elif Path(path).expanduser().exists():
    df_raw = load_csv(str(Path(path).expanduser()))
else:
    st.info("Provide a valid CSV path or upload a CSV to begin.")
    st.stop()

# ----------------------------
# SIDEBAR: COLUMN MAPPING
# ----------------------------
lat_guess, lon_guess, ts_guess, id_guess = try_detect_columns(df_raw)

def _default_index(guess: str | None, exclude: set[str]) -> int:
    if guess in df_raw.columns:
        return int(df_raw.columns.get_loc(guess))
    for idx, col in enumerate(df_raw.columns):
        if col not in exclude:
            return idx
    return 0

lat_col = st.sidebar.selectbox(
    "Latitude column",
    list(df_raw.columns),
    index=_default_index(lat_guess, exclude=set()),
)
lon_col = st.sidebar.selectbox(
    "Longitude column",
    list(df_raw.columns),
    index=_default_index(lon_guess, exclude={lat_col}),
)
id_col = st.sidebar.selectbox(
    "Track ID column",
    list(df_raw.columns),
    index=_default_index(id_guess, exclude={lat_col, lon_col}),
)
ts_col = st.sidebar.selectbox(
    "Timestamp column",
    list(df_raw.columns),
    index=_default_index(ts_guess, exclude={lat_col, lon_col, id_col}),
)

# ----------------------------
# DATA NORMALISATION
# ----------------------------
selection_map = {
    "Latitude": lat_col,
    "Longitude": lon_col,
    "Track ID": id_col,
    "Timestamp": ts_col,
}
if len(set(selection_map.values())) != len(selection_map.values()):
    st.error("Latitude, longitude, track ID, and timestamp must be mapped to distinct columns.")
    st.stop()

df = df_raw.rename(
    columns={
        lat_col: "lat",
        lon_col: "lon",
        id_col: "track_id",
        ts_col: "timestamp",
    }
).copy()

# Ensure numeric lat/lon values
df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
df["lon"] = pd.to_numeric(df["lon"], errors="coerce")

# Robust timestamp handling
df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
df["ts_ms"] = to_epoch_ms(df["timestamp"])
df["timestamp_str"] = df["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S").fillna("n/a")

# Guard against missing IDs by synthesising stable placeholders
df["track_id"] = df["track_id"].astype(str)
missing_ids = df["track_id"].isin({"nan", "None", "NaT"})
if missing_ids.any():
    df.loc[missing_ids, "track_id"] = (
        "track_" + df.index.to_series().astype(str)
    )[missing_ids]

df_geo = df.dropna(subset=["lat", "lon"]).copy()
if df_geo.empty:
    st.warning("No valid latitude/longitude rows found. Check column selections.")
    st.stop()

# ----------------------------
# SIDEBAR: FILTERS
# ----------------------------
st.sidebar.header("Filters")
if df_geo["timestamp"].notna().any():
    ts_min = df_geo["timestamp"].min()
    ts_max = df_geo["timestamp"].max()
    if ts_min == ts_max:
        st.sidebar.info("Dataset has a single timestamp; time slider disabled.")
        ts_start, ts_end = ts_min, ts_max
    else:
        ts_start_py = ts_to_py_naive(ts_min)
        ts_end_py = ts_to_py_naive(ts_max)
        ts_start_py, ts_end_py = st.sidebar.slider(
            "Time range",
            min_value=ts_start_py,
            max_value=ts_end_py,
            value=(ts_start_py, ts_end_py),
            format="YYYY-MM-DD HH:mm:ss",
        )
        ts_start = pd.Timestamp(ts_start_py, tz="UTC")
        ts_end = pd.Timestamp(ts_end_py, tz="UTC")
    if ts_min != ts_max:
        df_geo = df_geo[df_geo["timestamp"].between(ts_start, ts_end)]

if df_geo.empty:
    st.warning("No rows remain after applying filters.")
    st.stop()

track_counts = df_geo["track_id"].value_counts()
track_total = int(track_counts.shape[0])
track_slider_max = max(track_total, 1)
if track_slider_max == 1:
    max_tracks = 1
else:
    max_tracks = st.sidebar.slider(
        "Max tracks to include",
        min_value=1,
        max_value=track_slider_max,
        value=min(50, track_slider_max),
        help="Limits the number of tracks rendered to keep the map responsive.",
    )
default_tracks = track_counts.head(max_tracks).index.tolist()
selected_tracks = st.sidebar.multiselect(
    "Track IDs",
    options=track_counts.index.tolist(),
    default=default_tracks,
    help="Defaults to the most sampled tracks.",
)
if not selected_tracks:
    st.sidebar.warning("Select at least one track; falling back to defaults.")
    selected_tracks = default_tracks

df_geo = df_geo[df_geo["track_id"].isin(selected_tracks)].copy()
if df_geo.empty:
    st.warning("Selected track IDs have no rows in the current filters.")
    st.stop()

# Derive deterministic colors per track
def color_from_track(track_id: str):
    digest = hashlib.sha256(str(track_id).encode("utf-8")).hexdigest()
    r = int(digest[0:2], 16)
    g = int(digest[2:4], 16)
    b = int(digest[4:6], 16)
    return [r, g, b, 200]

color_map = {track_id: color_from_track(track_id) for track_id in selected_tracks}
df_geo["color"] = df_geo["track_id"].map(color_map)

# Optionally animate trips if timestamp data available
animate_trips = st.sidebar.checkbox("Animate trips (TripsLayer)", value=False, help="Requires valid timestamps.")
trail_minutes = None
if animate_trips:
    if df_geo["ts_ms"].notna().any():
        trail_minutes = st.sidebar.slider("Trail length (minutes)", 1, 120, 15)
    else:
        st.sidebar.info("TripsLayer requires valid timestamps to animate.")

# ----------------------------
# AGGREGATIONS + SUMMARY
# ----------------------------
col1, col2, col3 = st.columns(3)
col1.metric("Tracks shown", f"{len(selected_tracks):,}")
col2.metric("Points rendered", f"{len(df_geo):,}")
span = ""
if df_geo["timestamp"].notna().any():
    span = f"{df_geo['timestamp'].min().strftime('%Y-%m-%d')} → {df_geo['timestamp'].max().strftime('%Y-%m-%d')}"
col3.metric("Time span", span or "n/a")

# ----------------------------
# MAP LAYERS
# ----------------------------
path_data = make_paths(df_geo)
if not path_data.empty:
    path_data["color"] = path_data["track_id"].map(color_map)

layers = [
    pdk.Layer(
        "ScatterplotLayer",
        df_geo,
        get_position="[lon, lat]",
        get_radius=25,
        radius_min_pixels=2,
        get_color="color",
        pickable=True,
    )
]

if animate_trips and df_geo["ts_ms"].notna().any():
    trip_data = make_trips(df_geo)
    if not trip_data.empty:
        trip_data["color"] = trip_data["track_id"].map(color_map)
        trip_data["timestamps"] = trip_data["path"].apply(lambda coords: [pt[2] for pt in coords])
        trip_data["path"] = trip_data["path"].apply(lambda coords: [[pt[0], pt[1]] for pt in coords])
        ts_min_ms = int(df_geo["ts_ms"].min())
        ts_max_ms = int(df_geo["ts_ms"].max())
        if ts_min_ms == ts_max_ms:
            st.sidebar.info("TripsLayer animation disabled — all timestamps collapse to a single instant.")
            current_time = ts_min_ms
        else:
            current_time = st.sidebar.slider(
                "Animation time",
                min_value=ts_min_ms,
                max_value=ts_max_ms,
                value=ts_min_ms,
                step=max(1, (ts_max_ms - ts_min_ms) // 200 or 1),
            )
        layers.append(
            pdk.Layer(
                "TripsLayer",
                trip_data,
                get_path="path",
                get_timestamps="timestamps",
                get_color="color",
                width_min_pixels=4,
                trail_length=int(trail_minutes * 60_000) if trail_minutes else 600_000,
                current_time=current_time,
                opacity=0.9,
            )
        )
else:
    if not path_data.empty:
        layers.append(
            pdk.Layer(
                "PathLayer",
                path_data,
                get_path="path",
                get_color="color",
                width_scale=30,
                width_min_pixels=2,
                rounded=True,
                pickable=True,
            )
        )

view_state = pdk.ViewState(
    latitude=float(df_geo["lat"].mean()),
    longitude=float(df_geo["lon"].mean()),
    zoom=8,
    pitch=10,
    bearing=0,
)

tooltip = {
    "html": "<b>Track:</b> {track_id}<br/><b>Time:</b> {timestamp_str}",
    "style": {"backgroundColor": "steelblue", "color": "white"},
}

st.pydeck_chart(
    pdk.Deck(
        map_style=None,
        initial_view_state=view_state,
        layers=layers,
        tooltip=tooltip,
    )
)

# ----------------------------
# RAW DATA VIEW
# ----------------------------
with st.expander("Filtered records", expanded=False):
    display_cols = ["track_id", "timestamp_str", "lat", "lon"]
    st.dataframe(df_geo[display_cols])
