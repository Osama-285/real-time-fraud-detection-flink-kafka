from pyflink.datastream import (
    StreamExecutionEnvironment,
    KeyedProcessFunction,
    OutputTag,
)
from pyflink.common.time import Time
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.common import Types, WatermarkStrategy, Duration
from pyflink.datastream.state import (
    ListStateDescriptor,
    ValueStateDescriptor,
    StateTtlConfig,
)
from pyflink.datastream.functions import RuntimeContext
from datetime import datetime
import math
import json

MAX_AMOUNT = 200
RAPID_WINDOW_MS = 10_000
RAPID_TX_COUNT = 3
IMPOSSIBLE_TRAVEL_MS = 120_000
EARTH_RADIUS_KM = 6371

LOCATION_COORDS = {
    "NY": (40.7128, -74.0060),
    "CA": (34.0522, -118.2437),
    "TX": (29.7604, -95.3698),
    "FL": (25.7617, -80.1918),
    "IL": (41.8781, -87.6298),
}

FRAUD_TAG = OutputTag("fraud", Types.STRING())
ALERT_TAG = OutputTag("alerts", Types.MAP(Types.STRING(), Types.STRING()))

def haversine(a,b):     #  fuction to determines the great-circle distance between two points
    lat1, lon1 = a
    lat2, lon2 = b
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    x = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlon / 2) ** 2
    )
    return 2 * EARTH_RADIUS_KM * math.atan2(math.sqrt(x), math.sqrt(1 - x))