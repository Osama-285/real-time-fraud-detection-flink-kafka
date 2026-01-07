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