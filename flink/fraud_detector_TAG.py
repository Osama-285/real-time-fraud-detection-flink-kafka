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
RAPID_TX_COUNT = 20
IMPOSSIBLE_TRAVEL_MS = 120_000
EARTH_RADIUS_KM = 6371

LOCATION_COORDS = {
    "NY": (40.7128, -74.0060),
    "CA": (34.0522, -118.2437),
    "TX": (29.7604, -95.3698),
    "FL": (25.7617, -80.1918),
    "IL": (41.8781, -87.6298),
}

FRAUD_ALERT_TAG = OutputTag("fraud-alerts", Types.STRING())

RISK_AUDIT_TAG = OutputTag("risk-audit", Types.STRING())


def haversine(a, b):
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


class FraudDetector(KeyedProcessFunction):

    def open(self, ctx: RuntimeContext):
        ttl = (
            StateTtlConfig.new_builder(Time.minutes(10))
            .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .build()
        )

        tx_desc = ListStateDescriptor("txs", Types.LONG())
        tx_desc.enable_time_to_live(ttl)
        self.tx_state = ctx.get_list_state(tx_desc)

        loc_desc = ValueStateDescriptor("loc", Types.STRING())
        loc_desc.enable_time_to_live(ttl)
        self.last_loc = ctx.get_state(loc_desc)

        time_desc = ValueStateDescriptor("time", Types.LONG())
        time_desc.enable_time_to_live(ttl)
        self.last_time = ctx.get_state(time_desc)

    def process_element(self, value, ctx):
        score = 0
        reasons = []

        event_id = value[1]
        card_id = value[4]
        amount = float(value[7])
        location = value[9]
        ip = value[10]
        ts = value[12]

        event_time = int(datetime.fromisoformat(ts.replace("Z", "")).timestamp() * 1000)

        # Rule 1: High amount
        if amount > MAX_AMOUNT:
            score += 40
            reasons.append("HIGH_AMOUNT")

        # Rule 2: Velocity
        history = list(self.tx_state.get())
        history = [t for t in history if event_time - t <= RAPID_WINDOW_MS]
        history.append(event_time)
        self.tx_state.update(history)

        if len(history) >= RAPID_TX_COUNT:
            score += 30
            reasons.append("RAPID_TRANSACTIONS")

        # Rule 3: Impossible travel
        last_loc = self.last_loc.value()
        last_time = self.last_time.value()

        if (
            last_loc
            and last_time
            and location != last_loc
            and event_time - last_time <= IMPOSSIBLE_TRAVEL_MS
            and location in LOCATION_COORDS
            and last_loc in LOCATION_COORDS
        ):
            dist = haversine(LOCATION_COORDS[last_loc], LOCATION_COORDS[location])
            if dist > 500:
                score += 50
                reasons.append("IMPOSSIBLE_TRAVEL")

        result = {
            "event_id": event_id,
            "card_id": card_id,
            "amount": amount,
            "location": location,
            "ip": ip,
            "risk_score": score,
            "reasons": reasons,
            "event_time": ts,
            "status": "FRAUD" if score >= 40 else "LEGIT",
        }

        # Update state
        self.last_loc.update(location)
        self.last_time.update(event_time)

        # Side outputs
        yield json.dumps(result)

    # ✅ Fraud side output
        if score >= 40:
            yield FRAUD_ALERT_TAG, json.dumps(result)

        # ✅ Risk audit side output (ALL events)
        yield RISK_AUDIT_TAG, json.dumps(
            {
                "card_id": card_id,
                "score": score,
                "rules_triggered": reasons,
                "event_time": ts,
            }
        )

        # Main stream
        yield json.dumps(result)


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(4)

    kafka = (
        KafkaSource.builder()
        .set_bootstrap_servers("broker2:29094")
        .set_topics("transactions")
        .set_group_id("fraud-v3")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(
            JsonRowDeserializationSchema.builder()
            .type_info(
                Types.ROW_NAMED(
                    [
                        "schema_version",
                        "event_id",
                        "transaction_id",
                        "customer_id",
                        "card_id",
                        "merchant_id",
                        "merchant_category",
                        "amount",
                        "currency",
                        "location",
                        "ip_address",
                        "event_type",
                        "timestamp",
                    ],
                    [
                        Types.STRING(),
                        Types.STRING(),
                        Types.STRING(),
                        Types.STRING(),
                        Types.STRING(),
                        Types.STRING(),
                        Types.STRING(),
                        Types.DOUBLE(),
                        Types.STRING(),
                        Types.STRING(),
                        Types.STRING(),
                        Types.STRING(),
                        Types.STRING(),
                    ],
                )
            )
            .build()
        )
        .build()
    )

    wm = WatermarkStrategy.for_bounded_out_of_orderness(
        Duration.of_seconds(5)
    ).with_timestamp_assigner(
        lambda e, ts: int(
            datetime.fromisoformat(e[12].replace("Z", "")).timestamp() * 1000
        )
    )
    stream = env.from_source(kafka, wm, "kafka-source")
    processed = stream.key_by(lambda e: e[4]).process(
        FraudDetector(), output_type=Types.STRING()
    )
    fraud_alerts = processed.get_side_output(FRAUD_ALERT_TAG)
    risk_audit = processed.get_side_output(RISK_AUDIT_TAG)

    processed.print("ALL")
    fraud_alerts.print("FRAUD_ALERT")
    risk_audit.print("RISK_AUDIT")

    env.execute("real-time-fraud-detection")


if __name__ == "__main__":
    main()
