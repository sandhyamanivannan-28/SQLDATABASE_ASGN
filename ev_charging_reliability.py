from __future__ import annotations

import math
import os
import random
import sqlite3
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path


# Change these settings before running if you want a different file or row count.
OUTPUT_DB = Path.cwd() / "ev_charging_reliability_assignment.sqlite"
SESSION_COUNT = 1_800  # The assignment requires at least 1,000 rows in one table.
RANDOM_SEED = 20261103  # Keeping this seed reproduces the same generated values.

REGIONS = (
    "North West", "North East", "Yorkshire", "West Midlands", "East Midlands",
    "South East", "South West", "Wales", "Scotland", "London",
)
CONNECTOR_POWER = {
    "Type 2": (7.0, 22.0),
    "CCS": (50.0, 150.0),
    "CHAdeMO": (40.0, 100.0),
}


SCHEMA = """
CREATE TABLE operators (
    operator_id   INTEGER PRIMARY KEY,
    operator_name TEXT NOT NULL UNIQUE,
    operator_type TEXT NOT NULL
        CHECK (operator_type IN ('regional', 'national', 'fleet')),
    founded_year  INTEGER NOT NULL CHECK (founded_year BETWEEN 2005 AND 2026)
);

CREATE TABLE stations (
    station_id   INTEGER PRIMARY KEY,
    operator_id  INTEGER NOT NULL REFERENCES operators(operator_id),
    station_name TEXT NOT NULL UNIQUE,
    region       TEXT NOT NULL,
    site_type    TEXT NOT NULL CHECK (site_type IN ('retail', 'motorway', 'workplace', 'car_park')),
    opened_on    TEXT NOT NULL,
    access_level TEXT NOT NULL CHECK (access_level IN ('public', 'staff_only', 'mixed')),
    is_active    INTEGER NOT NULL CHECK (is_active IN (0, 1))
);

CREATE TABLE chargers (
    charger_id  INTEGER PRIMARY KEY,
    station_id  INTEGER NOT NULL REFERENCES stations(station_id),
    charger_code TEXT NOT NULL,
    installed_on TEXT NOT NULL,
    charger_status TEXT NOT NULL
        CHECK (charger_status IN ('operational', 'maintenance', 'offline')),
    UNIQUE (station_id, charger_code)
);

-- A charger can offer more than one plug type. This composite key lets sessions
-- refer only to connector types that the selected charger actually supports.
CREATE TABLE charger_connectors (
    charger_id      INTEGER NOT NULL REFERENCES chargers(charger_id),
    connector_type  TEXT NOT NULL
        CHECK (connector_type IN ('Type 2', 'CCS', 'CHAdeMO')),
    rated_power_kw  REAL NOT NULL CHECK (rated_power_kw BETWEEN 3.0 AND 200.0),
    PRIMARY KEY (charger_id, connector_type)
);

CREATE TABLE drivers (
    driver_id        INTEGER PRIMARY KEY,
    vehicle_type     TEXT NOT NULL
        CHECK (vehicle_type IN ('small_car', 'family_car', 'van', 'fleet_vehicle')),
    home_region      TEXT NOT NULL,
    membership_level TEXT NOT NULL CHECK (membership_level IN ('standard', 'member', 'fleet')),
    account_created  TEXT NOT NULL,
    is_fleet_user    INTEGER NOT NULL CHECK (is_fleet_user IN (0, 1))
);

CREATE TABLE charging_sessions (
    session_id        INTEGER PRIMARY KEY,
    charger_id        INTEGER NOT NULL,
    connector_type    TEXT NOT NULL,
    driver_id         INTEGER NOT NULL REFERENCES drivers(driver_id),
    started_at        TEXT NOT NULL,
    ended_at          TEXT NOT NULL,
    energy_kwh        REAL NOT NULL CHECK (energy_kwh >= 0 AND energy_kwh <= 120),
    charging_minutes  INTEGER NOT NULL CHECK (charging_minutes BETWEEN 1 AND 360),
    energy_cost_gbp   REAL NOT NULL CHECK (energy_cost_gbp >= 0 AND energy_cost_gbp <= 150),
    ambient_temp_c    REAL NOT NULL CHECK (ambient_temp_c BETWEEN -15 AND 40),
    satisfaction_rating INTEGER CHECK (satisfaction_rating BETWEEN 1 AND 5),
    session_outcome   TEXT NOT NULL
        CHECK (session_outcome IN ('completed', 'interrupted', 'failed')),
    payment_method    TEXT NOT NULL
        CHECK (payment_method IN ('contactless', 'app', 'subscription', 'fleet_card')),
    CHECK (ended_at > started_at),
    FOREIGN KEY (charger_id, connector_type)
        REFERENCES charger_connectors(charger_id, connector_type)
);

CREATE TABLE technicians (
    technician_id INTEGER PRIMARY KEY,
    contractor_name TEXT NOT NULL,
    skill_level   INTEGER NOT NULL CHECK (skill_level BETWEEN 1 AND 5),
    joined_on     TEXT NOT NULL
);

CREATE TABLE maintenance_events (
    maintenance_id INTEGER PRIMARY KEY,
    charger_id     INTEGER NOT NULL REFERENCES chargers(charger_id),
    reported_at    TEXT NOT NULL,
    resolved_at    TEXT NOT NULL,
    issue_type     TEXT NOT NULL
        CHECK (issue_type IN ('payment_reader', 'connector_fault', 'network', 'power_unit', 'inspection')),
    severity       TEXT NOT NULL CHECK (severity IN ('low', 'medium', 'high', 'critical')),
    downtime_minutes INTEGER NOT NULL CHECK (downtime_minutes BETWEEN 0 AND 10080),
    repair_cost_gbp REAL NOT NULL CHECK (repair_cost_gbp BETWEEN 0 AND 10000),
    CHECK (resolved_at >= reported_at)
);

-- Maintenance jobs can involve several technicians, and each technician works
-- on many jobs. The pair is the natural identifier for each assignment.
CREATE TABLE maintenance_assignments (
    maintenance_id INTEGER NOT NULL REFERENCES maintenance_events(maintenance_id),
    technician_id  INTEGER NOT NULL REFERENCES technicians(technician_id),
    hours_worked   REAL NOT NULL CHECK (hours_worked BETWEEN 0.25 AND 24),
    PRIMARY KEY (maintenance_id, technician_id)
);

-- This table represents incoming meter records before review. It intentionally
-- permits duplicate source references and missing readings for data-quality work.
CREATE TABLE session_imports (
    import_id             INTEGER PRIMARY KEY,
    session_id            INTEGER NOT NULL REFERENCES charging_sessions(session_id),
    source_reference      TEXT NOT NULL,
    imported_at           TEXT NOT NULL,
    reported_energy_kwh   REAL CHECK (reported_energy_kwh BETWEEN 0 AND 120),
    reported_start_time   TEXT NOT NULL,
    import_status         TEXT NOT NULL CHECK (import_status IN ('accepted', 'needs_review', 'duplicate')),
    duplicate_of_import_id INTEGER REFERENCES session_imports(import_id)
);

CREATE TABLE data_quality_notes (
    note_id       INTEGER PRIMARY KEY,
    issue_type    TEXT NOT NULL UNIQUE,
    rows_affected INTEGER NOT NULL CHECK (rows_affected >= 0),
    rate_percent  REAL NOT NULL CHECK (rate_percent BETWEEN 0 AND 100),
    explanation   TEXT NOT NULL
);

CREATE INDEX idx_stations_operator ON stations(operator_id);
CREATE INDEX idx_chargers_station ON chargers(station_id);
CREATE INDEX idx_sessions_charger_time ON charging_sessions(charger_id, started_at);
CREATE INDEX idx_sessions_outcome ON charging_sessions(session_outcome);
CREATE INDEX idx_maintenance_charger_date ON maintenance_events(charger_id, reported_at);
CREATE INDEX idx_imports_session ON session_imports(session_id);
"""


def iso_time(value: datetime) -> str:
    """Store timestamps consistently in UTC with seconds precision."""
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat()


def random_datetime(rng: random.Random, start: datetime, end: datetime) -> datetime:
    """Choose a random timestamp from the inclusive date range."""
    span = int((end - start).total_seconds())
    return start + timedelta(seconds=rng.randint(0, span))


def make_reference_rows(rng: random.Random):
    """Create the small fictional tables that sessions and repairs refer to."""
    operators = [
        (i, f"Northstar Charge Network {i:02d}", rng.choice(("regional", "national", "fleet")),
         rng.randint(2008, 2024))
        for i in range(1, 7)
    ]

    stations = []
    station_open_dates = {}
    for station_id in range(1, 41):
        operator_id = rng.randint(1, len(operators))
        founded_year = operators[operator_id - 1][3]
        earliest_opening = date(max(2016, founded_year), 1, 1)
        latest_opening = date(2025, 12, 31)
        opened = date.fromordinal(rng.randint(
            earliest_opening.toordinal(), latest_opening.toordinal()
        ))
        station_open_dates[station_id] = opened
        stations.append((
            station_id,
            operator_id,
            f"Synthetic Charge Site {station_id:03d}",
            rng.choice(REGIONS),
            rng.choice(("retail", "motorway", "workplace", "car_park")),
            opened.isoformat(),
            rng.choices(("public", "staff_only", "mixed"), weights=(78, 8, 14))[0],
            int(rng.random() >= 0.04),
        ))

    chargers = []
    charger_connectors = []
    next_charger_id = 1
    for station_id, *_ in stations:
        for local_number in range(rng.randint(2, 4)):
            # A charger cannot be installed before its station opens.
            earliest_install = station_open_dates[station_id]
            latest_install = date(2025, 12, 31)
            installed = date.fromordinal(rng.randint(
                earliest_install.toordinal(), latest_install.toordinal()
            ))
            status = rng.choices(("operational", "maintenance", "offline"), weights=(91, 6, 3))[0]
            chargers.append((
                next_charger_id, station_id, f"CH-{local_number + 1:02d}",
                installed.isoformat(), status,
            ))

            # Fast units often have two plug standards; slower destination units
            # are more likely to have only a Type 2 connector.
            connector_types = rng.choices(
                [
                    ("Type 2",),
                    ("CCS",),
                    ("CCS", "CHAdeMO"),
                    ("Type 2", "CCS"),
                ],
                weights=(25, 35, 15, 25),
            )[0]
            for connector in connector_types:
                low, high = CONNECTOR_POWER[connector]
                if connector == "Type 2":
                    power = rng.choice((7.0, 11.0, 22.0))
                else:
                    power = float(rng.choice((50, 75, 100, 120, 150)))
                charger_connectors.append((next_charger_id, connector, min(max(power, low), high)))
            next_charger_id += 1

    drivers = []
    for driver_id in range(1, 501):
        fleet_user = rng.random() < 0.12
        vehicle_type = (
            rng.choice(("van", "fleet_vehicle")) if fleet_user
            else rng.choice(("small_car", "family_car", "van"))
        )
        drivers.append((
            driver_id,
            vehicle_type,
            rng.choice(REGIONS),
            "fleet" if fleet_user else rng.choices(("standard", "member"), weights=(65, 35))[0],
            # Every account predates the first charging session in this dataset.
            date(rng.randint(2020, 2024), rng.randint(1, 12), rng.randint(1, 25)).isoformat(),
            int(fleet_user),
        ))

    technicians = [
        (i, f"Fictional Service Team {((i - 1) // 5) + 1:02d}", rng.randint(1, 5),
         # All technicians join before the maintenance timeline begins in 2024.
         date(rng.randint(2018, 2023), rng.randint(1, 12), rng.randint(1, 25)).isoformat())
        for i in range(1, 31)
    ]
    return operators, stations, chargers, charger_connectors, drivers, technicians


def seasonal_temperature(rng: random.Random, month: int) -> float:
    """Return a plausible UK-like outdoor temperature for the session month."""
    # The sine curve gives cooler winters and warmer summers, with daily variation.
    typical = 9.0 + 7.0 * math.sin((month - 3) * 2 * math.pi / 12)
    return round(max(-10.0, min(35.0, rng.gauss(typical, 4.0))), 1)


def make_sessions(rng: random.Random, chargers, connector_rows, drivers, session_count: int):
    """Generate sessions with durations and costs tied to their energy and charger."""
    connectors_by_charger = {}
    for charger_id, connector_type, rated_power_kw in connector_rows:
        connectors_by_charger.setdefault(charger_id, []).append((connector_type, rated_power_kw))

    charger_status = {row[0]: row[4] for row in chargers}
    available_chargers = [row[0] for row in chargers if charger_status[row[0]] != "offline"]
    driver_ids = [row[0] for row in drivers]
    first_day = datetime(2025, 1, 1, tzinfo=timezone.utc)
    last_day = datetime(2026, 10, 31, 23, 59, tzinfo=timezone.utc)
    installed_dates = {
        row[0]: datetime.combine(date.fromisoformat(row[3]), time.min, tzinfo=timezone.utc)
        for row in chargers
    }

    sessions = []
    for session_id in range(1, session_count + 1):
        charger_id = rng.choice(available_chargers)
        connector_type, rated_power = rng.choice(connectors_by_charger[charger_id])
        # Session activity starts on or after the selected charger's installation.
        started = random_datetime(rng, max(first_day, installed_dates[charger_id]), last_day)
        outcome = rng.choices(("completed", "interrupted", "failed"), weights=(93, 5, 2))[0]

        if outcome == "failed":
            energy = 0.0
            minutes = rng.randint(1, 12)
        else:
            if outcome == "interrupted":
                energy = round(rng.uniform(1.0, 18.0), 2)
            elif connector_type == "Type 2":
                energy = round(rng.triangular(4.0, 45.0, 18.0), 2)
            else:
                energy = round(rng.triangular(8.0, 82.0, 34.0), 2)

            # Real charging is usually slower than the connector's rated maximum.
            effective_power = rated_power * rng.uniform(0.55, 0.88)
            minutes = int(round((energy / effective_power) * 60 + rng.uniform(4, 18)))
            minutes = max(1, min(360, minutes))

        ended = started + timedelta(minutes=minutes)
        unit_price = rng.uniform(0.34, 0.79)
        cost = round(energy * unit_price, 2)
        satisfaction = rng.choices((1, 2, 3, 4, 5), weights=(3, 7, 19, 39, 32))[0]
        if rng.random() < 0.28:  # Many drivers do not complete the optional survey.
            satisfaction = None

        sessions.append((
            session_id, charger_id, connector_type, rng.choice(driver_ids),
            iso_time(started), iso_time(ended), energy, minutes, cost,
            seasonal_temperature(rng, started.month), satisfaction, outcome,
            rng.choices(
                ("contactless", "app", "subscription", "fleet_card"),
                weights=(42, 32, 18, 8),
            )[0],
        ))
    return sessions


def make_import_rows(rng: random.Random, sessions):
    """Create raw meter imports, including a small documented quality sample."""
    imports = []
    missing_readings = 0
    duplicate_count = 0
    import_id = 1

    for session in sessions:
        session_id, _charger, _connector, _driver, started_text, _ended, energy = session[:7]
        started = datetime.fromisoformat(started_text)
        source_reference = f"MTR-{session_id:06d}"
        imported_at = iso_time(started + timedelta(minutes=rng.randint(1, 30)))

        # A small number of meter uploads arrive without an energy value. The
        # operational session table stays complete; this raw layer preserves the issue.
        missing = rng.random() < 0.025
        reported_energy = None if missing else energy
        if missing:
            missing_readings += 1

        status = "needs_review" if missing else "accepted"
        imports.append((
            import_id, session_id, source_reference, imported_at, reported_energy,
            started_text, status, None,
        ))
        original_import_id = import_id
        import_id += 1

        # A duplicate means the same session and source reference were submitted
        # again with the same business values. The import_id remains unique, and
        # duplicate_of_import_id points to the original row for easy review.
        if rng.random() < 0.03:
            duplicate_count += 1
            imports.append((
                import_id, session_id, source_reference, imported_at,
                reported_energy, started_text, "duplicate", original_import_id,
            ))
            # If the repeated row also lacks its reading, count that missing
            # value as well because it is another stored import row.
            if reported_energy is None:
                missing_readings += 1
            import_id += 1

    return imports, missing_readings, duplicate_count


def make_maintenance_rows(rng: random.Random, chargers, technicians):
    """Generate repair events and many-to-many technician assignments."""
    charger_ids = [row[0] for row in chargers]
    technician_ids = [row[0] for row in technicians]
    events, assignments = [], []
    start_range = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end_range = datetime(2026, 10, 1, tzinfo=timezone.utc)
    installed_dates = {
        row[0]: datetime.combine(date.fromisoformat(row[3]), time.min, tzinfo=timezone.utc)
        for row in chargers
    }

    for maintenance_id in range(1, 221):
        charger_id = rng.choice(charger_ids)
        reported = random_datetime(rng, max(start_range, installed_dates[charger_id]), end_range)
        issue = rng.choices(
            ("payment_reader", "connector_fault", "network", "power_unit", "inspection"),
            weights=(14, 32, 22, 20, 12),
        )[0]
        severity = rng.choices(("low", "medium", "high", "critical"), weights=(30, 43, 22, 5))[0]
        downtime = rng.randint(0, 180) if severity == "low" else rng.randint(30, 960)
        resolved = reported + timedelta(minutes=downtime)
        repair_cost = round(rng.uniform(0, 180) if issue == "inspection" else rng.uniform(35, 2800), 2)
        events.append((
            maintenance_id, charger_id, iso_time(reported), iso_time(resolved),
            issue, severity, downtime, repair_cost,
        ))

        assigned = rng.sample(technician_ids, rng.randint(1, 3))
        for technician_id in assigned:
            assignments.append((maintenance_id, technician_id, round(rng.uniform(0.5, 8.0), 2)))

    return events, assignments


def insert_many(connection: sqlite3.Connection, table: str, columns: str, rows) -> None:
    """Insert a batch with parameters rather than building SQL from data values."""
    if not rows:
        return
    placeholders = ", ".join("?" for _ in rows[0])
    connection.executemany(f"INSERT INTO {table} ({columns}) VALUES ({placeholders})", rows)


def validate_database(connection: sqlite3.Connection, expected_sessions: int) -> None:
    """Check the generated file's size and key relationships before saving it."""
    session_count = connection.execute("SELECT COUNT(*) FROM charging_sessions").fetchone()[0]
    if session_count != expected_sessions or session_count < 1_000:
        raise RuntimeError(f"Expected at least 1,000 sessions; found {session_count}.")

    session_columns = connection.execute("PRAGMA table_info(charging_sessions)").fetchall()
    if len(session_columns) < 7:
        raise RuntimeError("The main table has fewer than seven columns.")

    if connection.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
        raise RuntimeError("SQLite integrity_check did not return 'ok'.")
    foreign_key_issues = connection.execute("PRAGMA foreign_key_check").fetchall()
    if foreign_key_issues:
        raise RuntimeError(f"Invalid foreign-key references: {foreign_key_issues[:3]}")

    bad_sessions = connection.execute("""
        SELECT COUNT(*)
        FROM charging_sessions
        WHERE ended_at <= started_at
           OR energy_kwh < 0
           OR charging_minutes < 1
           OR ambient_temp_c NOT BETWEEN -15 AND 40
    """).fetchone()[0]
    if bad_sessions:
        raise RuntimeError(f"Found {bad_sessions} sessions outside the expected ranges.")

    bad_station_dates = connection.execute("""
        SELECT COUNT(*)
        FROM stations AS s
        JOIN chargers AS c USING (station_id)
        WHERE date(c.installed_on) < date(s.opened_on)
    """).fetchone()[0]
    if bad_station_dates:
        raise RuntimeError(f"Found {bad_station_dates} chargers installed before their station opened.")

    bad_operator_dates = connection.execute("""
        SELECT COUNT(*)
        FROM stations AS s
        JOIN operators AS o USING (operator_id)
        WHERE CAST(substr(s.opened_on, 1, 4) AS INTEGER) < o.founded_year
    """).fetchone()[0]
    if bad_operator_dates:
        raise RuntimeError(f"Found {bad_operator_dates} stations opened before their operator was founded.")

    bad_session_dates = connection.execute("""
        SELECT COUNT(*)
        FROM charging_sessions AS cs
        JOIN chargers AS c USING (charger_id)
        JOIN drivers AS d USING (driver_id)
        WHERE date(cs.started_at) < date(c.installed_on)
           OR date(cs.started_at) < date(d.account_created)
           OR ABS((julianday(cs.ended_at) - julianday(cs.started_at)) * 1440
                  - cs.charging_minutes) > 0.1
    """).fetchone()[0]
    if bad_session_dates:
        raise RuntimeError(f"Found {bad_session_dates} sessions with inconsistent dates or durations.")

    bad_session_values = connection.execute("""
        SELECT COUNT(*)
        FROM charging_sessions AS cs
        JOIN charger_connectors AS cc
          ON cc.charger_id = cs.charger_id
         AND cc.connector_type = cs.connector_type
        WHERE cs.energy_kwh > cc.rated_power_kw * cs.charging_minutes / 60.0 + 0.05
           OR cs.energy_cost_gbp < cs.energy_kwh * 0.34 - 0.02
           OR cs.energy_cost_gbp > cs.energy_kwh * 0.79 + 0.02
    """).fetchone()[0]
    if bad_session_values:
        raise RuntimeError(f"Found {bad_session_values} sessions with impossible power or tariff values.")

    bad_technician_dates = connection.execute("""
        SELECT COUNT(*)
        FROM maintenance_assignments AS a
        JOIN maintenance_events AS m USING (maintenance_id)
        JOIN technicians AS t USING (technician_id)
        WHERE date(t.joined_on) > date(m.reported_at)
    """).fetchone()[0]
    if bad_technician_dates:
        raise RuntimeError(f"Found {bad_technician_dates} jobs assigned before a technician joined.")

    anomaly_counts = connection.execute("""
        SELECT
            (SELECT COUNT(*) FROM session_imports WHERE reported_energy_kwh IS NULL),
            (SELECT COUNT(*) FROM session_imports WHERE duplicate_of_import_id IS NOT NULL),
            (SELECT COUNT(*) FROM charging_sessions WHERE satisfaction_rating IS NULL)
    """).fetchone()
    if not all(anomaly_counts):
        raise RuntimeError("The planned missing and duplicate values were not generated.")

    # Every table should have a primary key, and every flagged duplicate should
    # match its original business fields while keeping a different import_id.
    tables = [row[0] for row in connection.execute("""
        SELECT name FROM sqlite_master
        WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
    """)]
    for table in tables:
        primary_keys = [column for column in connection.execute(f'PRAGMA table_info("{table}")') if column[5]]
        if not primary_keys:
            raise RuntimeError(f"Table {table} does not have a primary key.")

    unmatched_duplicates = connection.execute("""
        SELECT COUNT(*)
        FROM session_imports AS duplicate
        LEFT JOIN session_imports AS original
          ON original.import_id = duplicate.duplicate_of_import_id
        WHERE duplicate.duplicate_of_import_id IS NOT NULL
          AND (original.import_id IS NULL
               OR original.session_id != duplicate.session_id
               OR original.source_reference != duplicate.source_reference
               OR original.reported_energy_kwh IS NOT duplicate.reported_energy_kwh
               OR original.reported_start_time != duplicate.reported_start_time
               OR original.import_id = duplicate.import_id)
    """).fetchone()[0]
    if unmatched_duplicates:
        raise RuntimeError(f"Found {unmatched_duplicates} duplicate imports that do not match their originals.")

    # Keep the written quality summary in step with the rows that actually exist.
    import_count = connection.execute("SELECT COUNT(*) FROM session_imports").fetchone()[0]
    quality_summary = {
        row[0]: (row[1], row[2])
        for row in connection.execute(
            "SELECT issue_type, rows_affected, rate_percent FROM data_quality_notes"
        )
    }
    expected_quality_summary = {
        "missing_meter_reading": (
            anomaly_counts[0], round(anomaly_counts[0] * 100 / import_count, 2)
        ),
        "duplicate_meter_upload": (
            anomaly_counts[1], round(anomaly_counts[1] * 100 / session_count, 2)
        ),
        "optional_satisfaction_missing": (
            anomaly_counts[2], round(anomaly_counts[2] * 100 / session_count, 2)
        ),
    }
    if quality_summary != expected_quality_summary:
        raise RuntimeError(
            "data_quality_notes does not match the missing and duplicate rows in the database."
        )


def create_database(output_path: Path, session_count: int, seed: int) -> dict[str, int]:
    """Build the SQLite file safely and return row counts for the summary."""
    if session_count < 1_000:
        raise ValueError("The assignment requires at least 1,000 charging sessions.")

    output_path = output_path.expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = output_path.with_name(output_path.name + ".building")
    if temp_path.exists():
        temp_path.unlink()

    rng = random.Random(seed)
    operators, stations, chargers, connectors, drivers, technicians = make_reference_rows(rng)
    sessions = make_sessions(rng, chargers, connectors, drivers, session_count)
    imports, missing_count, duplicate_count = make_import_rows(rng, sessions)
    maintenance, assignments = make_maintenance_rows(rng, chargers, technicians)

    connection = None
    try:
        connection = sqlite3.connect(temp_path)
        connection.execute("PRAGMA foreign_keys = ON")
        connection.executescript(SCHEMA)

        with connection:
            insert_many(connection, "operators",
                        "operator_id, operator_name, operator_type, founded_year", operators)
            insert_many(connection, "stations",
                        "station_id, operator_id, station_name, region, site_type, opened_on, access_level, is_active", stations)
            insert_many(connection, "chargers",
                        "charger_id, station_id, charger_code, installed_on, charger_status", chargers)
            insert_many(connection, "charger_connectors",
                        "charger_id, connector_type, rated_power_kw", connectors)
            insert_many(connection, "drivers",
                        "driver_id, vehicle_type, home_region, membership_level, account_created, is_fleet_user", drivers)
            insert_many(connection, "charging_sessions",
                        "session_id, charger_id, connector_type, driver_id, started_at, ended_at, energy_kwh, charging_minutes, energy_cost_gbp, ambient_temp_c, satisfaction_rating, session_outcome, payment_method", sessions)
            insert_many(connection, "technicians",
                        "technician_id, contractor_name, skill_level, joined_on", technicians)
            insert_many(connection, "maintenance_events",
                        "maintenance_id, charger_id, reported_at, resolved_at, issue_type, severity, downtime_minutes, repair_cost_gbp", maintenance)
            insert_many(connection, "maintenance_assignments",
                        "maintenance_id, technician_id, hours_worked", assignments)
            insert_many(connection, "session_imports",
                        "import_id, session_id, source_reference, imported_at, reported_energy_kwh, reported_start_time, import_status, duplicate_of_import_id", imports)

            notes = [
                (1, "missing_meter_reading", missing_count,
                 round(missing_count * 100 / len(imports), 2),
                 "A small share of raw meter submissions has no energy reading. These records are kept in session_imports and marked needs_review; the curated session record remains available."),
                (2, "duplicate_meter_upload", duplicate_count,
                 round(duplicate_count * 100 / len(sessions), 2),
                 "Some source uploads are repeated. Each repeated row is retained, marked duplicate, and linked to its first import with duplicate_of_import_id."),
                (3, "optional_satisfaction_missing",
                 connection.execute("SELECT COUNT(*) FROM charging_sessions WHERE satisfaction_rating IS NULL").fetchone()[0],
                 round(connection.execute("SELECT COUNT(*) FROM charging_sessions WHERE satisfaction_rating IS NULL").fetchone()[0] * 100 / len(sessions), 2),
                 "Satisfaction is an optional survey response, so blank ratings mean the driver did not answer; they are not treated as a low score."),
            ]
            insert_many(connection, "data_quality_notes",
                        "note_id, issue_type, rows_affected, rate_percent, explanation", notes)

        validate_database(connection, session_count)
        connection.close()
        connection = None
        os.replace(temp_path, output_path)
    except Exception:
        if connection is not None:
            connection.close()
        if temp_path.exists():
            temp_path.unlink()
        raise

    with sqlite3.connect(output_path) as saved:
        table_names = [
            "operators", "stations", "chargers", "charger_connectors", "drivers",
            "charging_sessions", "technicians", "maintenance_events",
            "maintenance_assignments", "session_imports", "data_quality_notes",
        ]
        return {
            table: saved.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in table_names
        }


def display_database_details(output_path: Path, session_count: int) -> None:
    """Print every table's columns and the actual missing/duplicate data totals."""
    with sqlite3.connect(output_path) as connection:
        tables = [row[0] for row in connection.execute("""
            SELECT name FROM sqlite_master
            WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
            ORDER BY name
        """)]

        print("\nTable structure and row counts")
        for table in tables:
            # Table names come from sqlite_master, not from user input.
            row_count = connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
            columns = connection.execute(f'PRAGMA table_info("{table}")').fetchall()
            print(f"\n{table} — {row_count:,} rows")
            for column in columns:
                key_note = " PRIMARY KEY" if column[5] else ""
                null_note = " NOT NULL" if column[3] else ""
                print(f"  {column[1]}: {column[2]}{key_note}{null_note}")

            foreign_keys = connection.execute(f'PRAGMA foreign_key_list("{table}")').fetchall()
            for key in foreign_keys:
                print(f"  FOREIGN KEY {key[3]} → {key[2]}.{key[4]}")

            unique_indexes = [
                index for index in connection.execute(f'PRAGMA index_list("{table}")')
                if index[2] and index[3] == "u"
            ]
            for index in unique_indexes:
                key_columns = [
                    item[2] for item in connection.execute(f'PRAGMA index_info("{index[1]}")')
                ]
                print(f"  UNIQUE: ({', '.join(key_columns)})")

        session_import_count = connection.execute(
            "SELECT COUNT(*) FROM session_imports"
        ).fetchone()[0]
        missing_meter = connection.execute(
            "SELECT COUNT(*) FROM session_imports WHERE reported_energy_kwh IS NULL"
        ).fetchone()[0]
        duplicate_imports = connection.execute(
            "SELECT COUNT(*) FROM session_imports WHERE duplicate_of_import_id IS NOT NULL"
        ).fetchone()[0]
        missing_ratings = connection.execute(
            "SELECT COUNT(*) FROM charging_sessions WHERE satisfaction_rating IS NULL"
        ).fetchone()[0]

        print("\nPlanned data-quality values retained in the database")
        print("  Duplicate definition: same session_id, source_reference, reported energy, and reported start time; import_id stays unique.")
        print(f"  Duplicate meter submissions: {duplicate_imports:,} ({duplicate_imports / session_count:.2%} of sessions)")
        print(f"  Missing raw meter readings (SQL NULL): {missing_meter:,} ({missing_meter / session_import_count:.2%} of imports)")
        print(f"  Missing optional satisfaction ratings (SQL NULL): {missing_ratings:,} ({missing_ratings / session_count:.2%} of sessions)")

        print("\nMeasurement types and ordinal ordering")
        print("  Nominal: region, connector_type, payment_method; categories have no ranking.")
        print("  Ordinal: satisfaction_rating 1 < 2 < 3 < 4 < 5; 1 is very dissatisfied and 5 is very satisfied.")
        print("  Ordinal: maintenance severity low < medium < high < critical.")
        print("  Interval: ambient_temp_c; Celsius differences are meaningful, while zero is not the absence of temperature.")
        print("  Ratio: energy_kwh, charging_minutes, energy_cost_gbp; zero means none of the measured amount.")

        print("\nTable separation and key relationships")
        print("  operators → stations → chargers → charging_sessions keeps each entity's details in one place.")
        print("  charger_connectors uses the composite key (charger_id, connector_type) to limit sessions to supported plugs.")
        print("  maintenance_assignments uses (maintenance_id, technician_id) for the many-to-many repair staffing relationship.")
        print("  session_imports preserves raw meter submissions so missing readings and repeated business records remain auditable.")


def main() -> None:
    create_database(OUTPUT_DB, SESSION_COUNT, RANDOM_SEED)
    print(f"Created database: {OUTPUT_DB.resolve()}")
    print(f"Random seed: {RANDOM_SEED}")
    display_database_details(OUTPUT_DB.resolve(), SESSION_COUNT)
    print("\nDatabase integrity and foreign-key checks passed.")


if __name__ == "__main__":
    main()