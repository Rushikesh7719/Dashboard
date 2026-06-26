#it is just to test the git
from flask import Flask, jsonify, request, render_template
import serial
import threading
import time
import json
import os
import sqlite3
from datetime import datetime
from flask import session
from werkzeug.security import generate_password_hash, check_password_hash
from flask_cors import CORS
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
import secrets

# API_KEY   = "svr123"
PICO_PORT = "/dev/ttyACM0"
PICO_BAUD = 115200

# DB_PATH = "/home/pi/solar_dash/new_code/Dashboard/logs/telemetry.db"
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs", "telemetry.db")
app = Flask(__name__, static_folder='static', template_folder='templates')
app.secret_key = secrets.token_hex(32)
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    SESSION_COOKIE_SECURE=True,
    PERMANENT_SESSION_LIFETIME=3600,
)

CORS(app, supports_credentials=True, origins=[
    "https://overhear-culminate-cyclist.ngrok-free.dev/"
])

limiter = Limiter(get_remote_address, app=app, default_limits=["300 per minute"])

BACKEND_USERS = {
    "admin":    {"hash": "scrypt:32768:8:1$OfCzoxdOHms1wwFF$9e96f032ad39ae6fc88a62be5413a27e50fa61b300f937073fc06cf05de7449d255e291225f51b7dd6c43684182d875cf1c56adebe4f2dc1866801e4d528a77b", "role": "Administrator"},
    "operator": {"hash": "scrypt:32768:8:1$1vkeCSpIIwJEBJqb$275ff2d7806776d7eb0ff87fc2fe069bdce619d49296ed15bb7b32381ac337f0a3a0d49656d65543a280872ec9de1e87a87363de920ec3dbca867b071c3ba824", "role": "Operator"},
    "viewer":   {"hash": "scrypt:32768:8:1$1u1obA9SKZGmvCBy$a0ac01fd5cb4460b4a7fb0a65dc29902ac4d483db3f89f528cbe78f5dc60aeada06b855e6eb8a6943da2a28e31922edd01ce80222264bf8a945ed9f3e4c0000b", "role": "Viewer"},
}
CONTROL_ROLES = {"Administrator", "Operator"}

telemetry       = {}
notifications   = []
serial_lock     = threading.Lock()
pico_ser        = None
debug_log       = []
_prev_wind_alarm = False
_rtc_last_second = -1
_rtc_stuck_ticks = 0
_RTC_STUCK_LIMIT = 5
_commanded_mode = "auto"

# ── DB write queue (avoids blocking the serial read thread) ───────────────────
import queue
_db_queue = queue.Queue(maxsize=500)

# =============================================================================
#  DATABASE INIT
# =============================================================================
def db_init():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.executescript("""
        CREATE TABLE IF NOT EXISTS telemetry (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ts          TEXT    NOT NULL,
            cur_az      REAL, cur_el      REAL,
            az_actual   REAL, el_actual   REAL,
            cur_pulse   INTEGER, tar_pulse INTEGER,
            az_err      INTEGER,
            act_pulse   INTEGER, act_target INTEGER,
            el_err      INTEGER,
            wind_speed  REAL, wind_limit  REAL,
            wind_park_az INTEGER, wind_park_el INTEGER,
            wind_cool    INTEGER,
            tc_avg      REAL, tc_cj       REAL,
            tc_ok       INTEGER, tc_fault INTEGER,
            ds_temp     REAL,  ds_ok      INTEGER,
            lat         REAL, lon         REAL,
            rtc_date    TEXT, rtc_time    TEXT,
            rtc_ok      INTEGER,
            day_start   INTEGER, day_end  INTEGER,
            pitch       REAL, roll        REAL, yaw  REAL,
            ax          REAL, ay          REAL, az_imu REAL,
            gx          REAL, gy          REAL, gz   REAL,
            imu_ok      INTEGER, imu_alarm INTEGER,
            imu_el_diff REAL, imu_el_alert INTEGER,
            imu_enc_az  REAL, imu_vs_enc  REAL,
            mode        TEXT,
            synced      INTEGER, ishome      INTEGER,
            is_homing   INTEGER, night_park  INTEGER,
            night_done  INTEGER, moving_home INTEGER,
            prox_az     INTEGER, prox_el     INTEGER,
            el_full     INTEGER,
            az_home_miss INTEGER, el_home_miss INTEGER,
            nudge_cnt   INTEGER, el_nudge_cnt INTEGER,
            step_az     INTEGER, step_el     INTEGER,
            az_fault    INTEGER,
            cal_min_a   REAL, cal_max_a  REAL, cal_home_a REAL,
            cal_stroke  REAL, cal_ppm    REAL,
            cal_ppr     INTEGER, cal_gear INTEGER, cal_tgp INTEGER,
            tc_json     TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_ts ON telemetry(ts);""")
    con.commit()

    cur.execute("PRAGMA table_info(telemetry)")
    cols = [r[1] for r in cur.fetchall()]
    for new_col in ["tc_json"] + [f"tc_{i}" for i in range(1, 10)]:
        if new_col not in cols:
            coltype = "TEXT" if new_col == "tc_json" else "REAL"
            cur.execute(f"ALTER TABLE telemetry ADD COLUMN {new_col} {coltype}")
    con.commit()
    con.close()
    print(f"✅ DB ready: {DB_PATH}")

def db_writer_thread():
    """Drain the queue and batch-insert into SQLite."""
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    BATCH = 20          
    FLUSH_SECS = 5.0 
    buf = []
    last_flush = time.time()
    SQL = """
        INSERT INTO telemetry (
            ts,
            cur_az, cur_el, az_actual, el_actual,
            cur_pulse, tar_pulse, az_err,
            act_pulse, act_target, el_err,
            wind_speed, wind_limit, wind_park_az, wind_park_el, wind_cool,
            tc_avg, tc_cj, tc_ok, tc_fault, ds_temp, ds_ok,
            lat, lon, rtc_date, rtc_time, rtc_ok, day_start, day_end,
            pitch, roll, yaw, ax, ay, az_imu, gx, gy, gz,
            imu_ok, imu_alarm, imu_el_diff, imu_el_alert, imu_enc_az, imu_vs_enc,
            mode, synced, ishome, is_homing, night_park, night_done, moving_home,
            prox_az, prox_el, el_full, az_home_miss, el_home_miss,
            nudge_cnt, el_nudge_cnt, step_az, step_el, az_fault,
            cal_min_a, cal_max_a, cal_home_a, cal_stroke, cal_ppm,
            cal_ppr, cal_gear, cal_tgp,
            tc_1, tc_2, tc_3, tc_4, tc_5, tc_6, tc_7, tc_8, tc_9,
            tc_json
        ) VALUES (
            ?,
            ?,?,?,?,
            ?,?,?,
            ?,?,?,
            ?,?,?,?,?,
            ?,?,?,?,?,?,
            ?,?,?,?,?,?,?,
            ?,?,?,?,?,?,?,?,?,
            ?,?,?,?,?,?,
            ?,?,?,?,?,?,?,
            ?,?,?,?,?,
            ?,?,?,?,?,
            ?,?,?,?,?,
            ?,?,?,
            ?,?,?,?,?,?,?,?,?,
            ?
        )
    """
    def _flush(buf):
        if not buf:
            return
        try:
            con.executemany(SQL, buf)
            con.commit()
        except Exception as e:
            print(f"❌ DB write error: {e}")
    while True:
        try:
            row = _db_queue.get(timeout=FLUSH_SECS)
            buf.append(row)
            if len(buf) >= BATCH or (time.time() - last_flush) >= FLUSH_SECS:
                _flush(buf)
                buf = []
                last_flush = time.time()
        except queue.Empty:
            _flush(buf)
            buf = []
            last_flush = time.time()

def _enqueue_row(d):
    """Build a DB row tuple from the telemetry dict and push to queue."""
    t = telemetry
    tc_list = t.get("tc_temps", [])
    def _ch(i):
        return tc_list[i] if i < len(tc_list) else None

    row = (
        datetime.now().isoformat(timespec='seconds'),
        t.get("cur_az"), t.get("cur_el"), t.get("az_actual"), t.get("el_actual"),
        t.get("cur_pulse"), t.get("tar_pulse"), t.get("az_err"),
        t.get("act_pulse"), t.get("act_target"), t.get("el_err"),
        t.get("wind_speed"), t.get("wind_limit"),
        t.get("wind_park_az"), t.get("wind_park_el"), t.get("wind_cool"),
        t.get("tc_avg"), t.get("tc_cj"), t.get("tc_ok"), t.get("tc_fault"),
        t.get("ds_temp"), t.get("ds_ok"),
        t.get("lat"), t.get("lon"),
        t.get("rtc_date"), t.get("rtc_time"), t.get("rtc_ok"),
        t.get("day_start"), t.get("day_end"),
        t.get("pitch"), t.get("roll"), t.get("yaw"),
        t.get("ax"), t.get("ay"), t.get("az_imu"),
        t.get("gx"), t.get("gy"), t.get("gz"),
        t.get("imu_ok"), t.get("imu_alarm"),
        t.get("imu_el_diff"), t.get("imu_el_alert"),
        t.get("imu_enc_az"), t.get("imu_vs_enc"),
        t.get("mode"),
        t.get("synced"), t.get("ishome"), t.get("is_homing"),
        t.get("night_park"), t.get("night_done"), t.get("moving_home"),
        t.get("prox_az"), t.get("prox_el"), t.get("el_full"),
        t.get("az_home_miss"), t.get("el_home_miss"),
        t.get("nudge_cnt"), t.get("el_nudge_cnt"),
        t.get("step_az"), t.get("step_el"), t.get("az_fault"),
        t.get("cal_min_a"), t.get("cal_max_a"), t.get("cal_home_a"),
        t.get("cal_stroke"), t.get("cal_ppm"),
        t.get("cal_ppr"), t.get("cal_gear"), t.get("cal_tgp"),
        _ch(0), _ch(1), _ch(2), _ch(3), _ch(4), _ch(5), _ch(6), _ch(7), _ch(8),
        json.dumps(tc_list),
    )
    try:
        _db_queue.put_nowait(row)
    except queue.Full:
        pass  

def connect_pico():
    global pico_ser
    while True:
        if pico_ser is None or not pico_ser.is_open:
            try:
                pico_ser = serial.Serial(PICO_PORT, PICO_BAUD, timeout=1)
                pico_ser.reset_input_buffer()
                print(f"✅ Connected to Pico on {PICO_PORT}")
                _push_notif("Pico connected on " + PICO_PORT, "success")
            except Exception as e:
                print(f"⏳ Waiting for Pico... {e}")
                pico_ser = None
        time.sleep(2)

def send_to_pico(cmd_dict):
    global pico_ser
    if pico_ser and pico_ser.is_open:
        with serial_lock:
            try:
                json_cmd = json.dumps(cmd_dict) + "\n"
                pico_ser.write(json_cmd.encode())
                pico_ser.flush()
                print(f"📤 Sent: {json_cmd.strip()}")
            except Exception as e:
                print(f"❌ Serial write error: {e}")
                pico_ser.close()
                pico_ser = None
    else:
        print("⚠️  Pico not connected, skipping command")

def _check_rtc_ok(d):
    global _rtc_last_second, _rtc_stuck_ticks
    year = d.get("rtc_y", 2000)
    sec  = d.get("rtc_s", 0)
    if year <= 2000:
        _rtc_stuck_ticks = 0
        _rtc_last_second = -1
        return 0
    if sec == _rtc_last_second:
        _rtc_stuck_ticks += 1
    else:
        _rtc_stuck_ticks = 0
    _rtc_last_second = sec
    if _rtc_stuck_ticks >= _RTC_STUCK_LIMIT:
        return 0
    return 1

def read_pico_thread():
    global pico_ser, telemetry, _prev_wind_alarm
    while True:
        try:
            if pico_ser and pico_ser.is_open and pico_ser.in_waiting:
                raw = pico_ser.readline().decode(errors="ignore").strip()
                if raw.startswith("{") and raw.endswith("}"):
                    d = json.loads(raw)
                    if "error" in d and len(d) == 1:
                        print(f"[PICO ERROR] {d['error']}")
                        _push_debug(f"Pico: {d['error']}")
                        continue
                    tc_temps  = d.get("tc_temp",  [])
                    tc_cjs    = d.get("tc_cj",    [])
                    tc_oks    = d.get("tc_ok",    [])
                    tc_faults = d.get("tc_fault", [])
                    telemetry.update({
                        "mode":        d.get("mode", "auto"),
                        "cur_az":      d.get("az",       0.0),
                        "cur_el":      d.get("el",       0.0),
                        "az_actual":   d.get("az_actual", 0.0),
                        "el_actual":   d.get("el_actual", 0.0),
                        "cur_pulse":   d.get("az_enc",   0),
                        "tar_pulse":   d.get("az_tgt",   0),
                        "az_err":      d.get("az_err",   0),
                        "act_pulse":   d.get("el_enc",   0),
                        "act_target":  d.get("el_tgt",   0),
                        "el_err":      d.get("el_err",   0),
                        "wind_speed":  d.get("wind",     0.0),
                        "wind_limit":  d.get("wind_thr", 0.0),
                        "tc_temps":  tc_temps,
                        "tc_cjs":    tc_cjs,
                        "tc_oks":    tc_oks,
                        "tc_faults": tc_faults,
                        "tc_avg":    tc_temps[0] if tc_temps else 0.0,
                        "tc_cj":     tc_cjs[0]   if tc_cjs   else 0.0,
                        "tc_ok":     tc_oks[0]   if tc_oks   else 0,
                        "tc_fault":  tc_faults[0] if tc_faults else 0,
                        "ds_temp":   d.get("ds_temp", -999.0),
                        "ds_ok":     d.get("ds_ok", 0),
                        "lat":         d.get("lat",  0.0),
                        "lon":         d.get("lon",  0.0),
                        "rtc_time":    "{:02d}:{:02d}:{:02d}".format(
                                            d.get("rtc_h",  0),
                                            d.get("rtc_m",  0),
                                            d.get("rtc_s",  0)),
                        "rtc_date":    "{:04d}-{:02d}-{:02d}".format(
                                            d.get("rtc_y",  2000),
                                            d.get("rtc_mo", 1),
                                            d.get("rtc_d",  1)),
                        "rtc_ok":      _check_rtc_ok(d),
                        "day_start":   d.get("day_start", 6),
                        "day_end":     d.get("day_end",  20),
                        "imu_ok":      d.get("imu_ok",  0),
                        "imu_alarm":   d.get("imu_alarm", 0),
                        "ax":          d.get("imu_ax",  0.0),
                        "ay":          d.get("imu_ay",  0.0),
                        "az_imu":      d.get("imu_az",  0.0),
                        "gx":          d.get("imu_gx",  0.0),
                        "gy":          d.get("imu_gy",  0.0),
                        "gz":          d.get("imu_gz",  0.0),
                        "pitch":       d.get("imu_pitch", 0.0),
                        "roll":        d.get("imu_roll",  0.0),
                        "yaw":         d.get("imu_yaw",   0.0),
                        "imu_el_diff":   d.get("imu_el_diff",  0.0),
                        "imu_el_alert":  d.get("imu_el_alert", 0),
                        "imu_enc_az":    d.get("imu_enc_az",   0.0),
                        "imu_vs_enc":    d.get("imu_vs_enc",   0.0),
                        "prox_az":     d.get("prox_az", 0),
                        "prox_el":     d.get("prox_el", 0),
                        "el_full":     d.get("el_full", 0),
                        "wind_park_az":d.get("wind_park_az", 0),
                        "wind_park_el":d.get("wind_park_el", 0),
                        "wind_cool": 1 if (d.get("wind_cool_az", 0) or d.get("wind_cool_el", 0)) else 0,
                        "ishome":      d.get("ishome",      0),
                        "is_homing":   d.get("is_homing",   0),
                        "night_park":  d.get("night_park",  0),
                        "synced":      d.get("synced",      0),
                        "night_done":  d.get("night_done",  0),
                        "moving_home": d.get("moving_home", 0),
                        "nudge_cnt":   d.get("nudge_cnt",   0),
                        "az_home_miss":d.get("az_home_miss",0),
                        "el_home_miss":d.get("el_home_miss",0),
                        "el_nudge_cnt":d.get("el_nudge_cnt",0),
                        "step_az":     d.get("step_az", 0),
                        "step_el":     d.get("step_el", 0),
                        "ts":          d.get("ts", 0),
                        "cal_min_a":  d.get("cal_min_a",  9.533),
                        "cal_max_a":  d.get("cal_max_a",  95.0),
                        "cal_home_a": d.get("cal_home_a", 90.0),
                        "cal_stroke": d.get("cal_stroke", 550.0),
                        "cal_ppm":    d.get("cal_ppm",    3800.0),
                        "cal_ppr":    d.get("cal_ppr",    12050),
                        "cal_gear":   d.get("cal_gear",   1200),
                        "cal_tgp":    d.get("cal_tgp",    14460000),
                    })
                    az_raw = telemetry.get("az_actual", 0.0)
                    if az_raw is not None and (az_raw < 0 or az_raw > 360):
                        telemetry["az_actual"] = 0.0

                    # ── Write to DB (non-blocking)
                    _enqueue_row(d)
                    ws  = d.get("wind", 0.0)
                    thr = d.get("wind_thr", 0.0)
                    wind_alarm_now = (ws > thr > 0)
                    if wind_alarm_now and not _prev_wind_alarm:
                        _push_notif(f"High wind: {ws:.1f} m/s (limit {thr:.1f})", "danger")
                    elif not wind_alarm_now and _prev_wind_alarm:
                        _push_notif(f"Wind back to safe: {ws:.1f} m/s", "success")
                    _prev_wind_alarm = wind_alarm_now
                elif raw:
                    print(f"[PICO] {raw}")
                    _push_debug(raw)
            else:
                time.sleep(0.05)
        except json.JSONDecodeError:
            pass
        except Exception as e:
            print(f"❌ Read error: {e}")
            time.sleep(1)

def _push_notif(msg, level="info"):
    notifications.append({"msg": msg, "level": level,
                           "time": datetime.now().strftime("%H:%M:%S")})
    if len(notifications) > 50:
        notifications.pop(0)

def _push_debug(msg):
    debug_log.append({"msg": msg, "time": datetime.now().strftime("%H:%M:%S")})
    if len(debug_log) > 100:
        debug_log.pop(0)

# =============================================================================
#  FLASK ROUTES
# =============================================================================
@app.before_request
def check_auth():
    if request.path == "/" or request.path.startswith("/static"):
        return
    if request.path in ("/api/login", "/api/session"):
        return
    if request.method == "OPTIONS":
        return
    if request.path.startswith("/api"):
        if not session.get("logged_in"):
            return jsonify({"error": "Unauthorized"}), 401
        if request.path in ("/api/control", "/api/update") and session.get("role") not in CONTROL_ROLES:
            return jsonify({"error": "Forbidden — viewer role is read-only"}), 403
        
@app.route("/")
def index():
    return render_template("dashboard.html")

@app.route("/api/data")
def get_data():
    return jsonify(telemetry)

@app.route("/api/notifications")
def get_notifications():
    return jsonify(notifications[-20:])

@app.route("/api/debug")
def get_debug():
    return jsonify(debug_log[-50:])

@app.route("/api/wind")
def get_wind():
    speed  = telemetry.get("wind_speed", 0.0)
    limit  = telemetry.get("wind_limit", 0.0)
    status = "HIGH" if speed > limit else "SAFE"
    return jsonify({"speed": speed, "limit": limit, "status": status})

@app.route("/api/today")
def get_today():
    return jsonify(today_log[-100:])

# ── NEW: historical log query endpoint
@app.route("/api/logs")
def get_logs():
    """
    Query params:
      ?from=2025-01-01T00:00:00   (ISO, inclusive, default: 24 h ago)
      ?to=2025-01-02T00:00:00     (ISO, inclusive, default: now)
      ?limit=500                  (max rows, capped at 5000)
      ?cat=position|wind|imu|thermal|storage|gps|system|motor|all
    """
    from_ts  = request.args.get("from")
    to_ts    = request.args.get("to")
    limit    = min(int(request.args.get("limit", 200)), 5000)
    cat      = request.args.get("cat", "all")
    if not from_ts:
        from_ts = datetime.fromtimestamp(time.time() - 86400).isoformat(timespec='seconds')
    if not to_ts:
        to_ts = datetime.now().isoformat(timespec='seconds')

    # Column sets per category
    COL_SETS = {
        "position": "ts, cur_az, cur_el, az_actual, el_actual, cur_pulse, tar_pulse, az_err, act_pulse, act_target, el_err",
        "wind":     "ts, wind_speed, wind_limit, wind_park_az, wind_park_el, wind_cool",
        "imu":      "ts, pitch, roll, yaw, ax, ay, az_imu, gx, gy, gz, imu_ok, imu_alarm, imu_el_diff, imu_el_alert, imu_enc_az, imu_vs_enc",
        "thermal":  "ts, tc_1, tc_2, tc_3, tc_4, ds_temp, ds_ok",
        "storage":  "ts, tc_5, tc_6, tc_7, tc_8, tc_9",
        "gps":      "ts, lat, lon, rtc_date, rtc_time, rtc_ok, day_start, day_end",
        "system":   "ts, mode, synced, ishome, is_homing, night_park, night_done, moving_home, prox_az, prox_el, el_full, az_home_miss, el_home_miss, nudge_cnt, el_nudge_cnt, az_fault",
        "motor":    "ts, step_az, step_el, az_fault, wind_park_az, wind_park_el, wind_cool",
        "all":      "*",
    }
    cols = COL_SETS.get(cat, "*")
    try:
        con = sqlite3.connect(DB_PATH)
        con.row_factory = sqlite3.Row
        cur = con.cursor()
        cur.execute(
            f"SELECT {cols} FROM telemetry WHERE ts BETWEEN ? AND ? ORDER BY ts DESC LIMIT ?",
            (from_ts, to_ts, limit)
        )
        rows = [dict(r) for r in cur.fetchall()]
        con.close()
        return jsonify({"count": len(rows), "rows": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/login", methods=["POST"])
@limiter.limit("10 per minute")
def login():
    data = request.get_json(force=True) or {}
    uid = (data.get("username") or "").strip().lower()
    pw  = data.get("password") or ""
    user = BACKEND_USERS.get(uid)
    if not user or not check_password_hash(user["hash"], pw):
        return jsonify({"error": "Invalid credentials"}), 401
    session.clear()
    session["logged_in"] = True
    session["user"] = uid
    session["role"] = user["role"]
    session.permanent = True
    return jsonify({"status": "ok", "user": uid, "role": user["role"]})

@app.route("/api/logout", methods=["POST"])
def logout():
    session.clear()
    return jsonify({"status": "ok"})

@app.route("/api/session")
def get_session():
    if session.get("logged_in"):
        return jsonify({"logged_in": True, "user": session.get("user"), "role": session.get("role")})
    return jsonify({"logged_in": False})

@app.route("/api/logs/stats")
def get_log_stats():
    """Return DB size, row count, oldest/newest record."""
    try:
        con = sqlite3.connect(DB_PATH)
        cur = con.cursor()
        cur.execute("SELECT COUNT(*), MIN(ts), MAX(ts) FROM telemetry")
        count, oldest, newest = cur.fetchone()
        con.close()
        size_mb = os.path.getsize(DB_PATH) / 1_048_576
        return jsonify({
            "rows":    count,
            "oldest":  oldest,
            "newest":  newest,
            "size_mb": round(size_mb, 2)
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/chart")
def get_chart_data():
    """
    Returns downsampled time-series data for charts.
    Query params:
      ?from=2025-01-01T00:00:00
      ?to=2025-01-02T00:00:00
      ?type=temperature|storage|position   (default: temperature)
      ?points=200                  (max data points, default 200, max 1000)
    Uses SQLite's modulo trick to downsample evenly across the range.
    """
    from_ts = request.args.get("from")
    to_ts   = request.args.get("to")
    chart_type = request.args.get("type", "temperature")
    max_pts = min(int(request.args.get("points", 200)), 1000)
    if not from_ts:
        from_ts = datetime.fromtimestamp(time.time() - 3600).isoformat(timespec='seconds')
    if not to_ts:
        to_ts = datetime.now().isoformat(timespec='seconds')
    if chart_type == "temperature":
        cols = "ts, tc_1, tc_2, tc_3, tc_4, ds_temp"
    elif chart_type == "storage":
        cols = "ts, tc_5, tc_6, tc_7, tc_8, tc_9"
    else:
        cols = "ts, cur_az, cur_el, az_actual, el_actual"
    try:
        con = sqlite3.connect(DB_PATH)
        con.row_factory = sqlite3.Row
        cur = con.cursor()

        # Count rows in range first so we can compute step for downsampling
        cur.execute(
            "SELECT COUNT(*) FROM telemetry WHERE ts BETWEEN ? AND ?",
            (from_ts, to_ts)
        )
        total = cur.fetchone()[0]
        if total == 0:
            con.close()
            return jsonify({"count": 0, "rows": []})

        # Downsample: pick every Nth row so we return at most max_pts points
        step = max(1, total // max_pts)
        cur.execute(
            f"""
            SELECT {cols} FROM (
                SELECT {cols}, ROW_NUMBER() OVER (ORDER BY ts) AS rn
                FROM telemetry
                WHERE ts BETWEEN ? AND ?
            ) WHERE rn % ? = 0
            ORDER BY ts ASC
            """,
            (from_ts, to_ts, step)
        )
        rows = [dict(r) for r in cur.fetchall()]
        con.close()
        return jsonify({"count": len(rows), "total": total, "step": step, "rows": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@app.route("/api/control", methods=["POST"])
@limiter.limit("30 per minute")
def control_actuator():
    global _commanded_mode
    try:
        data   = request.get_json(force=True) or {}
        action = data.get("action")
        cmd    = data.get("cmd")
        button_map = {
            "up":        {"cmd": "el_jog",   "dir":  1},
            "down":      {"cmd": "el_jog",   "dir": -1},
            "el_stop":   {"cmd": "el_jog",   "dir":  0},
            "left":      {"cmd": "az_jog",   "dir": -1},
            "right":     {"cmd": "az_jog",   "dir":  1},
            "az_stop":   {"cmd": "az_jog",   "dir":  0},
            "manual_on": {"cmd": "set_mode", "val": "manual"},
            "manual_off":{"cmd": "set_mode", "val": "auto"},
        }
        if action in button_map:
            pico_cmd = button_map[action]
            send_to_pico(pico_cmd)
            if pico_cmd.get("cmd") == "set_mode":
                _commanded_mode = pico_cmd.get("val", _commanded_mode)
                print(f"🔧 _commanded_mode → {_commanded_mode}")
            return jsonify({"status": "ok", "dispatched": pico_cmd})
        if cmd in ("el_jog", "az_jog"):
            dir_val = data.get("dir")
            if dir_val is None:
                return jsonify({"error": "Missing dir"}), 400
            try:
                dir_val = int(dir_val)
            except (ValueError, TypeError):
                return jsonify({"error": "Invalid dir"}), 400
            if dir_val not in (-1, 0, 1):
                return jsonify({"error": "dir must be -1, 0, or 1"}), 400
            send_to_pico({"cmd": cmd, "dir": dir_val})
            return jsonify({"status": "ok", "dispatched": {"cmd": cmd, "dir": dir_val}})
        if cmd == "set_mode":
            val = data.get("val")
            if val not in ("auto", "manual"):
                return jsonify({"error": "val must be 'auto' or 'manual'"}), 400
            send_to_pico({"cmd": "set_mode", "val": val})
            _commanded_mode = val
            print(f"🔧 _commanded_mode → {_commanded_mode}")
            return jsonify({"status": "ok"})
        if cmd == "apply":
            send_to_pico({"cmd": "apply"})
            _commanded_mode = "auto"
            print(f"🔧 _commanded_mode → auto (apply sent)")
            return jsonify({"status": "ok"})
        return jsonify({"error": "Unknown action or command"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route("/api/update", methods=["POST"])
@limiter.limit("20 per minute")
def update_settings():
    global _commanded_mode
    try:
        data = request.get_json(force=True) or {}
        print(f"🔍 /api/update called. _commanded_mode={_commanded_mode}")
        if _commanded_mode != "manual":
            return jsonify({"error": "Pico is not in MANUAL mode."}), 409

        errors = []
        if "rtc_date" in data and "rtc_time" in data:
            try:
                date_s = data["rtc_date"]
                time_s = data["rtc_time"]
                y, mo, d = map(int, date_s.split("-"))
                parts = list(map(int, time_s.split(":")))
                h, m, s = parts[0], parts[1], (parts[2] if len(parts) > 2 else 0)
                send_to_pico({"cmd": "set_rtc",
                               "h": h, "m": m, "s": s,
                               "d": d, "mo": mo, "y": y})
                _push_notif(f"RTC set to {date_s} {time_s}", "success")
            except Exception as e:
                errors.append(f"RTC: {e}")
        if "lat" in data and "lon" in data:
            try:
                tz = float(data.get("tz", 5.5))
                send_to_pico({"cmd": "set_gps",
                               "lat": float(data["lat"]),
                               "lon": float(data["lon"]),
                               "tz":  tz})
                _push_notif("GPS coordinates updated", "success")
            except Exception as e:
                errors.append(f"GPS: {e}")
        if "wind_limit" in data:
            try:
                send_to_pico({"cmd": "set_wind",
                               "thr": float(data["wind_limit"])})
                _push_notif(f"Wind limit set to {data['wind_limit']} m/s", "info")
            except Exception as e:
                errors.append(f"Wind: {e}")
        if "sunrise" in data and "sunset" in data:
            try:
                def _hh(t): return int(t.split(":")[0])
                send_to_pico({"cmd": "set_day",
                               "start": _hh(data["sunrise"]),
                               "end":   _hh(data["sunset"])})
                _push_notif("Day window updated", "info")
            except Exception as e:
                errors.append(f"Sun: {e}")
        if "calib" in data:
            try:
                c = data["calib"]
                send_to_pico({
                 "cmd":    "set_calib",
                    "min_a":  float(c["min_a"]),
                    "max_a":  float(c["max_a"]),
                    "home_a": float(c["home_a"]),
                    "stroke": float(c["stroke"]),
                    "ppm":    float(c["ppm"]),
                    "ppr":    int(c["ppr"]),
                    "gear":   int(c["gear"]),
                    "tgp":    int(c["tgp"]),
                })
                _push_notif("Calibration constants queued", "info")
            except Exception as e:
                errors.append(f"Calib: {e}")        
        if "mode" in data:
            val = data["mode"]
            if val in ("auto", "manual"):
                send_to_pico({"cmd": "set_mode", "val": val})
                _commanded_mode = val
            else:
                errors.append("mode must be 'auto' or 'manual'")
        if errors:
            return jsonify({"status": "partial", "errors": errors}), 207
        return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"error": str(e)}), 400
today_log = []

def _snapshot_thread():
    while True:
        if telemetry:
            today_log.append({
                "time":   datetime.now().strftime("%H:%M:%S"),
                "tc_avg": telemetry.get("tc_avg",    0),
                "az":     telemetry.get("cur_az",    0),
                "el":     telemetry.get("cur_el",    0),
                "wind":   telemetry.get("wind_speed",0),})
            if len(today_log) > 86400:
                today_log.pop(0)
        time.sleep(1)
if __name__ == "__main__":
    db_init()
    threading.Thread(target=connect_pico,     daemon=True).start()
    threading.Thread(target=read_pico_thread, daemon=True).start()
    threading.Thread(target=db_writer_thread, daemon=True).start()
    threading.Thread(target=_snapshot_thread, daemon=True).start()

    print("🚀 Solar Tracker SCADA Online")
    print("📍 dashboard: http://127.0.0.1:5000")
    app.run(host="0.0.0.0", port=5000)