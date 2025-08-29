import streamlit as st
import pandas as pd
import requests
import time
import random
import threading
import json
from io import StringIO
from datetime import datetime

# =========================
# Page & Styling
# =========================
st.set_page_config(page_title="LinkedIn Outreach Automation", layout="wide")
st.title("LinkedIn Outreach Automation (PhantomBuster)")
st.caption("Upload a CSV of LinkedIn profiles and automate outreach using PhantomBuster API.")

# Simple theme switch (CSS injection)
with st.sidebar:
    theme = st.radio("Theme", ["Light", "Dark"], index=1)
if theme == "Dark":
    st.markdown("""
        <style>
        .stApp { background:#0E1117; color:#FAFAFA; }
        .stButton>button { border-radius:12px; }
        </style>
    """, unsafe_allow_html=True)

# =========================
# Sidebar – Config
# =========================
st.sidebar.header("Configuration")
api_key = st.sidebar.text_input("PhantomBuster API Key", type="password")
agent_id = st.sidebar.text_input("Agent ID", value="2222814165510688")

st.sidebar.subheader("Working Hours (Mon–Fri)")
start_hour = st.sidebar.number_input("Start Hour (24h)", value=9, min_value=0, max_value=23)
end_hour = st.sidebar.number_input("End Hour (24h)", value=17, min_value=0, max_value=23)

st.sidebar.subheader("Delay Settings")
min_delay = st.sidebar.number_input("Min Delay (sec)", value=45, min_value=1)
max_delay = st.sidebar.number_input("Max Delay (sec)", value=120, min_value=1)
extended_break_chance = st.sidebar.slider("Extended Break Chance (%)", 0, 100, 15)
extended_break_min = st.sidebar.number_input("Extended Break Min (sec)", value=300, min_value=1)
extended_break_max = st.sidebar.number_input("Extended Break Max (sec)", value=600, min_value=1)

st.sidebar.divider()
st.sidebar.caption("Tip: Keep delays human-like to respect platform limits.")

# =========================
# Session State Init
# =========================
def _init_state():
    ss = st.session_state
    ss.setdefault("df", None)
    ss.setdefault("processed_profiles", set())
    ss.setdefault("logs", [])  # list of dicts for dataframe
    ss.setdefault("is_running", False)
    ss.setdefault("is_paused", False)
    ss.setdefault("is_stopped", False)
    ss.setdefault("thread", None)
    ss.setdefault("start_time", None)
    ss.setdefault("completed", 0)
    ss.setdefault("total", 0)
    ss.setdefault("avg_secs", None)
    ss.setdefault("lock", threading.Lock())
    ss.setdefault("pause_event", threading.Event())
    ss.setdefault("stop_event", threading.Event())
    # pause_event semantics: set() means RUNNING; clear() means PAUSED
    ss.pause_event.set()

_init_state()

# =========================
# Helpers
# =========================
PROCESSED_FILE = "processed_profiles.json"

def load_processed_profiles_from_disk():
    try:
        with open(PROCESSED_FILE, "r") as f:
            return set(json.load(f))
    except FileNotFoundError:
        return set()
    except Exception:
        return set()

def save_processed_profiles_to_disk(s: set):
    try:
        with open(PROCESSED_FILE, "w") as f:
            json.dump(list(s), f)
    except Exception:
        pass

def is_within_working_hours(now: datetime) -> bool:
    # Monday (0) .. Friday (4)
    if now.weekday() >= 5:
        return False
    return start_hour <= now.hour < end_hour

def launch_phantom(api_key: str, agent_id: str, profile_url: str, message: str):
    url = f"https://api.phantombuster.com/api/v2/agents/{agent_id}/launch"
    headers = {
        "Content-Type": "application/json",
        "X-Phantombuster-Key-1": api_key,
    }
    payload = {
        "arguments": {
            "specificProfileUrl": profile_url,
            "message": message,
            "delay": random.randint(3, 8),
            "maxRequestsPerDay": 20,
            "randomizeDelay": True,
        }
    }
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=30)
        if resp.status_code == 200:
            return {"ok": True, "data": resp.json()}
        return {"ok": False, "error": f"API Error: {resp.status_code} - {resp.text}"}
    except requests.exceptions.RequestException as e:
        return {"ok": False, "error": f"Network error: {str(e)}"}

def add_log(row: dict):
    with st.session_state.lock:
        st.session_state.logs.append(row)

def compute_eta():
    ss = st.session_state
    remaining = max(ss.total - ss.completed, 0)
    if ss.avg_secs is None or ss.avg_secs <= 0:
        return None, remaining
    seconds_left = int(remaining * ss.avg_secs)
    return seconds_left, remaining

def secs_to_hms(sec: int):
    h = sec // 3600
    m = (sec % 3600) // 60
    s = sec % 60
    if h > 0:
        return f"{h}h {m}m {s}s"
    if m > 0:
        return f"{m}m {s}s"
    return f"{s}s"

# =========================
# Upload CSV
# =========================
st.subheader("1) Upload CSV")
uploaded_file = st.file_uploader(" Upload CSV with columns: profileUrl, message", type=["csv"])

if uploaded_file:
    try:
        df = pd.read_csv(uploaded_file)
    except Exception as e:
        st.error(f"Could not read CSV: {e}")
        df = None

    if df is not None:
        missing = [c for c in ("profileUrl", "message") if c not in df.columns]
        if missing:
            st.error(f"CSV is missing required columns: {missing}")
        else:
            st.session_state.df = df.copy()
            st.success(f"Loaded {len(df)} rows.")
            st.dataframe(df.head(10), use_container_width=True)

# Load processed profiles from disk once per session (optional safety)
if not st.session_state.processed_profiles:
    st.session_state.processed_profiles = load_processed_profiles_from_disk()

# =========================
# Controls
# =========================
st.subheader("2) Controls")

c1, c2, c3, c4, c5 = st.columns([1,1,1,1,2])

def start():
    ss = st.session_state
    if ss.df is None:
        st.warning("Please upload a CSV first.")
        return
    if not api_key or not agent_id:
        st.warning("Please provide API Key & Agent ID in the sidebar.")
        return
    if ss.is_running:
        st.info("Already running.")
        return

    # Prepare queue
    df = ss.df
    unprocessed_df = df[~df["profileUrl"].isin(ss.processed_profiles)]
    ss.total = len(unprocessed_df)
    ss.completed = 0
    ss.logs = []
    ss.is_stopped = False
    ss.stop_event.clear()
    ss.pause_event.set()  # start in running state
    ss.is_paused = False
    ss.is_running = True
    ss.start_time = time.time()
    ss.avg_secs = None

    # Start worker thread
    def worker(rows):
        for _, row in rows.iterrows():
            if ss.stop_event.is_set():
                break

            # Pause handling
            while not ss.pause_event.is_set():
                time.sleep(0.2)
                if ss.stop_event.is_set():
                    break
            if ss.stop_event.is_set():
                break

            # Working hours check
            now = datetime.now()
            if not is_within_working_hours(now):
                add_log({
                    "time": now.isoformat(timespec="seconds"),
                    "profileUrl": None,
                    "status": "WAIT",
                    "details": "Outside working hours. Sleeping 1 hour.",
                    "elapsed_sec": None,
                })
                # sleep in small chunks to remain responsive to stop/pause
                for _ in range(60 * 60 // 2):  # 1 hour in 0.5s steps
                    if ss.stop_event.is_set():
                        break
                    while not ss.pause_event.is_set():
                        time.sleep(0.2)
                        if ss.stop_event.is_set():
                            break
                    time.sleep(0.5)
                if ss.stop_event.is_set():
                    break

            profile_url = str(row["profileUrl"])
            message = str(row["message"])
            started = time.time()
            add_log({
                "time": datetime.now().isoformat(timespec="seconds"),
                "profileUrl": profile_url,
                "status": "START",
                "details": "Launching phantom…",
                "elapsed_sec": None,
            })

            res = launch_phantom(api_key, agent_id, profile_url, message)
            elapsed = time.time() - started

            if res["ok"]:
                container_id = res["data"].get("containerId")
                add_log({
                    "time": datetime.now().isoformat(timespec="seconds"),
                    "profileUrl": profile_url,
                    "status": "SUCCESS",
                    "details": f"Container ID: {container_id}",
                    "elapsed_sec": round(elapsed, 2),
                })
                with ss.lock:
                    ss.processed_profiles.add(profile_url)
                    save_processed_profiles_to_disk(ss.processed_profiles)
            else:
                add_log({
                    "time": datetime.now().isoformat(timespec="seconds"),
                    "profileUrl": profile_url,
                    "status": "ERROR",
                    "details": res["error"],
                    "elapsed_sec": round(elapsed, 2),
                })

            # Update counters and avg
            with ss.lock:
                ss.completed += 1
                # compute rolling average time per profile
                finished = [r for r in ss.logs if r["status"] in ("SUCCESS", "ERROR") and r["elapsed_sec"] is not None]
                if finished:
                    ss.avg_secs = sum(r["elapsed_sec"] for r in finished) / len(finished)

            # Delay simulation w/ occasional extended break
            delay = random.uniform(min_delay, max_delay)
            if random.random() < (extended_break_chance / 100.0):
                delay += random.uniform(extended_break_min, extended_break_max)
                add_log({
                    "time": datetime.now().isoformat(timespec="seconds"),
                    "profileUrl": None,
                    "status": "INFO",
                    "details": f"Extended break: ~{round(delay/60,1)} min",
                    "elapsed_sec": None,
                })

            # Sleep in small chunks so pause/stop stays responsive
            slept = 0.0
            step = 0.25
            while slept < delay:
                if ss.stop_event.is_set():
                    break
                while not ss.pause_event.is_set():
                    time.sleep(0.2)
                    if ss.stop_event.is_set():
                        break
                if ss.stop_event.is_set():
                    break
                time.sleep(step)
                slept += step
            if ss.stop_event.is_set():
                break

        # mark finished
        ss.is_running = False

    st.session_state.thread = threading.Thread(target=worker, args=(unprocessed_df,), daemon=True)
    st.session_state.thread.start()

def pause():
    ss = st.session_state
    if ss.is_running and not ss.is_paused:
        ss.is_paused = True
        ss.pause_event.clear()

def resume():
    ss = st.session_state
    if ss.is_running and ss.is_paused:
        ss.is_paused = False
        ss.pause_event.set()

def stop():
    ss = st.session_state
    if ss.is_running:
        ss.is_stopped = True
        ss.stop_event.set()
        ss.pause_event.set()
        ss.is_paused = False
        ss.is_running = False

with c1:
    st.button("▶ Start", use_container_width=True, on_click=start)
with c2:
    st.button("⏸ Pause", use_container_width=True, on_click=pause, disabled=not st.session_state.is_running or st.session_state.is_paused)
with c3:
    st.button("⏯ Resume", use_container_width=True, on_click=resume, disabled=not st.session_state.is_running or not st.session_state.is_paused)
with c4:
    st.button("⏹ Stop", use_container_width=True, on_click=stop, disabled=not st.session_state.is_running)

with c5:
    st.metric("Processed", f"{st.session_state.completed} / {st.session_state.total}")

# =========================
# Progress, ETA, Status
# =========================
st.subheader("3) Status")
progress = 0.0 if st.session_state.total == 0 else st.session_state.completed / st.session_state.total
st.progress(progress)

eta_sec, remaining = compute_eta()
left_col, right_col, third_col = st.columns(3)
left_col.metric("Remaining", remaining)
right_col.metric("Avg Time/Profile", f"{st.session_state.avg_secs:.1f}s" if st.session_state.avg_secs else "—")
third_col.metric("ETA", secs_to_hms(eta_sec) if eta_sec is not None else "—")

status = "Running" if st.session_state.is_running and not st.session_state.is_paused else \
         "Paused" if st.session_state.is_running and st.session_state.is_paused else \
         "Stopped" if st.session_state.is_stopped else "Idle"
st.info(f"Status: **{status}**")

# Auto-refresh the dashboard while running or paused so the table updates live
if st.session_state.is_running or st.session_state.is_paused:
    st.autorefresh(interval=1500, key="auto_refresh_key")

# =========================
# Real-time Log Table
# =========================
st.subheader("4) Real-Time Logs")
if st.session_state.logs:
    df_logs = pd.DataFrame(st.session_state.logs)
    # Keep last 500 rows in view for speed
    st.dataframe(df_logs.tail(500), use_container_width=True, height=360)
else:
    st.caption("No logs yet. Click Start to begin.")

# =========================
# Download Logs
# =========================
st.subheader("5) Export")
def download_logs_button():
    if not st.session_state.logs:
        st.warning("No logs to download yet.")
        return
    df_logs = pd.DataFrame(st.session_state.logs)
    csv_buf = StringIO()
    df_logs.to_csv(csv_buf, index=False)
    st.download_button(
        " Download Logs (CSV)",
        data=csv_buf.getvalue(),
        file_name=f"outreach_logs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv",
        use_container_width=True,
    )

download_logs_button()

st.caption("Processed profile URLs are also persisted in `processed_profiles.json` to avoid duplicates across reruns in the same app instance.")
