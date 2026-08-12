"""Client Input Service — Flask status dashboard fronting the synthetic
watch/rating event generator (generator.py, run as a background thread).
Login pattern is a direct reuse of Catalog Admin's (catalog-input/frontend/
app.py) — same shared-credential/session-cookie approach, for a consistent
security posture across both custom frontends.
"""
import hmac
import logging
import os
from datetime import timedelta
from functools import wraps

from flask import Flask, flash, jsonify, redirect, render_template, request, session, url_for
from flask_wtf import CSRFProtect
from flask_wtf.csrf import CSRFError

import generator
import state

logging.basicConfig(level=logging.INFO)

app = Flask(__name__)
app.secret_key = os.environ.get("CLIENT_INPUT_SECRET_KEY", "dev-secret-change-me")
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.permanent_session_lifetime = timedelta(hours=8)

csrf = CSRFProtect(app)

ADMIN_USERNAME = os.environ.get("CLIENT_INPUT_ADMIN_USERNAME", "admin")
ADMIN_PASSWORD = os.environ.get("CLIENT_INPUT_ADMIN_PASSWORD", "changeme")

state.init_db(
    generator.ARRIVAL_PROBABILITY, generator.MAX_ARRIVALS_PER_TICK, generator.SIMULATION_SPEED,
    generator.SESSION_MAX_ITEMS, generator.RATING_PROBABILITY,
    generator.ABANDON_PROBABILITY,
)
generator.start_generator_thread()


def parse_int(raw, default=None):
    raw = (raw or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def parse_float(raw, default=None):
    raw = (raw or "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def login_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if not session.get("authenticated"):
            return redirect(url_for("login", next=request.path))
        return view(*args, **kwargs)
    return wrapped


@app.errorhandler(CSRFError)
def handle_csrf_error(_e):
    flash("Your session expired — please sign in again.", "warning")
    return redirect(url_for("login"))


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username", "")
        password = request.form.get("password", "")
        if hmac.compare_digest(username, ADMIN_USERNAME) and hmac.compare_digest(password, ADMIN_PASSWORD):
            session.clear()
            session["authenticated"] = True
            session.permanent = True
            return redirect(request.args.get("next") or url_for("dashboard"))
        flash("Invalid username or password.", "danger")
    return render_template("login.html")


@app.route("/logout", methods=["POST"])
def logout():
    session.clear()
    flash("Signed out.", "success")
    return redirect(url_for("login"))


def _dashboard_state():
    gen = generator.get_generator()
    return {
        "counts": state.counts(),
        "active_sessions": [dict(row) for row in state.active_sessions()],
        "recent_finished": [dict(row) for row in state.recent_finished_items(limit=30)],
        "device_breakdown": state.device_breakdown(),
        "paused": state.is_paused(),
        "events_produced": gen.events_produced if gen else 0,
    }


@app.route("/")
@login_required
def dashboard():
    gen = generator.get_generator()
    return render_template(
        "dashboard.html",
        tick_seconds=generator.TICK_SECONDS,
        simulation_speed=state.get_simulation_speed(),
        arrival_probability=state.get_arrival_probability(),
        max_arrivals_per_tick=state.get_max_arrivals_per_tick(),
        session_max_items=state.get_session_max_items(),
        rating_probability=state.get_rating_probability(),
        abandon_probability=state.get_abandon_probability(),
        device_types=generator.DEVICE_TYPES,
        free_user_ids=gen.free_user_ids() if gen else [],
        item_ids=gen.item_ids() if gen else [],
        **_dashboard_state(),
    )


@app.route("/api/status")
@login_required
def api_status():
    return jsonify(_dashboard_state())


@app.route("/pause", methods=["POST"])
@login_required
def pause():
    state.set_paused(True)
    flash("Generator paused.", "warning")
    return redirect(url_for("dashboard"))


@app.route("/resume", methods=["POST"])
@login_required
def resume():
    state.set_paused(False)
    flash("Generator resumed.", "success")
    return redirect(url_for("dashboard"))


@app.route("/sessions/add", methods=["POST"])
@login_required
def sessions_add():
    count = parse_int(request.form.get("count"), default=1)
    if count is None or count < 1:
        flash("Session count must be a positive whole number.", "danger")
        return redirect(url_for("dashboard"))
    count = min(count, 100)
    gen = generator.get_generator()
    created = gen.open_sessions(count) if gen else 0
    if created:
        flash(f"Opened {created} new session(s).", "success")
    else:
        flash("Could not open sessions — catalog pool isn't populated yet, or every user is already active.", "warning")
    return redirect(url_for("dashboard"))


@app.route("/sessions/custom", methods=["POST"])
@login_required
def sessions_custom():
    # Every field is optional — whatever's left blank falls back to the same
    # random selection "Open sessions now" uses (see Generator.open_custom_session).
    user_id = request.form.get("user_id", "").strip() or None
    item_id = request.form.get("item_id", "").strip() or None
    device_type = request.form.get("device_type", "").strip() or None
    outcome = request.form.get("outcome", "").strip() or None
    max_items = parse_int(request.form.get("max_items"))
    gen = generator.get_generator()
    if not gen:
        flash("Generator isn't running yet.", "danger")
        return redirect(url_for("dashboard"))
    created_user_id, error = gen.open_custom_session(
        user_id=user_id, item_id=item_id, device_type=device_type,
        outcome=outcome, max_items=max_items,
    )
    if error:
        flash(error, "danger")
    else:
        flash(f"Custom session opened for {created_user_id}.", "success")
    return redirect(url_for("dashboard"))


@app.route("/sessions/<user_id>/end", methods=["POST"])
@login_required
def sessions_end(user_id):
    gen = generator.get_generator()
    ok = gen.end_session_now(user_id) if gen else False
    return jsonify({"ok": ok, "user_id": user_id})


@app.route("/sessions/end_bulk", methods=["POST"])
@login_required
def sessions_end_bulk():
    # Multiselect "end session" from the dashboard — same single-session
    # close as /sessions/<user_id>/end, just looped. Best-effort: a user_id
    # that raced to close on its own between the checkbox render and this
    # request is simply skipped, not an error.
    user_ids = request.form.getlist("user_ids")
    gen = generator.get_generator()
    ended = [uid for uid in user_ids if gen and gen.end_session_now(uid)]
    return jsonify({"ended": ended, "requested": len(user_ids)})


@app.route("/config", methods=["POST"])
@login_required
def update_config():
    probability = parse_float(request.form.get("arrival_probability"))
    max_arrivals = parse_int(request.form.get("max_arrivals_per_tick"))
    sim_speed = parse_int(request.form.get("simulation_speed"))
    session_max_items = parse_int(request.form.get("session_max_items"))
    rating_probability = parse_float(request.form.get("rating_probability"))
    abandon_probability = parse_float(request.form.get("abandon_probability"))
    if probability is None or not (0 <= probability <= 1):
        flash("Arrival probability must be a number between 0 and 1.", "danger")
        return redirect(url_for("dashboard"))
    if max_arrivals is None or max_arrivals < 0:
        flash("New active users per tick must be a non-negative whole number.", "danger")
        return redirect(url_for("dashboard"))
    if sim_speed is None or sim_speed < 1:
        flash("Simulated seconds per tick must be a positive whole number.", "danger")
        return redirect(url_for("dashboard"))
    if session_max_items is None or session_max_items < 1:
        flash("Max items per session must be a positive whole number.", "danger")
        return redirect(url_for("dashboard"))
    if rating_probability is None or not (0 <= rating_probability <= 1):
        flash("Rating probability must be a number between 0 and 1.", "danger")
        return redirect(url_for("dashboard"))
    if abandon_probability is None or not (0 <= abandon_probability <= 1):
        flash("Abandon probability must be a number between 0 and 1.", "danger")
        return redirect(url_for("dashboard"))
    state.set_arrival_probability(probability)
    state.set_max_arrivals_per_tick(max_arrivals)
    state.set_simulation_speed(sim_speed)
    state.set_session_max_items(session_max_items)
    state.set_rating_probability(rating_probability)
    state.set_abandon_probability(abandon_probability)
    flash("Generator config updated.", "success")
    return redirect(url_for("dashboard"))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)
