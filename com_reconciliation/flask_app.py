"""
Lightweight Flask front-end for the financial data reconciliation system.
(Replacement for streamlit_app.py - same workflow, same features, far less RAM.)

Run:
    pip install flask
    python flask_app.py            # http://localhost:5000

Low-memory design:
  * No pandas / pyarrow / Streamlit in the UI layer; server-rendered HTML only.
  * Uploads are streamed to disk and deleted right after parsing.
  * Per-browser-session private SQLite DB (temp dir), idle sessions auto-evicted,
    and the number of live sessions is capped.
  * Tables show the first MAX_SHOW rows; the CSV downloads contain everything.
"""

import csv
import gc
import io
import json
import shutil
import sys
import tempfile
import threading
import time
import traceback
from pathlib import Path
from uuid import uuid4

from flask import (Flask, Response, abort, make_response, redirect,
                   render_template_string, request, url_for)

PROJECT_ROOT = Path(__file__).resolve().parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from models.database import create_db_engine  # noqa: E402
from services.reconciliation import DataLoader, ReconciliationService  # noqa: E402
from utils.parsers import MEMBER_COLUMNS, TRANSACTION_COLUMNS  # noqa: E402

ALLOWED_EXT = {".csv", ".xlsx", ".xls"}
MAX_SHOW = 300            # max rows rendered per table (downloads are complete)
MAX_SESSIONS = 5          # cap live sessions (each owns a SQLite DB + engine)
IDLE_SECONDS = 30 * 60    # evict sessions idle longer than this
MAX_UPLOAD_MB = 200

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = MAX_UPLOAD_MB * 1024 * 1024

# ---------------------------------------------------------------------------
# Column definitions (key -> label), same as the Streamlit version
# ---------------------------------------------------------------------------

TXN_SKIP_COLS = [
    ("reason", "Reason"), ("source", "File / Sheet"), ("transaction_id", "Transaction ID / UTR"),
    ("date", "Date"), ("amount", "Amount"), ("name", "Name"), ("member_ids", "Policy/Member ID(s)"),
    ("raw_member_cell", "Raw Policy/Member cell"), ("raw_amount", "Raw amount"),
    ("raw_description", "Raw description"),
]
MEMBER_SKIP_COLS = [
    ("reason", "Reason"), ("source", "File / Sheet"), ("member_id", "Member/Policy No"),
    ("member_name", "Member Name"), ("date", "Date"), ("credit", "Credit"), ("debit", "Debit"),
    ("raw_account_no", "Raw Account No"), ("raw_member_name", "Raw Member Name"),
    ("raw_date", "Raw Date"),
]
MATCHED_COLS = [
    ("transaction_id", "Transaction ID"), ("date", "Date"), ("amount", "Amount"),
    ("cr_dr", "Cr/Dr"), ("payment_type", "Payment Type"), ("member_id", "Member ID"),
    ("member_name", "Member Name"), ("branch", "Branch"), ("matched_on", "Matched on"),
]
PARTIAL_COLS = [
    ("transaction_id", "Transaction ID"), ("date", "Date"), ("payment_type", "Payment Type"),
    ("member_id", "Member ID"), ("member_name", "Member Name"), ("branch", "Branch"),
    ("issues", "Why it's a partial match"), ("expected", "Expected"), ("actual", "Actual"),
]
UNMATCHED_COLS = [
    ("transaction_id", "Transaction ID"), ("date", "Date"), ("payment_type", "Payment Type"),
    ("member_id", "Member ID (as read from the bank row - not found on the ledger)"),
    ("issues", "Why it didn't match"),
]

# download kind -> (session key, columns, file name)
DOWNLOADS = {
    "matched": ("matched", MATCHED_COLS, "reconciliation_matched.csv"),
    "partial": ("partial", PARTIAL_COLS, "reconciliation_partially_matched.csv"),
    "unmatched": ("unmatched", UNMATCHED_COLS, "reconciliation_unmatched.csv"),
    "txn_skipped": ("txn_skipped", TXN_SKIP_COLS, "transactions_skipped.csv"),
    "member_skipped": ("member_skipped", MEMBER_SKIP_COLS, "members_skipped.csv"),
}

# ---------------------------------------------------------------------------
# Per-browser session store
# ---------------------------------------------------------------------------

_SESSIONS = {}
_SESSIONS_LOCK = threading.Lock()


def _new_session():
    tmp_dir = Path(tempfile.mkdtemp(prefix="recon_session_"))
    database_url = f"sqlite:///{tmp_dir / 'reconciliation.db'}"
    engine = create_db_engine(database_url)
    return {
        "tmp_dir": tmp_dir,
        "engine": engine,
        "loader": DataLoader(database_url, engine=engine),
        "service": ReconciliationService(database_url, engine=engine),
        "lock": threading.RLock(),
        "last_seen": time.time(),
        "txn_summaries": [], "member_summaries": [],
        "txn_skipped": [], "member_skipped": [],
        "result": None, "matched": [], "partial": [], "unmatched": [],
        "messages": [],
    }


def _destroy_session(s):
    try:
        s["engine"].dispose()
    except Exception:
        pass
    shutil.rmtree(s["tmp_dir"], ignore_errors=True)


def _evict_locked():
    """Drop idle sessions, then the oldest ones beyond MAX_SESSIONS. Caller holds _SESSIONS_LOCK."""
    now = time.time()
    for sid in [k for k, v in _SESSIONS.items() if now - v["last_seen"] > IDLE_SECONDS]:
        _destroy_session(_SESSIONS.pop(sid))
    while len(_SESSIONS) >= MAX_SESSIONS:
        oldest = min(_SESSIONS, key=lambda k: _SESSIONS[k]["last_seen"])
        _destroy_session(_SESSIONS.pop(oldest))


def get_session(create=True):
    """Return (sid, session) for this browser; creates one when missing."""
    sid = request.cookies.get("sid")
    with _SESSIONS_LOCK:
        s = _SESSIONS.get(sid)
        if s is None:
            if not create:
                return None, None
            _evict_locked()
            sid = uuid4().hex
            s = _SESSIONS[sid] = _new_session()
        s["last_seen"] = time.time()
    return sid, s


def _with_cookie(resp, sid):
    resp.set_cookie("sid", sid, httponly=True, samesite="Lax")
    return resp


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _cell(value):
    if value is None:
        return ""
    if isinstance(value, (list, tuple)):
        return "; ".join(str(v) for v in value)
    return str(value)


def _rows_to_csv(rows, cols):
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow([k for k, _ in cols])
    for r in rows:
        writer.writerow([_cell(r.get(k)) for k, _ in cols])
    return buf.getvalue()


def _safe_remove(path):
    """Delete a temp file. On Windows the parser may still hold the file open,
    so retry briefly and otherwise leave it; the session temp dir is wiped later."""
    for _ in range(5):
        gc.collect()  # release any leaked file handles (e.g. unclosed Excel workbooks)
        try:
            path.unlink(missing_ok=True)
            return
        except OSError:
            time.sleep(0.2)


def _flash(s, kind, text):
    s["messages"].append((kind, text))


# ---------------------------------------------------------------------------
# Template
# ---------------------------------------------------------------------------

PAGE = """<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Financial Data Reconciliation</title>
<style>
*{box-sizing:border-box}
body{margin:0;font:14px/1.45 system-ui,Segoe UI,Arial,sans-serif;color:#222;background:#f6f7f9}
.wrap{display:flex;min-height:100vh}
aside{width:250px;background:#fff;border-right:1px solid #ddd;padding:16px;flex:none}
main{flex:1;padding:20px 28px;min-width:0}
h1{margin:0 0 4px;font-size:26px} h2{margin:6px 0 8px;font-size:20px} h3{font-size:16px}
.cap{color:#666;font-size:13px}
.metric{margin:8px 0}.metric b{display:block;font-size:22px}
.metrics{display:flex;gap:12px;flex-wrap:wrap;margin:8px 0}
.metrics .metric{background:#fff;border:1px solid #ddd;border-radius:6px;padding:8px 14px;min-width:130px}
button,.btn{background:#e74c3c;color:#fff;border:0;border-radius:5px;padding:8px 14px;cursor:pointer;font-size:14px;text-decoration:none;display:inline-block}
button.sec,.btn.sec{background:#fff;color:#222;border:1px solid #bbb}
button:disabled{opacity:.5;cursor:not-allowed}
input[type=text]{padding:8px;border:1px solid #bbb;border-radius:5px;width:100%}
input[type=file]{margin:8px 0;display:block}
table{border-collapse:collapse;width:100%;background:#fff;font-size:13px}
th,td{border:1px solid #ddd;padding:5px 8px;text-align:left;vertical-align:top}
th{background:#f0f1f3;position:sticky;top:0}
.scroll{overflow:auto;max-height:420px;margin:6px 0}
details{background:#fff;border:1px solid #ddd;border-radius:6px;padding:8px 12px;margin:10px 0}
summary{cursor:pointer;font-weight:600}
.box{background:#fff;border:1px solid #ddd;border-radius:6px;padding:12px;margin:10px 0}
.msg{padding:8px 12px;border-radius:5px;margin:8px 0}
.msg.ok{background:#e8f8ee}.msg.warn{background:#fff6dc}.msg.err{background:#fdeaea}.msg.info{background:#e8f1fb}
hr{border:0;border-top:1px solid #ddd;margin:22px 0}
.bar{display:flex;align-items:center;margin:6px 0}.bar .l{width:150px}
.bar .t{flex:1;background:#eee;border-radius:4px;overflow:hidden}
.bar .f{color:#fff;padding:4px 8px;font-size:12px;min-width:24px}
.top{position:fixed;bottom:24px;right:24px;background:#e74c3c;color:#fff;border-radius:50%;width:46px;height:46px;display:flex;align-items:center;justify-content:center;font-size:20px;text-decoration:none;box-shadow:0 2px 8px rgba(0,0,0,.3)}
.row{display:flex;gap:8px;align-items:center}
code{background:#f0f1f3;padding:2px 4px;border-radius:3px;font-size:12px;word-break:break-word;display:block}
@media(max-width:800px){.wrap{flex-direction:column}aside{width:auto}}
</style></head><body>
<div class="wrap">
<aside>
  <h3>Session</h3>
  <div class="metric">Transaction files loaded<b>{{ s.txn_summaries|length }}</b></div>
  <div class="metric">Reference files loaded<b>{{ s.member_summaries|length }}</b></div>
  {% if s.result %}<div class="msg ok">Reconciliation has been run.</div>
  {% else %}<div class="msg info">Reconciliation not run yet.</div>{% endif %}
  <form method="post" action="{{ url_for('reset') }}"><button class="sec" style="width:100%">🗑️ New session (clear everything)</button></form>
  <details><summary>Expected file columns</summary>
    <p class="cap">Source/transaction files:</p><code>{{ txn_cols }}</code>
    <p class="cap">Reference/member files:</p><code>{{ member_cols }}</code>
    <p class="cap">Extra or missing columns are tolerated — only the columns above are used for matching.</p>
  </details>
</aside>

<main>
<div id="recon-top"></div>
<div class="row" style="justify-content:space-between">
  <div><h1>🔄 Financial Data Reconciliation</h1>
  <div class="cap">Upload your source data files, upload the reference data to check them against, then run reconciliation and review the results — all in one place.</div></div>
  {% if show_search %}<a class="btn sec" href="{{ url_for('index') }}">✖ Close search</a>
  {% else %}<a class="btn" href="{{ url_for('index', search=1) }}">🔍 Search</a>{% endif %}
</div>

{% for kind, text in messages %}<div class="msg {{ kind }}">{{ text }}</div>{% endfor %}

{% macro table(rows, cols) %}
  {% if not rows %}<div class="cap">No rows to show.</div>{% else %}
  <div class="scroll"><table><thead><tr>{% for k, label in cols %}<th>{{ label }}</th>{% endfor %}</tr></thead>
  <tbody>{% for r in rows[:max_show] %}<tr>{% for k, label in cols %}<td>{{ r.get(k)|cell }}</td>{% endfor %}</tr>{% endfor %}</tbody></table></div>
  {% if rows|length > max_show %}<div class="cap">Showing first {{ max_show }} of {{ rows|length }} rows — download the CSV for everything.</div>{% endif %}
  {% endif %}
{% endmacro %}

{% macro section(title, rows, cols, kind, empty, open) %}
<details {% if open %}open{% endif %}><summary>{{ title }} ({{ rows|length }} row(s))</summary>
  {% if not rows %}<div class="cap">{{ empty }}</div>{% else %}
    {{ table(rows, cols) }}
    <a class="btn sec" href="{{ url_for('download', kind=kind) }}">⬇️ Download {{ title.split(' ', 1)[-1] }} (CSV)</a>
  {% endif %}
</details>
{% endmacro %}

{% if show_search %}
<div class="box">
  <form method="get" class="row"><input type="hidden" name="search" value="1">
    <input type="text" name="q" value="{{ q }}" autofocus placeholder="🔍 Transaction No / UTR / RRN, member name, or policy number...">
    <button>Search</button></form>
  {% if not q %}<div class="cap">Start typing a transaction/UTR/RRN number, member name, or policy number.</div>
  {% elif not found %}<div class="msg warn">No matches found for <b>{{ q }}</b>.</div>
  {% else %}
    <div class="cap">Found <b>{{ found.transactions|length }}</b> transaction(s), <b>{{ found.members|length }}</b> ledger record(s), <b>{{ found.reconciliation_records|length }}</b> reconciliation result(s) matching <b>{{ q }}</b>.</div>
    {% if found.transactions %}<h3>Bank / UPI transactions</h3>{{ table(found.transactions, [
      ('transaction_id','Transaction ID / UTR'),('value_date','Date'),('transaction_amount','Amount'),
      ('cr_dr','Cr/Dr'),('name','Name'),('member_ids','Policy/Member ID(s)'),('source','Source')]) }}{% endif %}
    {% if found.members %}<h3>Ledger / member records</h3>{{ table(found.members, [
      ('member_id','Member/Policy No'),('tranx_id','Tranx ID'),('transaction_number','Transaction Number'),
      ('member_name','Member Name'),('branch','Branch'),('credit','Credit'),('debit','Debit'),('transaction_date','Date')]) }}{% endif %}
    {% if found.reconciliation_records %}<h3>Reconciliation results (from the last run)</h3>{{ table(found.reconciliation_records, [
      ('transaction_id','Transaction ID'),('date','Date'),('member_id','Member ID'),('member_name','Member Name'),
      ('amount','Amount'),('status','Status'),('matched_on','Matched on')]) }}{% endif %}
  {% endif %}
</div>
{% endif %}

<hr>
<h2>1️⃣ Upload source data files</h2>
<p>The data you want to reconcile — bank transfers, UPI payments, cash deposits, EMI, RD, FD exports, and so on. Upload as many as you have (e.g. 10).</p>
<form method="post" action="{{ url_for('upload', kind='txn') }}" enctype="multipart/form-data">
  <input type="file" name="files" multiple accept=".csv,.xlsx,.xls" required>
  <button>📥 Load transaction files</button>
</form>
{% if s.txn_summaries %}
  {{ table(s.txn_summaries, [('file','File'),('parsed','Rows found'),('inserted','Inserted'),('skipped','Skipped / duplicate'),('error','Error')]) }}
  <div class="cap"><b>{{ s.txn_summaries|sum(attribute='inserted') }}</b> transaction row(s) inserted so far across all files.</div>
  {{ section('⏭️ Skipped rows', s.txn_skipped, txn_skip_cols, 'txn_skipped', 'Nothing skipped — every parsed row was either inserted or already covered above.', false) }}
{% endif %}

<hr>
<h2>2️⃣ Upload reference data files</h2>
<p>The data to reconcile <i>against</i> — your member / policy master records. Usually a smaller set (e.g. 2-3 files).</p>
<form method="post" action="{{ url_for('upload', kind='member') }}" enctype="multipart/form-data">
  <input type="file" name="files" multiple accept=".csv,.xlsx,.xls" required>
  <button>📥 Load reference files</button>
</form>
{% if s.member_summaries %}
  {{ table(s.member_summaries, [('file','File'),('parsed','Rows found'),('inserted','Inserted'),('skipped','Skipped / duplicate'),('error','Error')]) }}
  <div class="cap"><b>{{ s.member_summaries|sum(attribute='inserted') }}</b> reference record(s) inserted so far across all files.</div>
  {{ section('⏭️ Skipped rows', s.member_skipped, member_skip_cols, 'member_skipped', 'Nothing skipped — every parsed row was either inserted or already covered above.', false) }}
{% endif %}

<hr>
<h2>3️⃣ Run reconciliation</h2>
{% if dr.count == 0 %}<div class="msg warn">Load at least one transaction file above before running reconciliation.</div>
{% else %}<p>Found transaction data across <b>{{ dr.count }}</b> date(s) ({{ span }}). Running reconciliation covers every one of them, not just today.</p>{% endif %}
<form method="post" action="{{ url_for('run') }}"><button {% if dr.count == 0 %}disabled{% endif %}>🚀 Run reconciliation</button></form>

{% if res %}
  {% if res.status == 'no_data' %}<div class="msg warn">{{ res.message }}</div>
  {% else %}
    {% set sm = res.summary %}
    <h3>Summary</h3>
    <div class="metrics">
      <div class="metric">Total processed<b>{{ sm.total_processed }}</b></div>
      <div class="metric">Matched<b>{{ sm.matched }}</b></div>
      <div class="metric">Partially matched<b>{{ sm.partially_matched }}</b></div>
      <div class="metric">Unmatched<b>{{ sm.unmatched }}</b></div>
      <div class="metric">Match rate<b>{{ sm.match_rate }}%</b></div>
      <div class="metric">Discrepancy amount (₹)<b>{{ '{:,.2f}'.format(sm.discrepancy_amount) }}</b></div>
    </div>
    {% set mx = [sm.matched, sm.partially_matched, sm.unmatched]|max or 1 %}
    {% for label, n, color in [('Matched', sm.matched, '#2ecc71'), ('Partially matched', sm.partially_matched, '#f1c40f'), ('Unmatched', sm.unmatched, '#e74c3c')] %}
      <div class="bar"><div class="l">{{ label }}</div><div class="t"><div class="f" style="width:{{ (n / mx * 100)|round(1) }}%;background:{{ color }}">{{ n }}</div></div></div>
    {% endfor %}

    <details><summary>Per-date breakdown ({{ res.dates_processed|length }} date(s))</summary>
      {{ table(daily, [('date','Date'),('status','Status'),('total','Total'),('matched','Matched'),('partial','Partially matched'),('unmatched','Unmatched'),('rate','Match rate %')]) }}
    </details>

    <h3>Results by status</h3>
    {{ section('✅ Matched', s.matched, matched_cols, 'matched', 'No transactions matched cleanly.', false) }}
    {{ section('🟡 Partially matched', s.partial, partial_cols, 'partial', 'No partially-matched transactions.', true) }}
    {{ section('🔴 Unmatched', s.unmatched, unmatched_cols, 'unmatched', 'No unmatched transactions.', true) }}

    <h3>Download full report</h3>
    <a class="btn sec" href="{{ url_for('download', kind='report') }}">⬇️ Full report (JSON) — all statuses, all fields</a>
  {% endif %}
{% endif %}
</main></div>
<a class="top" href="#recon-top" title="Back to top">⬆️</a>
</body></html>
"""


@app.template_filter("cell")
def _cell_filter(value):
    return _cell(value)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    sid, s = get_session()
    with s["lock"]:
        messages, s["messages"] = s["messages"], []
        show_search = request.args.get("search") == "1"
        q = (request.args.get("q") or "").strip()
        found = None
        if show_search and q:
            r = s["service"].search_all(q)
            if r["transactions"] or r["members"] or r["reconciliation_records"]:
                found = r

        dr = s["service"].get_transaction_date_range()
        span = (dr["min_date"] if dr["min_date"] == dr["max_date"]
                else f"{dr['min_date']} to {dr['max_date']}") if dr["count"] else ""

        res = s["result"]
        daily = []
        if res and res.get("status") != "no_data":
            for d in res.get("daily_results", []):
                r = d.get("results", {})
                daily.append({
                    "date": d.get("date"), "status": d.get("status"),
                    "total": r.get("total_processed", 0), "matched": r.get("matched", 0),
                    "partial": r.get("partially_matched", 0), "unmatched": r.get("unmatched", 0),
                    "rate": r.get("match_rate", 0.0),
                })

        html = render_template_string(
            PAGE, s=s, messages=messages, show_search=show_search, q=q, found=found,
            dr=dr, span=span, res=res, daily=daily, max_show=MAX_SHOW,
            txn_cols=", ".join(TRANSACTION_COLUMNS), member_cols=", ".join(MEMBER_COLUMNS),
            txn_skip_cols=TXN_SKIP_COLS, member_skip_cols=MEMBER_SKIP_COLS,
            matched_cols=MATCHED_COLS, partial_cols=PARTIAL_COLS, unmatched_cols=UNMATCHED_COLS,
        )
    return _with_cookie(make_response(html), sid)


@app.route("/upload/<kind>", methods=["POST"])
def upload(kind):
    if kind not in ("txn", "member"):
        abort(404)
    sid, s = get_session()
    files = [f for f in request.files.getlist("files") if f.filename]
    if not files:
        _flash(s, "warn", "No files selected.")
        return _with_cookie(redirect(url_for("index")), sid)

    with s["lock"]:
        load = s["loader"].load_transactions_from_file if kind == "txn" \
            else s["loader"].load_members_from_file
        summaries = s["txn_summaries" if kind == "txn" else "member_summaries"]
        skipped = s["txn_skipped" if kind == "txn" else "member_skipped"]

        for f in files:
            suffix = Path(f.filename).suffix.lower()
            if suffix not in ALLOWED_EXT:
                summaries.append({"file": f.filename, "parsed": 0, "inserted": 0, "skipped": 0,
                                  "error": "Unsupported file type (use csv/xlsx/xls)"})
                continue
            # Keep the original file name in the temp path: the parsers may use it
            # (e.g. bank / UPI / cash / EMI / RD / FD) to detect the source type.
            stem = Path(f.filename).stem.replace(" ", "_")
            path = s["tmp_dir"] / f"upload_{uuid4().hex[:8]}_{stem}{Path(f.filename).suffix}"
            f.save(path)  # streamed to disk, not held in RAM
            try:
                summary = load(str(path))
            except Exception as exc:  # keep the app alive on a bad file
                traceback.print_exc()  # full error in the terminal
                summary = {"parsed": 0, "inserted": 0, "skipped": 0,
                           "error": f"{type(exc).__name__}: {exc}"}
            finally:
                _safe_remove(path)
            summary["file"] = f.filename
            skipped.extend(summary.pop("skipped_details", []) or [])
            summary["error"] = summary.get("error") or ""
            summaries.append(summary)

        s["result"], s["matched"], s["partial"], s["unmatched"] = None, [], [], []  # stale now
        _flash(s, "ok", f"Processed {len(files)} file(s).")
    return _with_cookie(redirect(url_for("index")), sid)


@app.route("/run", methods=["POST"])
def run():
    sid, s = get_session()
    with s["lock"]:
        try:
            result = s["service"].perform_full_reconciliation()
        except Exception as exc:
            _flash(s, "err", f"Reconciliation failed: {exc}")
            return _with_cookie(redirect(url_for("index")), sid)
        s["result"] = result
        details = result.get("details", []) if result.get("status") != "no_data" else []
        s["matched"] = result.get("matched_details", [])
        s["partial"] = [d for d in details if d.get("status") == "partially_matched"]
        s["unmatched"] = [d for d in details if d.get("status") == "unmatched"]
    return _with_cookie(redirect(url_for("index") + "#recon-results"), sid)


@app.route("/download/<kind>")
def download(kind):
    sid, s = get_session(create=False)
    if s is None:
        abort(404)
    with s["lock"]:
        if kind == "report":
            if s["result"] is None:
                abort(404)
            body, mime, name = json.dumps(s["result"], indent=2, default=str), \
                "application/json", "reconciliation_report.json"
        elif kind in DOWNLOADS:
            key, cols, name = DOWNLOADS[kind]
            body, mime = _rows_to_csv(s[key], cols), "text/csv"
        else:
            abort(404)
    return Response(body, mimetype=mime,
                    headers={"Content-Disposition": f'attachment; filename="{name}"'})


@app.route("/reset", methods=["POST"])
def reset():
    sid = request.cookies.get("sid")
    with _SESSIONS_LOCK:
        s = _SESSIONS.pop(sid, None)
    if s is not None:
        _destroy_session(s)
    return redirect(url_for("index"))


@app.errorhandler(413)
def too_large(_):
    return f"Upload too large (limit {MAX_UPLOAD_MB} MB per request).", 413


if __name__ == "__main__":
    # threaded dev server is fine for a handful of users; for production use:
    #   pip install waitress && waitress-serve --threads=4 flask_app:app
    app.run(host="0.0.0.0", port=5000, threaded=True, debug=False)