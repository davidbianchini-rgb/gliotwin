from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from app.db import db, init_db, rows_as_dicts
from app.routers import files, imports, patients, sessions, workspace, rhglioseg

STATIC_DIR = Path(__file__).parent / "static"

BROWSABLE_TABLES = {
    "subjects",
    "sessions",
    "sequences",
    "radiological_structures",
    "computed_structures",
    "clinical_events",
    "subject_external_refs",
    "radiotherapy_courses",
    "signal_metric_cache",
    "signal_metric_jobs",
}


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(title="GlioTwin", version="0.1.0", docs_url="/api/docs", lifespan=lifespan)

app.include_router(patients.router, prefix="/api")
app.include_router(sessions.router, prefix="/api")
app.include_router(files.router,    prefix="/api")
app.include_router(imports.router,  prefix="/api")
app.include_router(workspace.router,  prefix="/api")
app.include_router(rhglioseg.router,  prefix="/api")

app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/api/db/{table}")
def browse_table(table: str, limit: int = 200, offset: int = 0):
    """Generic table browser used by the Database view."""
    if table not in BROWSABLE_TABLES:
        raise HTTPException(400, f"Table '{table}' not browsable")
    with db() as conn:
        total = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        rows  = conn.execute(
            f"SELECT * FROM {table} LIMIT ? OFFSET ?", (limit, offset)
        ).fetchall()
    return {"table": table, "total": total, "rows": rows_as_dicts(rows)}


@app.get("/", include_in_schema=False)
def root():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/{full_path:path}", include_in_schema=False)
def spa_fallback(full_path: str):
    """Return index.html for all non-API paths (SPA client-side routing)."""
    return FileResponse(STATIC_DIR / "index.html")
