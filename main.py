"""服务入口：``uv run fastapi dev main.py`` 或 ``uv run uvicorn main:app``。"""

from app.service import create_app

app = create_app()
