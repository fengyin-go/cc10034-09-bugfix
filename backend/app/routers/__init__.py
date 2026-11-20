"""业务路由汇总：管道运营接口在前，通用资源路由在后。"""
from __future__ import annotations

from app.routers import ops, resource

ROUTERS = [ops, resource]
