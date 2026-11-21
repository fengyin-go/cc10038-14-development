"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import resource, pipeline_router

# 流水线路由必须先注册：通用资源路由含 /api/{module}/{entry_id}，
# 会把 /api/pipeline/<子路径> 误当成模块 pipeline 的条目。
ROUTERS = [pipeline_router, resource]
