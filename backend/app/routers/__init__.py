"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。

序号17 的采样/调试复核接口固定在资源路由之前注册，路径都是静态前缀，
既不会被 /api/{module} 系路由抢先匹配，也不影响原有模块接口。
"""
from __future__ import annotations

from app.routers import resource, sampling

ROUTERS = [sampling, resource]
