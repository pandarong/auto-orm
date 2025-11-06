# 延迟导入或重新导出
from .core import DataEngine, MemoryStorage

# Condition 可以在这里定义或从 core 导入
# 由于 Condition 是简单的 dataclass，可以这样处理：
try:
    from .core import Condition
except ImportError:
    # 如果存在循环依赖问题，可以在这里定义
    from dataclasses import dataclass
    from typing import Any
    
    @dataclass
    class Condition:
        """查询条件"""
        field: str
        op: str
        value: Any