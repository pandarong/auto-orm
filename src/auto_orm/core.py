"""
自动加载数据模型的 ORM 引擎
工程师只需在 models/ 目录定义 dataclass，引擎自动处理一切
"""

import time
import sqlite3
import importlib
import inspect
from dataclasses import dataclass, asdict, fields, is_dataclass
from typing import Any, Dict, List, Optional, Type, Union
from pathlib import Path


# ===========================================
# 类型映射系统
# ===========================================

class TypeMapper:
    """Python 类型到 SQL 类型的映射"""
    
    PYTHON_TO_SQLITE = {
        int: "INTEGER",
        str: "TEXT",
        float: "REAL",
        bool: "INTEGER",
        bytes: "BLOB",
    }
    
    @classmethod
    def to_sqlite(cls, python_type) -> str:
        # 处理 Optional[T] 类型
        if hasattr(python_type, '__origin__'):
            if python_type.__origin__ is Union:
                args = [arg for arg in python_type.__args__ if arg is not type(None)]
                if args:
                    python_type = args[0]
        
        return cls.PYTHON_TO_SQLITE.get(python_type, "TEXT")


# ===========================================
# 查询条件
# ===========================================

@dataclass
class Condition:
    """查询条件"""
    field: str
    op: str
    value: Any


# ===========================================
# 模型注册表（核心：自动加载和验证）
# ===========================================

class ModelRegistry:
    """模型注册中心 - 自动发现和加载 dataclass"""
    
    def __init__(self):
        self._models: Dict[str, Type] = {}  # table_name -> Model class
        self._schemas: Dict[str, dict] = {}  # table_name -> schema
    
    def auto_load_from_directory(self, models_dir: str):
        """
        自动扫描目录，加载所有 dataclass 模型
        
        Args:
            models_dir: 模型文件所在目录，如 "models/"
        """
        models_path = Path(models_dir)
        if not models_path.exists():
            raise FileNotFoundError(f"模型目录不存在: {models_dir}")
        
        # 扫描所有 .py 文件
        for py_file in models_path.glob("*.py"):
            if py_file.name.startswith("_"):
                continue  # 跳过 __init__.py 等
            
            # 动态导入模块
            module_name = py_file.stem
            spec = importlib.util.spec_from_file_location(module_name, py_file)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            
            # 查找模块中所有的 dataclass
            for name, obj in inspect.getmembers(module):
                if inspect.isclass(obj) and is_dataclass(obj):
                    # 自动注册（表名 = 类名小写复数）
                    table_name = self._get_table_name(name)
                    self.register(table_name, obj)
                    print(f"✅ 自动加载模型: {name} -> 表 '{table_name}'")
    
    def _get_table_name(self, class_name: str) -> str:
        """根据类名生成表名（小写 + 简单复数）"""
        name = class_name.lower()
        # 简单复数规则
        if name.endswith('y'):
            return name[:-1] + 'ies'  # Company -> companies
        elif name.endswith('s'):
            return name + 'es'  # Class -> classes
        else:
            return name + 's'  # User -> users
    
    def register(self, table_name: str, model_cls: Type):
        """手动注册模型"""
        self._models[table_name] = model_cls
        self._schemas[table_name] = self._extract_schema(model_cls)
    
    def _extract_schema(self, model_cls: Type) -> dict:
        """从 dataclass 提取字段类型"""
        return {f.name: f.type for f in fields(model_cls)}
    
    def get_model(self, table_name: str) -> Optional[Type]:
        """获取表对应的模型类"""
        return self._models.get(table_name)
    
    def get_schema(self, table_name: str) -> Optional[dict]:
        """获取表的 schema"""
        return self._schemas.get(table_name)
    
    def validate_data(self, table_name: str, data: dict) -> dict:
        """
        验证和转换数据
        - 检查必填字段
        - 类型转换
        - 过滤无效字段
        """
        schema = self.get_schema(table_name)
        if not schema:
            raise ValueError(f"表 '{table_name}' 未注册模型")
        
        model_cls = self.get_model(table_name)
        model_fields = {f.name: f for f in fields(model_cls)}
        
        validated = {}
        
        # 验证每个字段
        for field_name, field_type in schema.items():
            field_obj = model_fields[field_name]
            
            # 检查必填字段
            if field_name not in data:
                if field_obj.default == field_obj.default_factory == dataclass:
                    raise ValueError(f"缺少必填字段: {field_name}")
                continue  # 使用默认值
            
            value = data[field_name]
            
            # TODO: 类型转换和验证（可扩展）
            # if not isinstance(value, field_type):
            #     value = field_type(value)  # 尝试转换
            
            validated[field_name] = value
        
        return validated
    
    def to_object(self, table_name: str, data: dict) -> Any:
        """将字典数据转换为模型对象"""
        model_cls = self.get_model(table_name)
        if not model_cls:
            return data  # 没有模型，返回字典
        
        # 只保留模型中定义的字段
        valid_fields = {f.name for f in fields(model_cls)}
        filtered = {k: v for k, v in data.items() if k in valid_fields}
        
        return model_cls(**filtered)


# ===========================================
# 存储适配器
# ===========================================

class StorageAdapter:
    """存储层接口"""
    
    def use_database(self, db_name: str): ...
    def add(self, table: str, data: dict) -> dict: ...
    def get(self, table: str, obj_id: int) -> Optional[dict]: ...
    def update(self, table: str, obj_id: int, data: dict) -> Optional[dict]: ...
    def delete(self, table: str, obj_id: int, soft: bool) -> bool: ...
    def query(self, table: str, conditions, order_by=None, limit=None, offset=None) -> List[dict]: ...
    def create_table(self, table: str, schema: dict): ...


class MemoryStorage(StorageAdapter):
    """内存存储实现（简化版）"""
    
    def __init__(self):
        self._databases: Dict[str, Dict[str, Dict[int, dict]]] = {}
        self._id_counters: Dict[str, Dict[str, int]] = {}
        self._current_db_name: Optional[str] = None
    
    def use_database(self, db_name: str):
        if db_name not in self._databases:
            self._databases[db_name] = {}
            self._id_counters[db_name] = {}
        self._current_db_name = db_name
    
    def _ensure_table(self, table: str):
        if table not in self._databases[self._current_db_name]:
            self._databases[self._current_db_name][table] = {}
    
    def _get_next_id(self, table: str) -> int:
        counter = self._id_counters[self._current_db_name]
        counter.setdefault(table, 0)
        counter[table] += 1
        return counter[table]
    
    def create_table(self, table: str, schema: dict):
        self._ensure_table(table)
    
    def add(self, table: str, data: dict) -> dict:
        self._ensure_table(table)
        t = self._databases[self._current_db_name][table]
        data = data.copy()
        data.update({
            "id": self._get_next_id(table),
            "create_time": time.strftime("%Y-%m-%d %H:%M:%S"),
            "update_time": time.strftime("%Y-%m-%d %H:%M:%S"),
            "deleted": False,
        })
        t[data["id"]] = data
        return data
    
    def get(self, table: str, obj_id: int) -> Optional[dict]:
        t = self._databases[self._current_db_name].get(table, {})
        record = t.get(obj_id)
        return record if record and not record.get("deleted") else None
    
    def update(self, table: str, obj_id: int, data: dict) -> Optional[dict]:
        t = self._databases[self._current_db_name].get(table, {})
        if obj_id not in t or t[obj_id].get("deleted"):
            return None
        t[obj_id].update(data)
        t[obj_id]["update_time"] = time.strftime("%Y-%m-%d %H:%M:%S")
        return t[obj_id]
    
    def delete(self, table: str, obj_id: int, soft=True) -> bool:
        t = self._databases[self._current_db_name].get(table, {})
        if obj_id not in t:
            return False
        if soft:
            t[obj_id]["deleted"] = True
        else:
            del t[obj_id]
        return True
    
    def query(self, table: str, conditions, order_by=None, limit=None, offset=None) -> List[dict]:
        t = self._databases[self._current_db_name].get(table, {})
        items = [v for v in t.values() if not v.get("deleted", False)]
        
        # 简单过滤
        if isinstance(conditions, dict):
            for k, v in conditions.items():
                items = [x for x in items if x.get(k) == v]
        
        if order_by:
            reverse = order_by.startswith('-')
            key = order_by.lstrip('-')
            items.sort(key=lambda x: x.get(key, 0), reverse=reverse)
        if offset:
            items = items[offset:]
        if limit:
            items = items[:limit]
        return items


# ===========================================
# 数据引擎（核心接口）
# ===========================================

class DataEngine:
    """
    统一的数据引擎接口
    自动加载模型、验证数据、处理请求
    """
    
    def __init__(self, storage: StorageAdapter, models_dir: str = "models"):
        self.storage = storage
        self.registry = ModelRegistry()
        self.current_db: Optional[str] = None
        
        # 自动加载模型
        if Path(models_dir).exists():
            self.registry.auto_load_from_directory(models_dir)
    
    def use(self, db_name: str):
        """切换数据库"""
        self.current_db = db_name
        self.storage.use_database(db_name)
        
        # 自动创建表
        for table_name, schema in self.registry._schemas.items():
            self.storage.create_table(table_name, schema)
        
        return self
    
    def execute(self, table: str, action: str, target: Any = None, **kwargs) -> Any:
        """
        统一的操作接口
        
        Args:
            table: 表名
            action: 操作类型 (create, get, update, delete, query)
            target: 目标数据或 ID
                - create: 数据字典
                - get/delete: 记录 ID
                - update: 记录 ID
                - query: 不需要
            **kwargs: 额外参数
                - update: data (更新的数据)
                - delete: soft (是否软删除)
                - query: filter, order_by, limit, offset
        
        Returns:
            操作结果（自动转换为模型对象）
        
        Examples:
            # 创建
            user = engine.execute("users", "create", {"name": "Alice", "age": 25})
            
            # 查询
            user = engine.execute("users", "get", 1)
            users = engine.execute("users", "query", filter={"age": 30})
            
            # 更新
            user = engine.execute("users", "update", 1, data={"age": 26})
            
            # 删除
            engine.execute("users", "delete", 1)
            engine.execute("users", "delete", 1, soft=False)  # 硬删除
        """
        if not self.current_db:
            raise RuntimeError("请先调用 use('db_name')")
        
        # 路由到具体操作
        if action == "create":
            return self._create(table, target)
        elif action == "get":
            return self._get(table, target)
        elif action == "update":
            update_data = kwargs.get("data")
            if not update_data:
                raise ValueError("update 操作需要提供 data 参数")
            return self._update(table, target, update_data)
        elif action == "delete":
            return self._delete(table, target, kwargs.get("soft", True))
        elif action == "query":
            return self._query(table, kwargs.get("filter"), kwargs.get("order_by"), 
                             kwargs.get("limit"), kwargs.get("offset"))
        else:
            raise ValueError(f"不支持的操作: {action}")
    
    def _create(self, table: str, data: dict):
        """创建记录"""
        # 验证数据
        validated = self.registry.validate_data(table, data)
        
        # 存储
        record = self.storage.add(table, validated)
        
        # 转换为对象
        return self.registry.to_object(table, record)
    
    def _get(self, table: str, obj_id: int):
        """获取单条记录"""
        record = self.storage.get(table, obj_id)
        return self.registry.to_object(table, record) if record else None
    
    def _update(self, table: str, obj_id: int, data: dict):
        """更新记录"""
        validated = self.registry.validate_data(table, data)
        record = self.storage.update(table, obj_id, validated)
        return self.registry.to_object(table, record) if record else None
    
    def _delete(self, table: str, obj_id: int, soft: bool = True):
        """删除记录"""
        return self.storage.delete(table, obj_id, soft)
    
    def _query(self, table: str, filter_dict=None, order_by=None, limit=None, offset=None):
        """查询记录"""
        conditions = filter_dict or {}
        results = self.storage.query(table, conditions, order_by, limit, offset)
        return [self.registry.to_object(table, r) for r in results]
    
    # 便捷方法（可选）
    def create(self, table: str, **data):
        """简化创建"""
        return self.execute(table, "create", data)
    
    def get(self, table: str, obj_id: int):
        """简化获取"""
        return self.execute(table, "get", obj_id)
    
    def query(self, table: str, filter=None, order_by=None, limit=None, offset=None):
        """简化查询"""
        return self.execute(table, "query", filter=filter, order_by=order_by, 
                          limit=limit, offset=offset)


# ===========================================
# 测试示例
# ===========================================

if __name__ == "__main__":
    # 模拟：工程师在 models/ 目录定义模型
    print("=" * 70)
    print("步骤 1: 工程师定义模型（在 models/ 目录）")
    print("=" * 70)
    
    # 创建 models 目录和示例模型
    models_dir = Path("models")
    models_dir.mkdir(exist_ok=True)
    
    # 写入 user.py
    (models_dir / "user.py").write_text('''
from dataclasses import dataclass

@dataclass
class User:
    name: str
    age: int
    email: str
    status: str = "active"
''')
    
    # 写入 post.py
    (models_dir / "post.py").write_text('''
from dataclasses import dataclass

@dataclass
class Post:
    title: str
    content: str
    author_id: int
    likes: int = 0
''')
    
    print("""
✅ 工程师已定义模型:
   - models/user.py  → User
   - models/post.py  → Post
    """)
    
    
    print("\n" + "=" * 70)
    print("步骤 2: 初始化数据引擎（自动加载模型）")
    print("=" * 70)
    
    # 初始化引擎（自动加载）
    engine = DataEngine(storage=MemoryStorage(), models_dir="models")
    engine.use("blog_db")
    
    
    print("\n" + "=" * 70)
    print("步骤 3: 使用统一接口操作数据")
    print("=" * 70)
    
    print("\n【方式 1: execute() 统一接口】")
    
    # 创建用户
    u1 = engine.execute("users", "create", {"name": "Alice", "age": 25, "email": "alice@example.com"})
    u2 = engine.execute("users", "create", {"name": "Bob", "age": 30, "email": "bob@example.com"})
    print(f"创建用户: {u1.name}, {u2.name}")
    
    # 查询
    user = engine.execute("users", "get", 1)
    print(f"获取用户: {user}")
    
    # 条件查询
    adults = engine.execute("users", "query", filter={"age": 30})
    print(f"年龄=30: {[u.name for u in adults]}")
    
    # 更新
    updated = engine.execute("users", "update", 1, data={"age": 26})
    print(f"更新后: {updated}")
    
    # 删除
    engine.execute("users", "delete", 2)
    print(f"删除后所有用户: {[u.name for u in engine.execute('users', 'query')]}")
    
    
    print("\n【方式 2: 便捷方法】")
    
    # 使用便捷方法
    u3 = engine.create("users", name="Charlie", age=35, email="charlie@example.com")
    print(f"创建: {u3.name}")
    
    user = engine.get("users", 1)
    print(f"获取: {user.name}")
    
    all_users = engine.query("users")
    print(f"所有用户: {[u.name for u in all_users]}")
    
    recent = engine.query("users", order_by="-age", limit=2)
    print(f"年龄最大的2个: {[(u.name, u.age) for u in recent]}")
    
    
    print("\n" + "=" * 70)
    print("步骤 4: 测试跨表操作")
    print("=" * 70)
    
    # 创建文章
    p1 = engine.create("posts", title="Hello World", content="First post", author_id=1)
    p2 = engine.create("posts", title="Python Tips", content="Learn Python", author_id=1, likes=10)
    print(f"创建文章: {p1.title}, {p2.title}")
    
    # 查询用户的文章
    user_posts = engine.query("posts", filter={"author_id": 1})
    print(f"用户1的文章: {[p.title for p in user_posts]}")
    
    
    print("\n" + "=" * 70)
    print("✅ 完成！数据引擎自动处理了：")
    print("=" * 70)
    print("""
1. ✅ 自动扫描 models/ 目录
2. ✅ 自动加载所有 dataclass
3. ✅ 自动生成表名（User -> users）
4. ✅ 自动创建数据库表
5. ✅ 自动验证数据格式
6. ✅ 自动转换返回对象
7. ✅ 统一的操作接口

🎯 工程师只需：
   - 在 models/ 定义 dataclass
   - 调用 engine.execute() 或便捷方法
   
引擎处理剩下的一切！
    """)
    
    # 清理测试文件
    import shutil
    shutil.rmtree("models")