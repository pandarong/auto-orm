from auto_orm import DataEngine, MemoryStorage
from pathlib import Path


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