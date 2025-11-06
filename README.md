# AutoORM

Automatic ORM engine that loads `@dataclass` models from `models/` directory.

## Install

```bash
pip install auto-orm

## Quick Start
from orm_engine import DataEngine, MemoryStorage

engine = DataEngine(storage=MemoryStorage(), models_dir="models")
engine.use("mydb")

user = engine.create("users", name="Alice", age=30)
print(user.name)  # Alice