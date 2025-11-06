"""命令行接口"""

import argparse
import sys
from pathlib import Path

def main():
    parser = argparse.ArgumentParser(description="Auto ORM 命令行工具")
    parser.add_argument("--models-dir", default="models", help="模型目录路径")
    parser.add_argument("--db", default="default.db", help="数据库文件")
    
    args = parser.parse_args()
    
    # 这里可以添加具体的命令行功能
    print(f"Auto ORM - 模型目录: {args.models_dir}, 数据库: {args.db}")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())