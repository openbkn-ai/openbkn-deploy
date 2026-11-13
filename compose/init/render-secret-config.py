#!/usr/bin/env python3
"""把 ${VAR} 占位符替换为环境变量值，写出 data-migrator 的 secret-config.yaml。

用法: render-secret-config.py <template> <output>
缺失的环境变量会报错退出，避免生成半成品配置。
"""
import os
import re
import sys

TOKEN = re.compile(r"\$\{([A-Z0-9_]+)\}")


def main() -> int:
    if len(sys.argv) != 3:
        print(__doc__, file=sys.stderr)
        return 2
    template_path, output_path = sys.argv[1], sys.argv[2]
    with open(template_path, encoding="utf-8") as fh:
        text = fh.read()

    missing = sorted({m.group(1) for m in TOKEN.finditer(text) if m.group(1) not in os.environ})
    if missing:
        print("missing environment variables: " + ", ".join(missing), file=sys.stderr)
        return 1

    rendered = TOKEN.sub(lambda m: os.environ[m.group(1)], text)
    with open(output_path, "w", encoding="utf-8") as fh:
        fh.write(rendered)
    print(f"rendered {output_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
