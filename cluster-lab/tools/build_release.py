"""Build the Vue UI locally, then package both Python roles for deployment."""
from pathlib import Path
import shutil
import subprocess
import sys


def main():
    root = Path(__file__).resolve().parents[1]
    npm = shutil.which('npm')
    if not npm:
        raise SystemExit('本地构建需要 Node.js 与 npm；服务器使用 release ZIP 时不需要。')
    subprocess.run([npm, 'ci', '--no-audit', '--no-fund'], cwd=root/'frontend', check=True)
    subprocess.run([npm, 'run', 'build'], cwd=root/'frontend', check=True)
    subprocess.run([sys.executable, str(root/'tools/package.py')], cwd=root, check=True)


if __name__ == '__main__':
    main()
