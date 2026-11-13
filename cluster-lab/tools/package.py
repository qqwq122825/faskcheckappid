"""Build role-specific source archives without runtime data or credentials."""
from pathlib import Path
import re
import zipfile


def main():
    root = Path(__file__).resolve().parents[1]
    version = re.search(r"VERSION = '([^']+)'", (root/'clusterlab/__init__.py').read_text())[1]
    ui = root/'clusterlab/static/ui'
    if not (ui/'index.html').is_file():
        raise SystemExit('前端尚未构建，请先运行 python tools/build_release.py')
    release = root / 'release'
    release.mkdir(exist_ok=True)
    shared = [root/'app.py', root/'requirements.txt', root/'README.md', root/'DEPLOY_BT.md']
    shared.extend(p for p in (root/'clusterlab').rglob('*') if p.is_file() and '__pycache__' not in p.parts
                  and ('static' not in p.relative_to(root/'clusterlab').parts or ui in p.parents))
    for role in ('controller', 'node'):
        target = release / f'cluster-lab-{role}-{version}.zip'
        with zipfile.ZipFile(target,'w',zipfile.ZIP_DEFLATED) as archive:
            for path in [*shared, root/(role+'.py')]:
                archive.write(path, path.relative_to(root))
        print(target)


if __name__ == '__main__':
    main()
