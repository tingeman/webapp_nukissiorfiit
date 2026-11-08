#!/usr/bin/env python3
"""Compile hash-locked requirements.txt / requirements-dev.txt from the .in files.

Usage: python scripts/compile_lock.py [--upgrade] [--package NAME]... [--constraints PATH]
                                     [--exclude-newer DATE]
  (no flags)           keep versions pinned by --constraints (if given) or resolve fresh
  --constraints PATH   pip-freeze style file; versions are held to it (no-upgrade lock)
  --upgrade            resolve to the newest allowed versions (annual upgrade)
  --package NAME       upgrade only NAME (repeatable); the rest stays at the existing lock
  --exclude-newer DATE ignore releases uploaded after DATE (YYYY-MM-DD); a cooldown against
                       freshly published, possibly compromised versions

Runs uv inside a container of the pinned python image, so only Python 3.8+ (stdlib only)
and Docker are needed on the host. Works from Windows PowerShell, WSL, Linux and Git Bash.
NUKI_PYTHON_IMAGE / NUKI_UV_VERSION env vars override the pins in compose.pins.env.
"""

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path

REPO_DIR = Path(__file__).resolve().parent.parent
APP_DIR = REPO_DIR / 'app'
PINS_FILE = REPO_DIR / 'compose.pins.env'
LOCK_FILES = ('requirements.txt', 'requirements-dev.txt')


def read_pins(path):
    pins = {}
    try:
        text = path.read_text(encoding='utf-8')
    except OSError:
        return pins
    for line in text.splitlines():  # splitlines also drops CR of CRLF files
        line = line.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue
        key, value = line.split('=', 1)
        pins.setdefault(key.strip(), value.strip())
    return pins


def count_pins(path):
    try:
        text = path.read_text(encoding='utf-8')
    except OSError:
        return 0
    return len(re.findall(r'^[A-Za-z0-9][^\s=]*==', text, flags=re.MULTILINE))


def main(argv=None):
    parser = argparse.ArgumentParser(
        description='Compile hash-locked requirements with uv in a container.'
    )
    parser.add_argument(
        '--constraints', metavar='PATH', help='pip-freeze style file to hold versions to'
    )
    parser.add_argument(
        '--upgrade', action='store_true', help='resolve to the newest allowed versions'
    )
    parser.add_argument(
        '--package',
        action='append',
        default=[],
        metavar='NAME',
        help='upgrade only NAME (repeatable)',
    )
    parser.add_argument(
        '--exclude-newer',
        metavar='DATE',
        help='ignore releases uploaded after DATE (YYYY-MM-DD), e.g. a week before today',
    )
    args = parser.parse_args(argv)

    pins = read_pins(PINS_FILE)
    image = os.environ.get('NUKI_PYTHON_IMAGE') or pins.get('NUKI_PYTHON_IMAGE', '')
    uv_version = os.environ.get('NUKI_UV_VERSION') or pins.get('NUKI_UV_VERSION', '')
    if not image:
        sys.exit('NUKI_PYTHON_IMAGE not set and not found in compose.pins.env')
    if not uv_version:
        sys.exit('NUKI_UV_VERSION not set and not found in compose.pins.env')

    # Native resolved paths: C:\... on Windows, POSIX paths on WSL/Linux. Both are accepted by docker -v.
    mounts = ['-v', f'{APP_DIR.resolve()}:/work']
    compile_args = [
        '--generate-hashes',
        '--python-version',
        '3.13',
        '--python-platform',
        'linux',
        '--no-header',
        '--quiet',
    ]
    # Like the .sh version, constraints are ignored together with --upgrade.
    if args.constraints and not args.upgrade:
        cpath = Path(args.constraints).resolve()
        if not cpath.is_file():
            sys.exit(f'Constraints file not found: {args.constraints}')
        mounts += ['-v', f'{cpath.parent}:/constraints:ro']
        compile_args += ['-c', f'/constraints/{cpath.name}']
    if args.exclude_newer:
        compile_args += ['--exclude-newer', args.exclude_newer]
    if args.upgrade:
        compile_args.append('--upgrade')
    else:
        for name in args.package:
            compile_args += ['--upgrade-package', name]

    # Executed by sh inside the container; quote everything for it.
    def sh_quote(s):
        return "'" + s.replace("'", "'\\''") + "'"

    common = ' '.join(sh_quote(a) for a in compile_args)
    script = '\n'.join(
        [
            'set -e',
            f'pip install --quiet --disable-pip-version-check --root-user-action=ignore {sh_quote("uv==" + uv_version)}',
            f'uv pip compile requirements.in {common} -o requirements.txt',
            f'uv pip compile requirements-dev.in {common} -o requirements-dev.txt',
        ]
    )
    cmd = ['docker', 'run', '--rm', *mounts, '-w', '/work', image, 'sh', '-c', script]

    try:
        result = subprocess.run(cmd)
    except FileNotFoundError:
        sys.exit('docker not found on PATH; install Docker (Desktop) and make sure it is running')
    if result.returncode != 0:
        sys.exit(
            f'docker run failed (exit code {result.returncode}); lock files were not (fully) updated'
        )

    for name in LOCK_FILES:
        print(f'Wrote app/{name} ({count_pins(APP_DIR / name)} pins)')
    return 0


if __name__ == '__main__':
    sys.exit(main())
