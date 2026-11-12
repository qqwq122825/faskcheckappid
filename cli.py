"""CLI entry point sharing the Web scheduler and protocol adapters."""
from fastcheck.cli import main

if __name__ == '__main__':
    import multiprocessing
    multiprocessing.freeze_support()
    raise SystemExit(main())
