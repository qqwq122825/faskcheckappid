"""Web entry point on Windows, macOS and Linux."""
from fastcheck.web.launcher import main

if __name__ == '__main__':
    import multiprocessing
    multiprocessing.freeze_support()
    main()
