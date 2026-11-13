import multiprocessing
import sys
from app import main

if __name__ == '__main__':
    multiprocessing.freeze_support()
    sys.argv.insert(1, 'node')
    main()
