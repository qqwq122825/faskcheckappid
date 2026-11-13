import sys
from app import main

if __name__ == '__main__':
    sys.argv.insert(1, 'controller')
    main()
