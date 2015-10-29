import os
import sys


sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))


from workflow_exec.main import main


main()
