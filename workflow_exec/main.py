from logging import basicConfig, DEBUG

from workflow_exec import Interpreter


def main():
    basicConfig(level=DEBUG)

    interpreter = Interpreter()
    interpreter.execute_pipeline()


if __name__ == '__main__':
    main()
