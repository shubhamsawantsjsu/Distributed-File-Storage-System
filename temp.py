from concurrent.futures import ThreadPoolExecutor
import threading
import random

def task(n):
    print(n*n)
    print("Task Executed {}".format(threading.current_thread()))

def main():
    print("Starting ThreadPoolExecutor")
    executor = ThreadPoolExecutor(max_workers=3)

    executor.submit(task, (2))
    executor.submit(task, (3))
    executor.submit(task, (4))

    print("All tasks complete")

if __name__ == '__main__':
    main()