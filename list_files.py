import sys
import os
import heapq


"""
This script expects 2 positional arguments
1) the root directory from where to crawl
2) an integer n

It outputs the top n largest files in the root dir

sample usage:

python3 /home/rahul/movies 10
"""


def yield_files(root):
    if os.path.isfile(root):
        yield root
    elif os.path.isdir(root):
        for child in os.listdir(root):
            yield from yield_files(os.path.join(root, child))


def nlargest_files(n, files):
    return heapq.nlargest(n, files, key=lambda file: os.path.getsize(file))


def main(n, root_dir):
    for file in nlargest_files(n, yield_files(root_dir)):
        print(os.path.getsize(file), file)


if __name__ == '__main__':
    main(n=int(sys.argv[2]), root_dir=sys.argv[1])
