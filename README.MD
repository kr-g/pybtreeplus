
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)


# pybtreeplus - B+-Tree 

`pybtreeplus` provides core methods for searching, inserting, and deleting data for a 
[`B+Tree`](https://en.wikipedia.org/wiki/B%2B_tree) 

for expert reading: 
[B-tree_and_UB-tree](http://www.scholarpedia.org/article/B-tree_and_UB-tree)


# What's new ?

Check
[`CHANGELOG`](https://github.com/kr-g/pybtreeplus/blob/main/CHANGELOG.MD)
for latest ongoing, or upcoming news


# how to use

reading and deleting is done via `Context` class which is reponsable for caching, preventing clashes,
and in-memory index manipulation prior writing all changes finally to the heap.
class methods using a `Context` have `_ctx` as naming convention.
if no ctx is provided a ctx is created on the fly and closed properly at the end.

refer also to test cases in [`tests`](https://github.com/kr-g/pybtreeplus/blob/main/tests)


# memory / file layout

see [`pybtreecore`](https://github.com/kr-g/pybtreecore/) for layout.


## limitation

- see other limits also here [`pybtreecore`](https://github.com/kr-g/pybtreecore/)
- 

# Platform

Tested on Python3, and Linux.


# development status

alpha state.
the API or logical call flow might change without prior notice.

read [`CHANGELOG`](https://github.com/kr-g/pybtreeplus/blob/main/CHANGELOG.MD)
for latest, or upcoming news.

remark: some of the test case perform a integrity check on the _whole_ tree after every insert, or delete.
those test are therefore very long running tasks. without integritry checks it performs much faster.


# installation
    
available on pypi. install with:

    python3 -m pip install pybtreeplus
    
