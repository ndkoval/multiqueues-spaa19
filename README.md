# Parallel Dijkstra Algorithm

[![Build Status](https://travis-ci.com/IST-CONCURRENCY-COURSE-2018/dijkstra-<your_GitHub_account>.svg?token=B2yLGFz6qwxKVjbLm9Ak&branch=master)](https://travis-ci.com/IST-CONCURRENCY-COURSE-2018/dijkstra-<your_GitHub_account>)

Please, use [IntelliJ IDEA](https://www.jetbrains.com/idea/) (better) or Eclipse/Netbeans (worse) as a development environment. Do not use vim or emacs!

## Project description
This project includes the following files:

* `Graph.kt` contains `Node` and `Edge` classes.
* `Dijkstra.kt` contains a sequential Dijkstra algorithm implementation (function `shortestPathSequential`), and a template for the parallel version (function `shortestPathParallel`).

You can use Java to instead of Kotlin for the `shortestPathParallel` implementation; just create a Java class with a static `int shortestPathParallel(Node from, Node to)` function, remove the already added `shortestPathParallel` function from `Dijkstra.kt` and fix the compilation errors.

## Task description
You need to implement a parallel version of the Dijkstra algorithm in `shortestPathParallel` function. Use `MultiPriorityQueue` (and implement it at first!)  as a relaxed priority queue. Use the same number of internal queues as the number of workers. Don't forget about races while updating the node distances -- you can use `CAS` or locks to update them.

## Submission format
Do the assignment in this repository, and commit (and push!) your solution to submit it. 

Replace `<your_GitHub_account>` in the beginning of this file with your GitHub account before submission (two places: image and build links). This is required to show a build status in Travis.