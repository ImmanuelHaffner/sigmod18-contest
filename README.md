# SIGMOD'18 Programming Contest

## Team

Immanuel Haffner <br>
[immanuel.haffner@bigdata.uni-saarland.de](mailto:immanuel.haffner@bigdata.uni-saarland.de) <br>
Saarland Informatics Campus <br>
Big Data Analytics Group

## Features

* Compute a join matrix for each batch.  The join matrix is a diagonal matrix, which counts how often two relations are
  joined together.  The join matrix helps to decide which relations or intermediate results to cache for future
  workloads.

* **TODO:** Joins implicitly aggregate their result by the join predicate of the next (parent) join.  This minimizes
  materialization.
