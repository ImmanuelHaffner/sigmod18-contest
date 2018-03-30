# SIGMOD'18 Programming Contest

## Team

Immanuel Haffner <br>
[immanuel.haffner@bigdata.uni-saarland.de](mailto:immanuel.haffner@bigdata.uni-saarland.de) <br>
Saarland Informatics Campus <br>
Big Data Analytics Group

## Misc

We don't need MQO.  Execute all queries independently.  Most queries are "simple", so we can do the in-place aggregation
join.  Sharing intermediate results would be very complicated and the gain is questionable.
For simplicitly, each query is planned and executed independently.

## Features

* Compute a join matrix for each batch.  The join matrix is a diagonal matrix, which counts how often two relations are
  joined together.  The join matrix helps to decide which relations or intermediate results to cache for future
  workloads.

* Joins implicitly aggregate their result by the join predicate of the next (parent) join.  This minimizes
  materialization.


## TODOs

* Support self-joins
    * Small, Batch 2 Q5: `3 0 13 13|0.2=1.0&1.0=2.1&2.1=3.2&0.2<74|1.2 2.5 3.5`
* Support multiple join predicates for one join (e.g. `A.x = B.A_x AND A.y = B.A_y`, where `(A.x, A.y)` is the primary
  key of `A`
    * Small, Batch 2 Q10: `9 1 11|0.2=1.0&1.0=2.1&1.0=0.2&0.3>3991|1.0` (Actually a duplicate join predicate: `0.2=1.0` =
      `1.0=0.2`)
    * Small, Batch 3 Q13: `7 0 9|0.1=1.0&1.0=0.1&1.0=2.1&0.1>3791|1.2 1.2` (Actually a duplicate join predicate:
      `0.1=1.0` = `1.0=0.1`)
    * Small, Batch 4 Q3: `7 1 3|0.2=1.0&1.0=2.1&1.0=0.2&0.2>6082|2.3 2.1` (Actually a duplicate join predicate:
      `0.2=1.0` = `1.0=0.2`)
* Support cyclic joins.  Break the cycle by removing the most costly, redundant join predicate(s).
