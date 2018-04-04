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


### Incorrect Queries

#### Small Workload
```
3 0 1|0.2=1.0&0.1=2.0&0.2>3499|1.2 0.1 # r3 has two join attribtues
10 0 13 1|0.2=1.0&1.0=2.2&0.1=3.0&0.1=209|0.2 2.5 2.2 # r10 has two join attributes
9 1 12|0.2=1.0&1.0=2.1&1.0=2.2&0.2<2685|2.0 # r12 has two join attributes; r1 |X| r12 has two join predicates
1 12 2|0.0=1.2&0.0=2.1&0.0=1.1&1.0>25064|0.2 1.3 # r12 has two join attributes; r1 |X| r12 has two join predicates
12 1 6 12|0.2=1.0&1.0=2.1&0.1=3.2&3.0<33199|2.1 0.1 0.2 # r12 has two join attributes; self join
13 0 2|0.2=1.0&0.1=1.0&1.0=2.2&0.1>4477|2.0 2.3 1.2 # r13 has two join attributes; r13 |X| r0 has two join predicates
8 0 13 13|0.2=1.0&1.0=2.2&2.1=3.2&0.1>7860|3.3 2.1 3.6 # r13 has two join attributes; self join
```

#### Large Workload
```
7 0 5|0.1=1.0&0.2=1.0&1.0=2.1&0.0<5405896|1.0 0.0 2.0 # r7 has two join attributes; r7 |X| r0 has two join predicates
3 0 1 7|0.2=1.0&0.1=2.0&1.0=3.2&0.3<8341&3.0<9877975|2.1 0.0 1.0 # r3 has two join attribtues
5 1 3 0|0.2=1.0&1.0=2.1&2.2=3.0&0.0<6227353&2.3<8745|3.0 1.0 # r3 has two join attributes
3 1 0|0.1=1.0&0.2=2.0&0.2>586487|1.1 1.0 # r3 has two join attributes
2 1 3 0|0.1=1.0&1.0=2.1&2.2=3.0&0.1<9934260|2.3 # r3 has two join attributes
7 0|0.1=1.0&0.2=1.0&0.2<794389|0.0 # r7 has two join attributes; r7 |X| r0 has two join predicates
8 0 7|0.1=1.0&0.2=1.0&1.0=2.2&0.2<239914|2.0 0.0 2.0 # r8 has two join attributes
7 0 5|0.1=1.0&1.0=2.1&0.2=1.0&0.1>759309&2.1>607349|2.1 # r7 has two join attributes; r7 |X| r0 has two join predicates
7 0 4|0.1=1.0&0.2=1.0&1.0=2.2&0.0=10350549&2.1=348213|1.1 2.1 0.1 # r7 has two join attributes; r7 |X| r0 has two join predicates
1 5 0|0.0=1.2&1.1=2.0&1.2=4603582|2.0 2.1 # r5 has two join attributes
0 5 7|0.0=1.1&0.0=2.2&0.0=2.1&1.1=178541|2.2 1.1 # r7 has two join attribtues; r0 |X| r7 has two join predicates
3 1 5 0|0.1=1.0&1.0=2.2&2.1=3.0&0.0=246419|0.3 3.0 # r5 has two join attribtues
```


## TODOs

* Support self-joins
    * Small, Batch 2 Q5: `3 0 13 13|0.2=1.0&1.0=2.1&2.1=3.2&0.2<74|1.2 2.5 3.5`
        * surprisingly resolved by avoiding duplicate join with r13; one of the join attributes must be unique
* Support multiple join predicates for one join (e.g. `A.x = B.A_x AND A.y = B.A_y`, where `(A.x, A.y)` is the primary
  key of `A`
    * Small, Batch 2 Q10: `9 1 11|0.2=1.0&1.0=2.1&1.0=0.2&0.3>3991|1.0` (Actually a duplicate join predicate: `0.2=1.0` =
      `1.0=0.2`)
    * Small, Batch 3 Q13: `7 0 9|0.1=1.0&1.0=0.1&1.0=2.1&0.1>3791|1.2 1.2` (Actually a duplicate join predicate:
      `0.1=1.0` = `1.0=0.1`)
    * Small, Batch 4 Q3: `7 1 3|0.2=1.0&1.0=2.1&1.0=0.2&0.2>6082|2.3 2.1` (Actually a duplicate join predicate:
      `0.2=1.0` = `1.0=0.2`)
* Support cyclic joins.  Break the cycle by removing the most costly, redundant join predicate(s).


## Ideas

* If a join has two attributes, convert all but one to filter conditions
* If a relation has two join attributes, and one of the attributes is unique, then join by the unique attribute first,
  and for each join result, store the second attribute. **NOT POSSIBLE:** all join columns have duplicates
* Can we boldly omit self-joins?  If all self-joins in the workload use an unique attribute, the self-join is
  superfluous.


* Implement method `simplify()` that simplifies query and returns `true` iff the query simple, that is, the query is
  computable with an in-place aggregation join.
