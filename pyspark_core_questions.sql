PySpark Core + Shuffle + Skew — Revision Q&A
-----------------------------------------------------
Round 1 – Spark execution fundamentals
Q1. What exactly triggers a Spark job?

Answer:
A Spark job is triggered only when an action is called (show, count, collect, write). Transformations are lazy.

Q2. Difference between Job, Stage, and Task?

Answer:

Job: Created when an action is triggered

Stage: Set of tasks separated by shuffle boundaries

Task: Smallest unit of work on one partition, executed by one executor core

Q3. Why does a wide transformation create a new stage?

Answer:
Wide transformations require data redistribution (shuffle) across partitions, which enforces a stage boundary.

Q4. Who decides the number of tasks in a stage?

Answer:
The driver decides it based on the number of partitions.
👉 Partitions = Tasks

Round 2 – Shuffle (critical section)
Q5. What data is shuffled — rows or partitions?

Answer:
Rows are shuffled, not partitions.
Rows are reassigned to new partitions based on shuffle keys.

Q6. Why does Spark need the driver during shuffle?

Answer:
The driver maintains shuffle metadata (map output locations) so reducers know where to fetch data from.

Q7. What happens if a reducer receives zero records?

Answer:
The reducer still runs.
Spark requires confirmation from all reducers, even empty ones.

Q8. One real production reason shuffle is slow even for small data?

Answer:

Missing broadcast join → shuffle join

Poor partitioning

Network or disk spill overhead

Round 3 – Cache / Persist
Q9. Cache vs Persist — real difference?

Answer:

cache() → memory only

persist() → configurable (memory, disk, serialized)
Used after expensive transformations that are reused.

Q10. When does cached data get materialized?

Answer:
Only when an action runs on the cached DataFrame.

Q11. Why is caching before a shuffle often useless?

Answer:
Because shuffle recreates partitions, invalidating cached layout.

Q12. When is not unpersisting a real bug?

Answer:
When cached data consumes executor memory and causes:

GC pressure

Spill to disk

OOM during later shuffles

Round 4 – File formats & writes
Q13. Why are too many small files bad?

Answer:
They increase:

Task scheduling overhead

Metadata load

Read latency in downstream jobs

Q14. coalesce vs repartition (execution-wise)?

Answer:

coalesce() → no shuffle, reduces partitions

repartition() → full shuffle, rebalances data

Q15. Why Parquet beats CSV even for small datasets?

Answer:
Columnar format + metadata (min/max) + predicate pushdown → less IO.

Q16. When is CSV still acceptable?

Answer:

Very small datasets

Data exchange / human-readable needs

Temporary or debugging outputs

Round 5 – Spark UI
Q17. Where do you identify skew in Spark UI?

Answer:
Stages → Tasks
Look for tasks with much longer duration or larger input size.

Q18. What does one long-running task indicate?

Answer:
Possible causes:

Key skew

Data skew

Spill to disk

GC pressure
(Not always key skew)

Q19. Why is GC time > task time a red flag?

Answer:
It indicates memory pressure and excessive object churn, not real computation.

Q20. Which Spark UI tab is most useful and why?

Answer:
Stages tab — shows task-level imbalance, shuffle, skew, and root causes.

Round 6 – Manual Salting
Q21. Why do we salt the fact table, not the dimension?

Answer:
Fact tables carry high-volume hot keys. Salting splits them across partitions.

Q22. Why must dimension salting be deterministic?

Answer:
So each fact row can reliably match all corresponding salted dimension keys during join.

Q23. What happens if salt size is too high?

Answer:

Explosion of keys

Join overhead

Memory and compute waste

Q24. When is salting the wrong solution?

Answer:
When the issue is partition skew, not key skew.
Fix with repartitioning or upstream data correction.

Final honesty check
Q25. One common Spark learning mistake?

Answer:
Assuming Spark will “auto-fix” problems instead of explicitly controlling joins, partitions, caching, and skew.

Spark Skew – Crisp Interview One-Liners
Q1. Why didn’t AQE fix your skew?

AQE can split skewed shuffle partitions but cannot redistribute dominant keys or fix structural data skew.

Q2. Why can AQE make a job slower?

AQE adds runtime planning overhead and can create extra tasks whose scheduling cost outweighs the skew benefit for small or stable datasets.

Q3. Why doesn’t AQE eliminate skew completely?

AQE parallelizes large partitions but does not change data distribution, so hot keys remain hot.

Q4. You salted but still see skew — why?

Common causes are insufficient salt size, non-deterministic dimension salting, or salting when broadcast would have avoided shuffle entirely.

Q5. Why is salting risky for left outer joins?

Salting can multiply dimension rows and break left-join semantics, causing duplicates or incorrect null preservation.

Q6. How do you choose salt size?

I choose salt size by matching hot-key workload to normal task size, validated through Spark UI task input size and duration.

Q7. Auto-broadcast happened but job is slow — why?

Either the broadcast table is too large causing executor memory pressure, or the fact-side computation dominates runtime.

Q8. Why can broadcast join be worse than shuffle join?

Because broadcasting large tables to every executor increases memory usage, GC pressure, and serialization overhead.

Q9. Tasks are even, but stage is slow — why?

The bottleneck is likely CPU-bound logic, expensive UDFs, serialization cost, or external IO, not skew.

Q10. High GC time but low shuffle — what does that mean?

The job is memory-bound due to excessive object creation, over-caching, or inefficient transformations.

Q11. Same key skew every day — what does that tell you?

The skew is a permanent business property of the data and must be addressed at modeling or upstream design level.

Q12. Why is upstream skew fix better than Spark-side fixes?

Upstream fixes eliminate repeated runtime overhead, reduce compute cost, and prevent long-term performance debt.

Q13. When would you accept a slow skewed job?

When runtime is predictable, within SLA, and optimization cost outweighs business value.

Q14. Explain: “AQE hides debt, salting repays it.”

AQE temporarily masks skew at runtime, while salting permanently fixes skew through data design.


Interview Quetions:


During one of our Spark jobs, we observed frequent job failures due to shuffle spill leading to OOM errors. The job involved heavy joins on ~45 million records. Initially it worked in lower environment but failed in production due to data skew and larger partition size. After analyzing Spark UI, we found one reducer handling disproportionately large data due to skewed key distribution.

We addressed this by applying salting technique on skewed keys, adjusting shuffle partitions appropriately, and enabling AQE to dynamically optimize skew joins. Post fix, shuffle spill reduced significantly and job stabilized within SLA


Small File Explosion

“Frequent micro-batch writes caused small file explosion in Iceberg, increasing planning time. We introduced threshold-based compaction and optimized write distribution to maintain ideal file size.”

🔹 Dynamic Partition Overwrite Misconfiguration

“Overwrite mode wiped full table because dynamic partition overwrite wasn’t enabled. We corrected configuration and implemented guardrails.”

🔹 Broadcast Join Failure

“Broadcast threshold was too high causing driver memory pressure. Tuned broadcast threshold and moved to shuffle join.”


Context

Domain

Data volume

Frequency

2️⃣ Architecture

Source → Bronze → Silver → Gold

Storage format (Iceberg)

Processing engine (Spark)

3️⃣ Your Role

Data ingestion

Transformations

Reconciliation

SLA monitoring

4️⃣ Challenges

Skew

Concurrency

Snapshot growth

Reconciliation lag

5️⃣ Improvements You Made

Optimized joins

Partition strategy

Introduced micro-batch merge

Implemented validation controls

Example Structured Answer

“In my recent project in banking risk reporting, we built a Spark-based pipeline ingesting ~40–50 million records daily from multiple upstream systems. Data was stored in Iceberg tables following Bronze-Silver-Gold architecture.

I was responsible for transformation logic, partition design, merge operations, and reconciliation checks before regulatory submission.

One major challenge was skew during heavy joins, causing shuffle spill and SLA delays. Using Spark UI analysis, we implemented key salting and enabled AQE skew optimization, which reduced runtime by ~35% and stabilized production runs.”

Clean. Clear. Confident.



“During migration from non-ACID table to Iceberg-backed partitioned table, we observed full-table overwrite because dynamic partition overwrite behavior differed from legacy setup. We added explicit config enforcement and write guards to prevent future accidental full rewrites.”


In lower environment dataset was small enough to broadcast. In production, dimension table grew significantly due to historical accumulation, crossing broadcast threshold and causing driver memory pressure. We moved to shuffle join and adjusted join strategy

Due to 5-minute micro-batch writes, file count increased rapidly impacting planning time. We implemented threshold-driven compaction and optimized write distribution to maintain target file size.”

Distribution looked balanced in lower environments, but in production one key became dominant due to business concentration. This caused one reducer to process disproportionately large partition. We identified this via Spark UI stage analysis.


Post fix, we added skew detection checks during data profiling phase to proactively monitor key distribution.


Initially, the dimension table was well within broadcast limits and validated against projected growth. However, over time historical retention increased and the table size crossed safe broadcast memory threshold in production. Instead of relying on static assumptions, we moved away from broadcast hinting and allowed adaptive execution to choose optimal join strategy. We also introduced periodic size monitoring to avoid similar issues.
----------------------------------------------------------------------
What makes Spark fast in modern versions?”

What would you say?

Mention:

Catalyst

Tungsten

Whole-stage codegen

AQE

Modern Spark is fast because of four layers working together. Catalyst analyzes logical plans and generates optimized physical plans using rule-based and cost-based optimizations like predicate pushdown and join reordering. Tungsten improves memory and CPU efficiency by storing data in compact binary format instead of JVM objects, reducing GC pressure and improving cache locality. Whole-stage codegen generates a single compiled JVM function for multiple operators, eliminating iterator and virtual call overhead. Finally, AQE (Adaptive Query Execution) re-optimizes the physical plan at runtime using shuffle statistics — enabling dynamic join strategy changes, skew partition splitting, and shuffle partition coalescing. Together, these make Spark compute-efficient, memory-efficient, and runtime-adaptive
--------------------------------------------------