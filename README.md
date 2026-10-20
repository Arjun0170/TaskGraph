# TaskGraph — dependency-aware job scheduling simulator

TaskGraph simulates batch workflows with job dependencies, limited workers, and retryable failures. It compares FIFO and critical-path scheduling and produces a deterministic trace of every execution attempt.

The engine uses a logical clock and completion events to calculate schedules without waiting for jobs to run. It supports DAG validation, bounded retries, descendant blocking, and JSON output.

## Quick start

Requirements: a GCC/Clang C++17 compiler and CMake 3.16+. The optional benchmark driver needs Python 3.10+. GitHub Actions builds with sanitizers, runs the tests, and reproduces the benchmark.

From this directory:

```console
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j 2
ctest --test-dir build --output-on-failure
./build/taskgraph examples/pipeline.tasks 2 fifo
./build/taskgraph examples/pipeline.tasks 2 critical
./build/taskgraph examples/recovery.tasks 2 critical
./build/taskgraph examples/heuristic_loses.tasks 2 fifo
./build/taskgraph examples/heuristic_loses.tasks 2 critical
python3 benchmark.py build/taskgraph
```

Output is JSON: overall makespan, total busy worker-time, a fault-free lower bound, final job states, and every executed attempt. A valid simulation returns exit code 0 even when simulated jobs fail; invalid input returns 2 and an error on stderr.

| Input, with two workers | FIFO | Critical-path priority | Behavior |
| --- | ---: | ---: | --- |
| `pipeline.tasks` | 8 | 6 | Start the longer dependency chain early |
| `heuristic_loses.tasks` | 6 | 7 | The greedy rule is not an optimal scheduler |

Times are simulated units. The table compares workload completion time under each policy.

In `recovery.tasks`, `fetch` succeeds on attempt 2, `validate` fails both attempts, `publish` and `notify` become blocked, and the independent backup finishes normally. Critical-policy makespan is 10 units.

## Input contract

Each non-comment line has five whitespace-separated fields:

```text
id duration max_retries failures_before_success dependency_ids
```

Dependencies are comma-separated, or `-` for none. Comment lines start with `#`; inline comments are not supported. IDs contain ASCII letters, digits, underscores, dots, and hyphens, with an alphanumeric/underscore first character. IDs cannot repeat. Durations are positive 64-bit integers; retry budgets are 0..1000; injected failure counts are nonnegative integers. Unknown/duplicate dependencies, cycles, invalid numbers, and overflowing time totals are rejected. An empty file is a valid empty workload.

`max_retries = 1` permits **two attempts total**. A failed attempt consumes its full duration. Retries rejoin the ready queue immediately; there is no backoff or preemption. Each attempt needs one identical worker. Jobs are all submitted at time zero, and durations are assumed known.

## Core design

`Graph` owns validated tasks, adjacency lists, and precomputed critical-path lengths. `Scheduler` owns the state machine and event loop. A `ReadyPolicy` only supplies a priority score, so changing a policy cannot bypass dependency checks. `FifoPolicy` and `CriticalPathPolicy` implement this small interface. `main.cpp` handles input/output.

The graph sorts IDs before construction. Initial ready jobs enter in ID order. FIFO uses enqueue sequence. Critical policy chooses the largest remaining-path score and breaks ties by enqueue sequence. Events with equal completion times are processed in ID order, and **all completions at the same time are processed before dispatching any new attempts**.

The algorithm:

1. Resolve IDs and build adjacency lists and indegrees.
2. Run Kahn's topological sort. If fewer than all jobs are removed, reject the graph as cyclic.
3. In reverse topological order, compute `priority(v) = duration(v) + max(priority(child))`; leaves have their own duration as priority.
4. Maintain a ready max-heap and a completion min-heap. Fill available workers, then jump directly to the earliest completion time.
5. Success decrements children's unresolved dependency counts. A job becomes ready only when all prerequisites have succeeded.
6. Retry recoverable failures. An exhausted retry budget marks the job failed and iteratively marks its descendants blocked.

```text
pending -> ready -> running -> succeeded
                       |  |
                 retry |  +-> failed -> pending descendants become blocked
                       v
                     ready
```

Blocked descendants cannot already be running: one of their prerequisites has never succeeded. Multiple failed ancestors do not repeatedly traverse already-blocked descendants.

Graph and scheduling state use STL containers with automatic lifetime management. The CLI selects a policy through `unique_ptr<ReadyPolicy>`; the interface provides a virtual destructor.

## Complexity and bounds

Let V be jobs, E dependency edges, and R extra executed retry attempts. ID resolution uses `std::map`; deterministic topological traversal uses a heap. Construction is bounded by O((V + E) log V), with O(V + E) graph storage. The critical-path DP itself is O(V + E).

Scheduling is O((V + R) log V + E), ignoring string-comparison costs and using `log(max(V, 2))` for very small graphs. Completion-heap operations are actually bounded by the number of workers. The retained attempt trace costs O(V + R) additional space. A long chain uses an explicit stack for failure propagation, avoiding recursion depth concerns.

For fault-free workloads, any schedule needs at least:

```text
max(longest dependency-chain duration, ceil(sum of durations / workers))
```

The bound may not be attainable under a particular workload. With unlimited workers, the makespan equals the critical path; with one worker, it equals total work. Both properties are checked on 200 seeded DAGs. For injected-failure workloads, this field is `null` because blocked descendants may be skipped.

## Tests and measurements

The C++ tests check known schedules, cycle/malformed-input rejection, overflow, retries, simultaneous completions, shared descendants, and 200 random DAGs. A separate trace validator checks prerequisite completion, interval capacity, attempt numbering, retry limits, final-state reachability, and accounting. Tests throw on failure rather than relying on `assert`, which may disappear in release builds.

The Python benchmark generates 40 synthetic DAGs, each with 40 jobs and four workers, using seeds 0..39. An edge from an earlier to a later job is included with probability 0.08; durations are uniform integers 1..20. These choices guarantee acyclic inputs and define a narrow workload distribution. All raw runs are reported, including wins/ties/losses. The separate five-job counterexample demonstrates that success on this distribution does not establish dominance over FIFO.

## Scope

TaskGraph models a finite batch of jobs submitted at time zero, known durations, and identical workers. Retries are immediate and non-preemptive. It simulates scheduling rather than launching processes or threads. Online arrivals, deadlines, resource-specific requirements, and restart recovery are outside the current model.

FIFO follows readiness order. Critical-path priority is a greedy heuristic that uses estimated durations and does not account for expected retries. The benchmark includes a separate counterexample where FIFO produces a shorter schedule.
