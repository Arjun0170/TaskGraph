#include "taskgraph.hpp"

#include <algorithm>
#include <functional>
#include <iostream>
#include <limits>
#include <map>
#include <random>
#include <sstream>
#include <stdexcept>

using namespace taskgraph;
namespace {
int checks = 0;
void require(bool condition, const std::string& message) {
    ++checks;
    if (!condition) throw std::runtime_error(message);
}
void rejects(const std::function<void()>& fn) {
    bool threw = false;
    try { fn(); } catch (const std::exception&) { threw = true; }
    require(threw, "expected rejection");
}
Graph parse(const std::string& text) {
    std::istringstream stream(text);
    return Graph::read(stream);
}

// Check the trace using constraints, independently of the scheduler's queues.
void verify(const Graph& graph, const Result& result, std::size_t workers) {
    std::map<std::string, std::size_t> index;
    std::vector<int> counts(graph.tasks().size());
    std::vector<std::pair<Time, int>> occupancy;
    std::vector<Time> previous_finish(graph.tasks().size(), -1);
    Time busy = 0, finish = 0;
    for (std::size_t i = 0; i < graph.tasks().size(); ++i) index[graph.tasks()[i].id] = i;
    for (const auto& attempt : result.trace) {
        const auto& task = graph.tasks()[attempt.task];
        require(attempt.finish - attempt.start == task.duration, "attempt duration");
        require(attempt.start >= previous_finish[attempt.task], "overlapping retries");
        require(attempt.number == ++counts[attempt.task], "attempt numbering");
        require(attempt.succeeded == (attempt.number > task.failures_before_success), "fault injection");
        previous_finish[attempt.task] = attempt.finish;
        for (const auto& dependency : task.dependencies) {
            const auto& parent = result.outcomes[index.at(dependency)];
            require(parent.state == State::succeeded && parent.finish <= attempt.start, "dependency violated");
        }
        occupancy.emplace_back(attempt.start, 1);
        occupancy.emplace_back(attempt.finish, -1);
        busy += task.duration;
        finish = std::max(finish, attempt.finish);
    }
    std::sort(occupancy.begin(), occupancy.end()); // End (-1) before start (+1).
    int running = 0;
    for (const auto& event : occupancy) {
        running += event.second;
        require(running >= 0 && static_cast<std::size_t>(running) <= workers, "worker capacity");
    }
    require(running == 0 && busy == result.busy_time && finish == result.makespan, "aggregate metrics");
    // Independent reachability oracle: descend from jobs that cannot succeed.
    std::vector<bool> blocked(graph.tasks().size());
    for (std::size_t root = 0; root < graph.tasks().size(); ++root) {
        const auto& task = graph.tasks()[root];
        if (task.failures_before_success <= task.max_retries) continue;
        std::vector<std::size_t> stack(graph.children()[root]);
        while (!stack.empty()) {
            const auto node = stack.back();
            stack.pop_back();
            if (blocked[node]) continue;
            blocked[node] = true;
            for (auto child : graph.children()[node]) stack.push_back(child);
        }
    }
    for (std::size_t i = 0; i < graph.tasks().size(); ++i) {
        const auto& task = graph.tasks()[i];
        const auto& outcome = result.outcomes[i];
        const auto expected = blocked[i] ? State::blocked :
            (task.failures_before_success > task.max_retries ? State::failed : State::succeeded);
        require(outcome.state == expected, "terminal state");
        require(outcome.attempts == counts[i], "trace completeness");
        require(outcome.attempts == (blocked[i] ? 0 : std::min(task.failures_before_success + 1, task.max_retries + 1)), "retry limit");
        if (blocked[i]) require(outcome.first_start == -1, "blocked task ran");
        else require(outcome.finish == previous_finish[i], "finish timestamp");
    }
}
}

int main() {
    try {
        Scheduler scheduler;
        FifoPolicy fifo;
        CriticalPathPolicy critical;
        auto demo = parse("a 2 0 0 -\nb 2 0 0 -\nc 2 0 0 -\nd 2 0 0 c\ne 2 0 0 d\n");
        require(scheduler.run(demo, 2, fifo).makespan == 8, "FIFO fixture");
        require(scheduler.run(demo, 2, critical).makespan == 6, "critical fixture");
        require(demo.critical_path() == 6 && demo.total_work() == 10, "graph metrics");
        auto counterexample = parse("a 3 0 0 -\nb 2 0 0 -\nc 2 0 0 -\nd 3 0 0 -\ne 2 0 0 -\n");
        require(scheduler.run(counterexample, 2, fifo).makespan == 6, "FIFO wins counterexample");
        require(scheduler.run(counterexample, 2, critical).makespan == 7, "heuristic is not optimal");
        // Both roots finish at t=1. Newly ready c must beat queued z under critical policy.
        auto simultaneous = parse("a 1 0 0 -\nb 1 0 0 -\nc 5 0 0 a,b\nz 1 0 0 -\n");
        auto simultaneous_result = scheduler.run(simultaneous, 2, critical);
        require(simultaneous_result.outcomes[2].first_start == 1, "completion batching");
        auto failure = parse("a 2 1 2 -\nb 1 0 0 a\nc 1 0 0 a\nd 1 0 0 b,c\ne 7 0 0 -\n");
        verify(failure, scheduler.run(failure, 2, critical), 2);
        auto recovery = parse("a 3 1 1 -\nb 2 0 0 a\n");
        require(scheduler.run(recovery, 2, fifo).makespan == 8, "retry then success");
        verify(recovery, scheduler.run(recovery, 2, fifo), 2);
        require(scheduler.run(Graph({}), 1, fifo).makespan == 0, "empty graph");
        rejects([&] { scheduler.run(demo, 0, fifo); });
        for (const std::string bad : {"a 1 0 0 b", "a 0 0 0 -", "a -1 0 0 -", "a 1 -1 0 -",
             "a 1 1001 0 -", "a 1 0 -1 -", "a 1 0 0 a", "a 1 0 0 -\na 1 0 0 -",
             "a 1 0 0 b\nb 1 0 0 a", "a 1 0 0 -\nb 1 0 0 a,a", "a 1 0 0 - extra",
             "a 1 0 0 b,", "a 1 0", "a\" 1 0 0 -", "a 9223372036854775808 0 0 -"}) {
            rejects([&] { parse(bad); });
        }
        rejects([] { Graph({{"a", std::numeric_limits<Time>::max()}, {"b", 1}}); });
        rejects([&] { scheduler.run(Graph({{"a", std::numeric_limits<Time>::max(), 1, 1}}), 1, fifo); });

        std::mt19937 random(20261021);
        for (int trial = 0; trial < 200; ++trial) {
            std::vector<Task> tasks;
            const int size = 1 + random() % 25;
            for (int i = 0; i < size; ++i) {
                Task task{"job_" + std::to_string(i), static_cast<Time>(1 + random() % 10), 2, static_cast<int>(random() % 4), {}};
                for (int j = 0; j < i; ++j) if (random() % 5 == 0) task.dependencies.push_back("job_" + std::to_string(j));
                tasks.push_back(task);
            }
            const std::size_t workers = 1 + random() % 6;
            const Graph faulty(tasks);
            verify(faulty, scheduler.run(faulty, workers, fifo), workers);
            verify(faulty, scheduler.run(faulty, workers, critical), workers);
            for (auto& task : tasks) task.failures_before_success = 0;
            const Graph clean(tasks);
            require(scheduler.run(clean, 1, critical).makespan == clean.total_work(), "serial work oracle");
            require(scheduler.run(clean, tasks.size(), critical).makespan == clean.critical_path(), "unlimited workers oracle");
            const auto run = scheduler.run(clean, workers, critical);
            require(run.makespan >= clean.critical_path(), "critical path lower bound");
            require(run.makespan >= clean.total_work() / static_cast<Time>(workers), "work lower bound");
            verify(clean, run, workers);
        }
        std::cout << "PASS: " << checks << " checks, including 200 seeded randomized DAGs\n";
    } catch (const std::exception& error) {
        std::cerr << "FAIL: " << error.what() << '\n';
        return 1;
    }
}
