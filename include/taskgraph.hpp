#pragma once

#include <cstdint>
#include <istream>
#include <string>
#include <vector>

namespace taskgraph {
using Time = std::int64_t;

struct Task {
    std::string id;
    Time duration;
    int max_retries = 0;
    int failures_before_success = 0; // Deterministic fault injection, not randomness.
    std::vector<std::string> dependencies;
};

class Graph {
public:
    explicit Graph(std::vector<Task> tasks);
    static Graph read(std::istream& input);
    const std::vector<Task>& tasks() const { return tasks_; }
    const std::vector<std::vector<std::size_t>>& children() const { return children_; }
    const std::vector<Time>& critical_paths() const { return critical_paths_; }
    Time critical_path() const;
    Time total_work() const;
private:
    std::vector<Task> tasks_;
    std::vector<std::vector<std::size_t>> children_;
    std::vector<Time> critical_paths_;
};

// Scheduling owns state transitions; a policy only ranks eligible work.
class ReadyPolicy {
public:
    virtual ~ReadyPolicy() = default;
    virtual Time rank(const Graph& graph, std::size_t task) const = 0;
};
class FifoPolicy final : public ReadyPolicy {
public:
    Time rank(const Graph&, std::size_t) const override { return 0; }
};
class CriticalPathPolicy final : public ReadyPolicy {
public:
    Time rank(const Graph& graph, std::size_t task) const override {
        return graph.critical_paths().at(task);
    }
};

enum class State { pending, ready, running, succeeded, failed, blocked };
std::string to_string(State state);
struct Outcome {
    State state = State::pending;
    int attempts = 0;
    Time first_start = -1;
    Time finish = -1; // For blocked tasks: time the blocking failure became known.
};
struct Attempt {
    std::size_t task;
    int number;
    Time start;
    Time finish;
    bool succeeded;
};
struct Result {
    std::vector<Outcome> outcomes;
    std::vector<Attempt> trace;
    Time makespan = 0;
    Time busy_time = 0;
};
class Scheduler {
public:
    Result run(const Graph& graph, std::size_t workers, const ReadyPolicy& policy) const;
};
} // namespace taskgraph
