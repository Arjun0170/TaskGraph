#include "taskgraph.hpp"

#include <algorithm>
#include <functional>
#include <limits>
#include <map>
#include <queue>
#include <regex>
#include <set>
#include <sstream>
#include <stdexcept>
#include <tuple>

namespace taskgraph {
namespace {
Time checked_add(Time a, Time b) {
    if (b > std::numeric_limits<Time>::max() - a) {
        throw std::overflow_error("duration total exceeds 64-bit range");
    }
    return a + b;
}
}

Graph::Graph(std::vector<Task> tasks) : tasks_(std::move(tasks)) {
    std::sort(tasks_.begin(), tasks_.end(), [](const Task& a, const Task& b) {
        return a.id < b.id;
    });
    std::map<std::string, std::size_t> index;
    const std::regex valid_id("[A-Za-z0-9_][A-Za-z0-9_.-]*");
    for (std::size_t i = 0; i < tasks_.size(); ++i) {
        const auto& task = tasks_[i];
        if (!std::regex_match(task.id, valid_id) || task.duration <= 0 ||
            task.max_retries < 0 || task.max_retries > 1000 ||
            task.failures_before_success < 0) {
            throw std::invalid_argument("invalid task fields: " + task.id);
        }
        if (!index.emplace(task.id, i).second) {
            throw std::invalid_argument("duplicate task: " + task.id);
        }
    }
    children_.resize(tasks_.size());
    std::vector<std::size_t> indegree(tasks_.size());
    for (std::size_t i = 0; i < tasks_.size(); ++i) {
        std::set<std::string> seen;
        for (const auto& dep : tasks_[i].dependencies) {
            if (index.count(dep) == 0) throw std::invalid_argument("unknown dependency: " + dep);
            if (!seen.insert(dep).second) throw std::invalid_argument("duplicate dependency: " + dep);
            children_[index.at(dep)].push_back(i);
            ++indegree[i];
        }
    }
    // Kahn's algorithm both validates the DAG and provides the DP order.
    std::priority_queue<std::size_t, std::vector<std::size_t>, std::greater<>> ready;
    for (std::size_t i = 0; i < tasks_.size(); ++i) if (indegree[i] == 0) ready.push(i);
    std::vector<std::size_t> order;
    while (!ready.empty()) {
        const auto current = ready.top();
        ready.pop();
        order.push_back(current);
        for (auto child : children_[current]) if (--indegree[child] == 0) ready.push(child);
    }
    if (order.size() != tasks_.size()) throw std::invalid_argument("dependency cycle detected");
    critical_paths_.resize(tasks_.size());
    for (auto it = order.rbegin(); it != order.rend(); ++it) {
        Time tail = 0;
        for (auto child : children_[*it]) tail = std::max(tail, critical_paths_[child]);
        critical_paths_[*it] = checked_add(tasks_[*it].duration, tail);
    }
    (void)total_work(); // Reject overflow even for independent jobs.
}

Graph Graph::read(std::istream& input) {
    std::vector<Task> tasks;
    std::string line;
    std::size_t line_number = 0;
    while (std::getline(input, line)) {
        ++line_number;
        if (line.find_first_not_of(" \t\r") == std::string::npos ||
            line.at(line.find_first_not_of(" \t\r")) == '#') continue;
        std::istringstream row(line);
        Task task;
        std::string dependencies, extra;
        if (!(row >> task.id >> task.duration >> task.max_retries >>
              task.failures_before_success >> dependencies) || row >> extra) {
            throw std::invalid_argument("expected five fields at line " + std::to_string(line_number));
        }
        if (dependencies != "-") {
            if (dependencies.back() == ',') throw std::invalid_argument("trailing dependency comma");
            std::istringstream deps(dependencies);
            std::string dep;
            while (std::getline(deps, dep, ',')) task.dependencies.push_back(dep);
        }
        tasks.push_back(std::move(task));
    }
    if (input.bad()) throw std::runtime_error("could not read task input");
    return Graph(std::move(tasks));
}

Time Graph::critical_path() const {
    return critical_paths_.empty() ? 0 : *std::max_element(critical_paths_.begin(), critical_paths_.end());
}
Time Graph::total_work() const {
    Time total = 0;
    for (const auto& task : tasks_) total = checked_add(total, task.duration);
    return total;
}

std::string to_string(State state) {
    switch (state) {
    case State::pending: return "pending";
    case State::ready: return "ready";
    case State::running: return "running";
    case State::succeeded: return "succeeded";
    case State::failed: return "failed";
    case State::blocked: return "blocked";
    }
    throw std::logic_error("unknown task state");
}

Result Scheduler::run(const Graph& graph, std::size_t workers, const ReadyPolicy& policy) const {
    if (workers == 0) throw std::invalid_argument("workers must be positive");
    const auto& tasks = graph.tasks();
    Result result;
    result.outcomes.resize(tasks.size());
    std::vector<std::size_t> remaining;
    for (const auto& task : tasks) remaining.push_back(task.dependencies.size());
    struct Ready {
        Time rank;
        std::size_t sequence;
        std::size_t task;
        bool operator<(const Ready& other) const {
            if (rank != other.rank) return rank < other.rank;
            return sequence > other.sequence;
        }
    };
    std::priority_queue<Ready> ready;
    // Finish time then task index gives deterministic ordering for simultaneous events.
    using Event = std::tuple<Time, std::size_t, Time>;
    std::priority_queue<Event, std::vector<Event>, std::greater<>> events;
    std::size_t sequence = 0;
    auto enqueue = [&](std::size_t task) {
        result.outcomes[task].state = State::ready;
        ready.push({policy.rank(graph, task), sequence++, task});
    };
    for (std::size_t i = 0; i < tasks.size(); ++i) if (remaining[i] == 0) enqueue(i);
    Time now = 0;
    while (!ready.empty() || !events.empty()) {
        while (!ready.empty() && events.size() < workers) {
            const auto id = ready.top().task;
            ready.pop();
            auto& outcome = result.outcomes[id];
            if (outcome.state != State::ready) continue;
            outcome.state = State::running;
            ++outcome.attempts;
            if (outcome.first_start == -1) outcome.first_start = now;
            events.emplace(checked_add(now, tasks[id].duration), id, now);
        }
        if (events.empty()) break;
        now = std::get<0>(events.top());
        // Drain ALL completions at this timestamp before dispatching more work.
        do {
            const auto [finish, id, start] = events.top();
            events.pop();
            auto& outcome = result.outcomes[id];
            const bool success = outcome.attempts > tasks[id].failures_before_success;
            result.trace.push_back({id, outcome.attempts, start, finish, success});
            result.busy_time = checked_add(result.busy_time, tasks[id].duration);
            if (success) {
                outcome.state = State::succeeded;
                outcome.finish = now;
                for (auto child : graph.children()[id]) {
                    if (--remaining[child] == 0 && result.outcomes[child].state == State::pending) {
                        enqueue(child);
                    }
                }
            } else if (outcome.attempts <= tasks[id].max_retries) {
                enqueue(id);
            } else {
                outcome.state = State::failed;
                outcome.finish = now;
                std::vector<std::size_t> stack(graph.children()[id]);
                while (!stack.empty()) {
                    const auto child = stack.back();
                    stack.pop_back();
                    auto& descendant = result.outcomes[child];
                    if (descendant.state == State::blocked) continue;
                    descendant.state = State::blocked;
                    descendant.finish = now;
                    for (auto next : graph.children()[child]) stack.push_back(next);
                }
            }
        } while (!events.empty() && std::get<0>(events.top()) == now);
    }
    result.makespan = now;
    return result;
}
} // namespace taskgraph
