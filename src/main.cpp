#include "taskgraph.hpp"

#include <algorithm>
#include <fstream>
#include <iostream>
#include <memory>
#include <stdexcept>

int main(int argc, char** argv) {
    try {
        if (argc != 4) {
            std::cerr << "Usage: taskgraph TASK_FILE WORKERS fifo|critical\n";
            return 2;
        }
        const std::string worker_text(argv[2]);
        if (worker_text.empty() || worker_text.find_first_not_of("0123456789") != std::string::npos) {
            throw std::invalid_argument("workers must be an integer in [1, 1000000]");
        }
        const auto workers = std::stoull(worker_text);
        if (workers == 0 || workers > 1000000) throw std::invalid_argument("workers out of range");
        std::unique_ptr<taskgraph::ReadyPolicy> policy;
        const std::string policy_name(argv[3]);
        if (policy_name == "fifo") policy = std::make_unique<taskgraph::FifoPolicy>();
        else if (policy_name == "critical") policy = std::make_unique<taskgraph::CriticalPathPolicy>();
        else throw std::invalid_argument("policy must be fifo or critical");
        std::ifstream input(argv[1]);
        if (!input) throw std::runtime_error("cannot open task file");
        const auto graph = taskgraph::Graph::read(input);
        const auto result = taskgraph::Scheduler().run(graph, workers, *policy);
        bool fault_free = true;
        for (const auto& task : graph.tasks()) if (task.failures_before_success) fault_free = false;
        const auto total = graph.total_work();
        const auto work_bound = total / static_cast<taskgraph::Time>(workers) + (total % workers != 0);
        std::cout << "{\"policy\":\"" << policy_name << "\",\"workers\":" << workers
                  << ",\"makespan\":" << result.makespan << ",\"busy_time\":" << result.busy_time
                  << ",\"fault_free_lower_bound\":";
        if (fault_free) std::cout << std::max(graph.critical_path(), work_bound);
        else std::cout << "null";
        std::cout << ",\"tasks\":[";
        for (std::size_t i = 0; i < graph.tasks().size(); ++i) {
            const auto& outcome = result.outcomes[i];
            if (i) std::cout << ',';
            std::cout << "{\"id\":\"" << graph.tasks()[i].id << "\",\"state\":\""
                      << taskgraph::to_string(outcome.state) << "\",\"attempts\":" << outcome.attempts
                      << ",\"first_start\":" << outcome.first_start << ",\"finish\":" << outcome.finish << '}';
        }
        std::cout << "],\"trace\":[";
        for (std::size_t i = 0; i < result.trace.size(); ++i) {
            const auto& attempt = result.trace[i];
            if (i) std::cout << ',';
            std::cout << "{\"id\":\"" << graph.tasks()[attempt.task].id << "\",\"attempt\":" << attempt.number
                      << ",\"start\":" << attempt.start << ",\"finish\":" << attempt.finish
                      << ",\"succeeded\":" << (attempt.succeeded ? "true" : "false") << '}';
        }
        std::cout << "]}\n";
        return 0; // Simulated job failures are data; malformed invocations return 2.
    } catch (const std::exception& error) {
        std::cerr << "error: " << error.what() << '\n';
        return 2;
    }
}
