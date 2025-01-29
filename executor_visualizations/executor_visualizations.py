import matplotlib.pyplot as plt
import re
from datetime import datetime


def parse_log_file(log_file):
    task_distribution = {}  # executor_id : task_count
    time_distribution = {}  # executor_id : total_time_ms

    with open(log_file, 'r') as f:
        for line in f:
            # Track finished tasks
            task_match = re.search(r"Finished task .+ in (\d+) ms on .+ \(executor (\d+)\)", line)
            if task_match:
                duration = int(task_match.group(1))
                executor_id = task_match.group(2)
                task_distribution[executor_id] = task_distribution.get(executor_id, 0) + 1
                time_distribution[executor_id] = time_distribution.get(executor_id, 0) + duration

    return task_distribution, time_distribution


def plot_task_distribution(task_distribution):
    plt.figure()
    sorted_items = sorted(task_distribution.items(), key=lambda x: int(x[0]))
    executors, tasks = zip(*sorted_items)
    plt.bar(executors, tasks)
    plt.title('Task Distribution Across Executors')
    plt.ylabel('Executor ID')
    plt.xlabel('Number of Tasks')
    plt.grid(axis='y')

    plt.tight_layout()
    plt.savefig('task_distribution.png')
    plt.close()


def plot_time_distribution(time_distribution):
    plt.figure()
    time_distribution_seconds = {k: v / 1000 for k, v in time_distribution.items()}
    sorted_items = sorted(time_distribution_seconds.items(), key=lambda x: int(x[0]))
    executors, times = zip(*sorted_items)

    plt.bar(executors, times)
    plt.title('Total Working Time per Executor')
    plt.ylabel('Total Time (seconds)')
    plt.xlabel('Executor ID')
    plt.grid(axis='y')

    plt.tight_layout()
    plt.savefig('time_distribution.png')
    plt.close()


log_file = "full_execution.log"
task_distribution, time_distribution = parse_log_file(log_file)
plot_task_distribution(task_distribution)
plot_time_distribution(time_distribution)