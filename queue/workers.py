"""Run several workers against one queue, each in its own thread.

Each worker loops on process_next, and the claim is atomic, so two workers
never run the same job. Start workers from the command line with:
python -m queue.workers 4
"""
import sys
import threading
import time

from queue.jobs import JobQueue


class Worker:
    """One worker thread, polling the queue until asked to stop."""

    def __init__(self, queue, worker_id, poll_interval=0.05):
        self.queue = queue
        self.worker_id = worker_id
        self.poll_interval = poll_interval
        self.processed = 0
        self._stop = threading.Event()
        self._thread = None

    def run_once(self):
        job = self.queue.process_next(worker_id=self.worker_id)
        if job is not None:
            self.processed += 1
            return True
        return False

    def run(self):
        while not self._stop.is_set():
            if not self.run_once():
                time.sleep(self.poll_interval)

    def run_until_empty(self):
        while self.run_once():
            pass

    def start(self):
        self._thread = threading.Thread(
            target=self.run, name=self.worker_id, daemon=True
        )
        self._thread.start()
        return self

    def stop(self):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)


def run_workers(queue, count, poll_interval=0.05):
    """Start count workers against one queue, return the list of workers."""
    workers = [
        Worker(queue, f"worker-{i + 1}", poll_interval) for i in range(count)
    ]
    for worker in workers:
        worker.start()
    return workers


def main(argv=None):
    args = list(sys.argv[1:] if argv is None else argv)
    count = int(args[0]) if args else 4
    queue = JobQueue()
    workers = run_workers(queue, count)
    print(f"Started {count} workers, press Ctrl+C to stop")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        for worker in workers:
            worker.stop()
        total = sum(worker.processed for worker in workers)
        print(f"Stopped, processed {total} jobs in total")


if __name__ == "__main__":
    main()
