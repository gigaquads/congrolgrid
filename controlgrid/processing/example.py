if __name__ == "__main__":
    from time import sleep

    from .dispatcher import JobDispatcher
    from .consumer import JobResultConsumer
    from .job import Job, JobResult

    class CustomConsumer(JobResultConsumer):
        def on_result(self, result: JobResult):
            print(f"\n{result}\n")
            if result.is_complete:
                for line in result.stdout:
                    print(line)

    worker = JobDispatcher()

    consumer = CustomConsumer()
    consumer.start()

    while True:
        sleep(0.1)
        parts = input(">>> ").split()
        sleep(0.1)
        if parts:
            command, args = parts[0], parts[1:]
            job = Job(command, args)
            worker.submit(job)
