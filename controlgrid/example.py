from threading import Thread
from time import sleep

from controlgrid.processing.dispatcher import JobDispatcher
from controlgrid.processing.data import Job, OutputLine
from controlgrid.log import log


def main() -> None:
    dispatcher = JobDispatcher()

    def consume():
        """Just print each line of STDOUT as it comes in."""
        while True:
            dispatcher.consume(
                lambda line: log.info(
                    f"{line.data.line_no:-3}: {line.data.text}"
                )
                if line.data
                else line.exit_code
            )
            sleep(0.1)

    # start consumer thread
    consumer = Thread(target=consume, daemon=True)
    consumer.start()

    #
    while True:
        # read command from STDIN and split into command + arguments
        raw_cmd = input("command >>> ")
        raw_cmd_parts = raw_cmd.strip().split()

        if not (raw_cmd_parts and raw_cmd_parts[0]):
            continue

        cmd, args = raw_cmd_parts[0], raw_cmd_parts[1:]

        # dispatch job for execution in daemon subprocess
        dispatcher.dispatch(Job.create(cmd, args))

        sleep(0.2)


if __name__ == "__main__":
    main()
