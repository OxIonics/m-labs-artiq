import argparse
import asyncio
import logging
import signal
import sys

from artiq.consts import WORKER_MANAGER_PORT
from artiq.master.worker_transport import PipeWorkerTransport
from artiq.test_tools.thread_worker_transport import ThreadWorkerTransport
from artiq.worker_manager.worker_manager import GracefulExit, WorkerManager


def raise_keyboard_interrupt(_handlers, _frame):
    raise KeyboardInterrupt("Term")


def main():
    parser = argparse.ArgumentParser(
        "Runs a \"worker manager\", which allows the artiq master to run "
        "experiment code here rather than in its own environment."
    )
    parser.add_argument(
        "--id",
        help="A globally unique id for the worker manager. The default is to "
             "generate a new uuid4. This is normally used by other applications "
             "so that they know the id of the worker manager to use it."
    )
    parser.add_argument(
        "--port",
        help="The port to connect to on the master",
        default=WORKER_MANAGER_PORT,
    )
    parser.add_argument(
        "--exit-on-idle",
        default=False,
        action="store_true",
        help="This process will exit when the number of workers drops to zero"
    )
    parser.add_argument(
        "-v", "--verbose", default=0, action="count",
        help="increase logging level. -v for info -vv for debug",
    )
    parser.add_argument(
        "--thread-worker",
        default=False,
        action="store_true",
        help="Run the workers as threads not processes. "
             "Can be useful for debugging."
    )
    parser.add_argument(
        "--parent",
        help="Report a parent in the worker manager metadata. "
             "Purely informational"
    )
    parser.add_argument(
        "description",
        help="The human readable description for the worker manager"
    )
    parser.add_argument(
        "master",
        help="IP address or hostname of of the artiq master",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.WARNING - args.verbose * 10,
        format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
    )

    # Treat SIGTERM the same as SIGINT
    signal.signal(signal.SIGTERM, raise_keyboard_interrupt)

    if args.thread_worker:
        transport_factory = ThreadWorkerTransport
    else:
        transport_factory = PipeWorkerTransport

    loop = asyncio.get_event_loop()
    mgr = loop.run_until_complete(
        WorkerManager.create(
            args.master,
            args.port,
            args.id,
            args.description,
            exit_on_idle=args.exit_on_idle,
            transport_factory=transport_factory,
            parent=args.parent,
        )
    )
    try:
        loop.run_until_complete(mgr.wait_for_exit())
    except KeyboardInterrupt:
        logging.info("Exiting")
        loop.run_until_complete(mgr.stop())
    except GracefulExit:
        pass
    except:
        logging.exception("Unhandled exception in receive loop")
        sys.exit(1)


if __name__ == '__main__':
    main()



