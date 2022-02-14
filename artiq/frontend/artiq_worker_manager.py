import argparse
import asyncio
import logging

from artiq.worker_manager.worker_manager import WorkerManager


def main():
    parser = argparse.ArgumentParser(
        "Runs a \"worker manager\", which allows the artiq master to run "
        "experiment code here rather than in it's own environment."
    )
    parser.add_argument(
        "--id",
        help="A globally unique id for the worker manager. The default is to "
             "generate a new uuid4."
    )
    parser.add_argument(
        "--port",
        help="The port to connect to on the master",
        default=3252,
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

    logging.basicConfig(level=logging.INFO)

    loop = asyncio.get_event_loop()
    mgr = loop.run_until_complete(WorkerManager.create(
        args.master,
        args.port,
        args.id,
        args.description,
    ))
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        logging.info("Exiting")
    finally:
        loop.run_until_complete(mgr.close())


if __name__ == '__main__':
    main()



