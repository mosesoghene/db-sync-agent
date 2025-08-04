import sys
import argparse
import logging

from core.config import load_config
from core.scheduler.jobs import start_sync_scheduler


def run_cli():
    config = load_config()
    node_id = config["node_id"]
    start_sync_scheduler(config, node_id)

    try:
        while True:
            pass
    except KeyboardInterrupt:
        logger = logging.getLogger("sync_agent")
        logger.info("🛑 Exiting sync agent")


def run_gui():
    from gui.gui_main import main as run_gui_main
    run_gui_main()


if __name__ == "__main__":
    if getattr(sys, 'frozen', False):
        # Frozen binary = assume GUI
        run_gui()
    else:
        # If running from source
        parser = argparse.ArgumentParser(description="DB Sync Agent")
        parser.add_argument("--gui", action="store_true", help="Run the sync agent with GUI")
        args = parser.parse_args()

        if args.gui:
            run_gui()
        else:
            run_cli()
