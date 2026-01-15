"""CLI entrypoint for FEED -> Jetshop sync."""

from __future__ import annotations

import argparse
import sys
import uuid
from pathlib import Path

from .config import load_config
from .discovery import discover_mapping
from .feed_client import FeedClient
from .jetshop_client import JetshopClient
from .logging_setup import setup_logging
from .mapping_loader import MappingError, load_mapping
from .state_store import StateStore
from .sync_engine import SyncEngine


def main() -> int:
    parser = argparse.ArgumentParser(description="MrPlant FEED -> Jetshop sync")
    subparsers = parser.add_subparsers(dest="command", required=True)

    sync_parser = subparsers.add_parser("sync", help="Run synchronization")
    sync_parser.add_argument("--since", help="ISO timestamp for exportFrom")
    sync_parser.add_argument("--productNo", help="Single product number")
    sync_parser.add_argument("--limit", type=int, help="Limit number of products")
    sync_parser.add_argument("--dry-run", action="store_true", help="Dry-run without writes")
    sync_parser.add_argument("--mapping", help="Override mapping file path")

    discover_parser = subparsers.add_parser("discover-mapping", help="Discover unmapped fields")
    discover_parser.add_argument("--since", help="ISO timestamp for exportFrom")
    discover_parser.add_argument("--productNo", help="Single product number")
    discover_parser.add_argument("--mapping", help="Override mapping file path")

    validate_parser = subparsers.add_parser("validate-mapping", help="Validate mapping file")
    validate_parser.add_argument("--mapping", help="Override mapping file path")

    args = parser.parse_args()

    config = load_config()
    mapping_path = args.mapping or config.mapping_file

    run_id = str(uuid.uuid4())
    logger = setup_logging(config.log_file, config.log_level, run_id)
    state_store = StateStore(Path("state/last_run.json"))

    try:
        mapping = load_mapping(mapping_path)
    except MappingError as exc:
        logger.error("mapping_invalid", extra={"event": "mapping_invalid", "detail": str(exc)})
        print(f"Mapping validation failed: {exc}")
        return 2

    if args.command == "validate-mapping":
        print("Mapping validation: OK")
        return 0

    export_from = args.since or state_store.read_last_run()
    if not export_from:
        print("Missing --since and no last_run.json found")
        return 2

    feed_client = FeedClient(config, logger)
    jetshop_client = JetshopClient(config, logger)

    if args.command == "discover-mapping":
        suggestions = discover_mapping(
            feed_client,
            jetshop_client,
            mapping,
            export_from,
            args.productNo,
        )
        print(f"Mapping suggestions written with {len(suggestions['unmapped_attributes'])} attributes.")
        return 0

    if args.command == "sync":
        engine = SyncEngine(feed_client, jetshop_client, mapping, logger, state_store)
        engine.sync(export_from, args.productNo, args.limit, args.dry_run)
        return 0

    return 1


if __name__ == "__main__":
    sys.exit(main())
