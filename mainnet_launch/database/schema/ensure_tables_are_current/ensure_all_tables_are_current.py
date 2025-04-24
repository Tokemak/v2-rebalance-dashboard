from mainnet_launch.database.schema.ensure_tables_are_current.update_destinations_table import (
    ensure_destinations_are_current,
)
from mainnet_launch.database.schema.ensure_tables_are_current.update_autopools_table import ensure_autopools_is_current


def main():
    ensure_destinations_are_current()
    ensure_autopools_is_current()


if __name__ == "__main__":
    main()
