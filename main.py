"""handles cli commands"""
import sys
import argparse
from mylib.extract import extract
from mylib.transform_load import load
from mylib.query import (
    general_query,
)


def handle_arguments(args):
    """add action based on inital call"""
    parser = argparse.ArgumentParser(description="Complex SQL query script")
    parser.add_argument(
        "action",
        choices=[
            "extract",
            "transform_load",
            "general_query",
        ],
    )
    args = parser.parse_args(args[:1])
    print(args.action)

    if args.action == "general_query":
        parser.add_argument("query")

    # parse again with ever
    return parser.parse_args(sys.argv[1:])


def main():
    """handles all the cli commands"""
    args = handle_arguments(sys.argv[1:])

    if args.action == "extract":
        print("Extracting data...")
        extract()
    elif args.action == "transform_load":
        print("Transforming data...")
        load()
    elif args.action == "general_query":
        general_query(args.query)

    else:
        print(f"Unknown action: {args.action}")


if __name__ == "__main__":
    main()
