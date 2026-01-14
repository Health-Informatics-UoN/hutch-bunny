import argparse

parser = argparse.ArgumentParser(
    prog="bunny-cli",
    description="This program takes a JSON string containing an RQuest query and solves it.",
)

group = parser.add_mutually_exclusive_group(required=True)
group.add_argument(
    "--body",
    dest="body",
    help="The JSON file containing the query",
)
group.add_argument(
    "--body-json",
    dest="body_json",
    help="The JSON query as an inline string",
)

parser.add_argument(
    "-o",
    "--output",
    dest="output",
    required=False,
    type=str,
    default="output.json",
    help="The path to the output file",
)

parser.add_argument(
    "-m",
    "--modifiers",
    dest="results_modifiers",
    required=False,
    type=str,
    default="[]",  # when parsed will produce an empty list
    help="The results modifiers",
)
