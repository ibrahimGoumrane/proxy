import argparse
import csv
from pathlib import Path


def normalize_name(value: str) -> str:
	return " ".join(str(value).split()).casefold()


def filter_unique_names(input_path: Path, output_path: Path) -> int:
	with input_path.open("r", encoding="utf-8", newline="") as csv_file:
		reader = csv.DictReader(csv_file)
		fieldnames = reader.fieldnames or []
		rows = []
		seen = set()
		for row in reader:
			name = normalize_name(row.get("name", ""))
			if not name or name in seen:
				continue
			seen.add(name)
			rows.append(row)

	temp_path = output_path.with_suffix(output_path.suffix + ".tmp")
	with temp_path.open("w", encoding="utf-8", newline="") as csv_file:
		writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
		writer.writeheader()
		writer.writerows(rows)

	temp_path.replace(output_path)
	return len(rows)


def main() -> None:
	parser = argparse.ArgumentParser(description="Keep only unique contact names in a CSV file.")
	parser.add_argument("input", nargs="?", default="matched_contacts.csv")
	parser.add_argument("output", nargs="?", default=None)
	args = parser.parse_args()

	input_path = Path(args.input)
	output_path = Path(args.output) if args.output else input_path
	count = filter_unique_names(input_path, output_path)
	print(f"Wrote {count} unique rows to {output_path}")


if __name__ == "__main__":
	main()