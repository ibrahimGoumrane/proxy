import json
from datetime import datetime, timezone
from pathlib import Path


def load_json(json_path: Path) -> dict:
	with json_path.open("r", encoding="utf-8") as file:
		return json.load(file)


def save_json(json_path: Path, data: dict | list) -> None:
	json_path.parent.mkdir(parents=True, exist_ok=True)
	with json_path.open("w", encoding="utf-8") as file:
		json.dump(data, file, ensure_ascii=False, indent=2)


def load_id_list(ids_file: Path) -> list[str]:
	if not ids_file.exists():
		return []
	data = load_json(ids_file)
	if not isinstance(data, list):
		return []

	cleaned: list[str] = []
	seen: set[str] = set()
	for item in data:
		value = str(item).strip()
		if value and value not in seen:
			cleaned.append(value)
			seen.add(value)
	return cleaned


def save_id_list(ids_file: Path, ids: list[str]) -> None:
	# Preserve order because this file acts as a queue/stack checkpoint.
	cleaned: list[str] = []
	seen: set[str] = set()
	for item in ids:
		value = str(item).strip()
		if value and value not in seen:
			cleaned.append(value)
			seen.add(value)
	save_json(ids_file, cleaned)


def load_processed_ids(processed_ids_file: Path) -> set[str]:
	if not processed_ids_file.exists():
		return set()
	data = load_json(processed_ids_file)
	if isinstance(data, list):
		return {str(item).strip() for item in data if str(item).strip()}
	return set()


def save_processed_ids(processed_ids_file: Path, processed_ids: set[str]) -> None:
	save_json(processed_ids_file, sorted(processed_ids))


def save_page_state(
	page_state_file: Path,
	page: int,
	next_url: str | None,
	has_next: bool,
	counters: dict,
	current_seed_external_id: str | None = None,
) -> None:
	state = {
		"page": page,
		"next_url": next_url,
		"has_next": has_next,
		"current_seed_external_id": current_seed_external_id,
		"updated_at": datetime.now(timezone.utc).isoformat(),
		"counters": counters,
	}
	save_json(page_state_file, state)


def load_page_state(page_state_file: Path) -> dict:
	if not page_state_file.exists():
		return {}
	data = load_json(page_state_file)
	return data if isinstance(data, dict) else {}
