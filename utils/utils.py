import csv
from pathlib import Path
from typing import Optional
from urllib.parse import urlencode, urljoin, urlparse, parse_qsl, urlunparse

import requests


OUTPUT_FIELDS = [
	"name",
	"fname",
	"lname",
	"email",
	"phone",
	"position",
	"address",
	"city",
	"country",
	"linkedin",
	"activity",
]


def safe_get(data: dict, key: str, default=None):
	value = data.get(key, default)
	return default if value is None else value


def json_request(
	url: str,
	method: str,
	headers: dict,
	payload: Optional[dict] = None,
	timeout: int = 45,
) -> dict:
	response = requests.request(
		method=method,
		url=url,
		headers=headers,
		json=payload,
		timeout=timeout,
	)
	response.raise_for_status()
	return response.json()


def update_query_params(url: str, updates: dict[str, str]) -> str:
	parsed = urlparse(url)
	query_params = dict(parse_qsl(parsed.query, keep_blank_values=True))
	query_params.update(updates)
	new_query = urlencode(query_params)
	return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))


def build_matchpro_initial_url(base_url: str, external_id: str, event_id: int, page_size: int) -> str:
	base = base_url.rstrip("/") + "/api/v1/matchmaking/matches/"
	query = urlencode(
		{
			"external_id": external_id,
			"event_id": str(event_id),
			"since": "",
			"page_size": str(page_size),
		}
	)
	return f"{base}?{query}"


def normalize_next_url(base_url: str, next_url: str | None, page_size: int) -> str | None:
	if not next_url:
		return None
	abs_url = urljoin(base_url.rstrip("/") + "/", next_url)
	return update_query_params(abs_url, {"page_size": str(page_size)})


def flatten_match_row(match: dict) -> dict:
	attributes = safe_get(match, "match_attributes", {})
	return {
		"external_id": safe_get(match, "external_id", ""),
		"match_reasons": " | ".join(safe_get(match, "match_reasons", [])),
		"match_label": safe_get(match, "match_label", ""),
		"match_attr_interested_solutions": " | ".join(safe_get(attributes, "interested_solutions", [])),
	}


def split_name(full_name: str) -> tuple[str, str]:
	clean_name = str(full_name).strip()
	if not clean_name:
		return "", ""
	parts = clean_name.split()
	if len(parts) == 1:
		return parts[0], ""
	return parts[0], " ".join(parts[1:])


def build_phone(country_code: str, mobile: str) -> str:
	code = str(country_code).strip()
	number = str(mobile).strip()
	if code and number:
		return f"{code} {number}"
	return number or code


def build_output_row(attendee: dict, match: dict) -> dict:
	match_data = flatten_match_row(match)
	name = safe_get(attendee, "firstname", "")
	fname, lname = split_name(name)
	country = safe_get(attendee, "country_of_residence", "") or safe_get(attendee, "country", "")
	activity = match_data["match_attr_interested_solutions"] or match_data["match_reasons"] or match_data["match_label"]

	return {
		"name": name,
		"fname": fname,
		"lname": lname,
		"email": safe_get(attendee, "email", ""),
		"phone": build_phone(safe_get(attendee, "country_code", ""), safe_get(attendee, "mobile", "")),
		"position": safe_get(attendee, "designation", ""),
		"address": safe_get(attendee, "address", ""),
		"city": safe_get(attendee, "city", ""),
		"country": country,
		"linkedin": safe_get(attendee, "linkedin", ""),
		"activity": activity,
	}


def build_attendee_index(attendees: list[dict]) -> dict:
	index: dict[str, dict] = {}
	for attendee in attendees:
		unique_id = str(safe_get(attendee, "unique_id", "")).strip()
		if unique_id:
			index[unique_id] = attendee
	return index


def append_rows_to_csv(output_csv: Path, rows: list[dict]) -> int:
	if not rows:
		return 0
	output_csv.parent.mkdir(parents=True, exist_ok=True)
	file_exists = output_csv.exists()
	with output_csv.open("a", encoding="utf-8", newline="") as csv_file:
		writer = csv.DictWriter(csv_file, fieldnames=OUTPUT_FIELDS)
		if not file_exists:
			writer.writeheader()
		for row in rows:
			writer.writerow({field: row.get(field, "") for field in OUTPUT_FIELDS})
	return len(rows)


def fetch_attendees_by_uids(
	gitex_base_url: str,
	bearer_token: str,
	event_id: int,
	uids: list[str],
	timeout: int,
) -> list[dict]:
	if not uids:
		return []
	url = gitex_base_url.rstrip("/") + "/customer_api/v2/attendees/by-uids/"
	headers = {
		"accept": "application/json, text/plain, */*",
		"authorization": f"Bearer {bearer_token}",
		"content-type": "application/json",
		"user-agent": "okhttp/4.9.2",
	}
	payload = {"event_id": event_id, "uids": [int(uid) for uid in uids]}
	response = json_request(url=url, method="POST", headers=headers, payload=payload, timeout=timeout)
	attendees = safe_get(response, "attendees", [])
	return attendees if isinstance(attendees, list) else []
