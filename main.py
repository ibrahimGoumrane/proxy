import argparse
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from dotenv import load_dotenv
from utils.logging_utils import setup_logger
from utils.persistence import (
	load_id_list,
	load_page_state,
	save_id_list,
	save_page_state,
)
from utils.utils import (
	append_rows_to_csv,
	build_attendee_index,
	build_matchpro_initial_url,
	build_output_row,
	fetch_attendees_by_uids,
	json_request,
	normalize_next_url,
	safe_get,
)

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent
TMP_DIR = BASE_DIR / "tmp"
OUTPUT_CSV = BASE_DIR / "matched_contacts.csv"
MATCHPRO_BASE_URL = "https://matchpro.eventxpro.com"
GITEX_BASE_URL = "https://gitexafrica-api.veuzco.com"
MATCHPRO_EVENT_ID = 5
GITEX_EVENT_ID = 24
FIXED_EXTERNAL_ID = "12464011"
DEFAULT_TIMEOUT = 45


def fetch_attendees_parallel(
	gitex_base_url: str,
	bearer_token: str,
	event_id: int,
	uids: list[str],
	timeout: int,
	max_parallel_requests: int,
	logger,
) -> list[dict]:
	if not uids:
		return []
	worker_count = max(1, min(max_parallel_requests, len(uids)))
	if worker_count == 1:
		try:
			return fetch_attendees_by_uids(
				gitex_base_url=gitex_base_url,
				bearer_token=bearer_token,
				event_id=event_id,
				uids=uids,
				timeout=timeout,
			)
		except Exception as exc:
			logger.warning("attendee_fetch_failed count=%s error=%s", len(uids), exc)
			return []

	chunk_size = max(1, (len(uids) + worker_count - 1) // worker_count)
	uid_chunks = [uids[i : i + chunk_size] for i in range(0, len(uids), chunk_size)]

	attendees: list[dict] = []
	with ThreadPoolExecutor(max_workers=worker_count) as executor:
		future_to_chunk = {
			executor.submit(
				fetch_attendees_by_uids,
				gitex_base_url=gitex_base_url,
				bearer_token=bearer_token,
				event_id=event_id,
				uids=chunk,
				timeout=timeout,
			): chunk
			for chunk in uid_chunks
		}
		for future in as_completed(future_to_chunk):
			chunk = future_to_chunk[future]
			try:
				attendees.extend(future.result())
			except Exception as exc:
				logger.warning("attendee_chunk_failed chunk_size=%s error=%s", len(chunk), exc)

	return attendees


def run_api_pipeline(args: argparse.Namespace) -> None:
	logger = setup_logger(args.log_dir)
	pending_ids = load_id_list(args.processed_ids_file)
	checked_ids = load_id_list(args.already_checked_ids_file)
	checked_set = set(checked_ids)
	pending_set = set(pending_ids)

	if FIXED_EXTERNAL_ID not in checked_set and FIXED_EXTERNAL_ID not in pending_set:
		pending_ids.append(FIXED_EXTERNAL_ID)
		pending_set.add(FIXED_EXTERNAL_ID)
		save_id_list(args.processed_ids_file, pending_ids)
		logger.info("seed_enqueued=%s pending_total=%s", FIXED_EXTERNAL_ID, len(pending_ids))

	page_state = load_page_state(args.page_state_file) if args.resume else {}
	current_seed_id = None
	current_url = None

	if args.resume and page_state.get("next_url") and page_state.get("current_seed_external_id"):
		current_seed_id = str(page_state.get("current_seed_external_id"))
		current_url = normalize_next_url(args.matchpro_base_url, page_state.get("next_url"), args.page_size)
		logger.info("resume_mode=true seed=%s next_url=%s", current_seed_id, current_url)

	if args.target_contacts:
		logger.info("target_contacts=%s", args.target_contacts)
	logger.info("parallel_requests=%s", args.max_parallel_requests)

	headers = {
		"accept": "application/json, text/plain, */*",
		"authorization": f"Token {args.matchpro_token}",
		"user-agent": "okhttp/4.9.2",
	}

	counters = {
		"seeds_processed": 0,
		"pages_processed": 0,
		"matches_seen": 0,
		"already_known_matches": 0,
		"new_ids_discovered": 0,
		"ids_posted": 0,
		"attendees_returned": 0,
		"rows_written": 0,
	}

	while True:
		if args.target_contacts and counters["rows_written"] >= args.target_contacts:
			logger.info("target_reached=%s total_written=%s", args.target_contacts, counters["rows_written"])
			break

		if not current_url:
			while pending_ids and pending_ids[-1] in checked_set:
				skipped = pending_ids.pop()
				pending_set.discard(skipped)
				logger.info("skip_seed_already_checked=%s", skipped)
				save_id_list(args.processed_ids_file, pending_ids)

			if not pending_ids:
				logger.info("pending_queue_empty=true checked_total=%s", len(checked_set))
				break

			current_seed_id = pending_ids.pop()
			pending_set.discard(current_seed_id)
			save_id_list(args.processed_ids_file, pending_ids)

			current_url = build_matchpro_initial_url(
				base_url=args.matchpro_base_url,
				external_id=current_seed_id,
				event_id=args.matchpro_event_id,
				page_size=args.page_size,
			)
			logger.info(
				"seed_popped=%s pending_remaining=%s checked_total=%s",
				current_seed_id,
				len(pending_ids),
				len(checked_set),
			)

		response = json_request(url=current_url, method="GET", headers=headers, timeout=args.timeout)
		meta = safe_get(response, "meta", {})
		page = int(safe_get(meta, "page", counters["pages_processed"] + 1))
		matches = safe_get(safe_get(response, "data", {}), "matches", [])
		if not isinstance(matches, list):
			matches = []

		external_ids: list[str] = []
		seen_in_page: set[str] = set()
		for match in matches:
			external_id = str(safe_get(match, "external_id", "")).strip()
			if external_id and external_id not in seen_in_page:
				external_ids.append(external_id)
				seen_in_page.add(external_id)

		new_discovered_ids: list[str] = []
		for uid in external_ids:
			if uid in checked_set or uid in pending_set:
				continue
			new_discovered_ids.append(uid)
			pending_ids.append(uid)
			pending_set.add(uid)

		already_known_count = len(external_ids) - len(new_discovered_ids)
		save_id_list(args.processed_ids_file, pending_ids)

		attendees = fetch_attendees_parallel(
			gitex_base_url=args.gitex_base_url,
			bearer_token=args.gitexafrica_token,
			event_id=args.gitex_event_id,
			uids=new_discovered_ids,
			timeout=args.timeout,
			max_parallel_requests=args.max_parallel_requests,
			logger=logger,
		)
		attendee_index = build_attendee_index(attendees)

		rows: list[dict] = []
		for match in matches:
			external_id = str(safe_get(match, "external_id", "")).strip()
			if external_id not in new_discovered_ids:
				continue
			attendee = attendee_index.get(external_id)
			if attendee:
				rows.append(build_output_row(attendee, match))

		written = append_rows_to_csv(args.output, rows)

		has_next = bool(safe_get(meta, "has_next", False))
		next_url = normalize_next_url(args.matchpro_base_url, safe_get(meta, "next_url", None), args.page_size)

		counters["pages_processed"] += 1
		counters["matches_seen"] += len(matches)
		counters["already_known_matches"] += already_known_count
		counters["new_ids_discovered"] += len(new_discovered_ids)
		counters["ids_posted"] += len(new_discovered_ids)
		counters["attendees_returned"] += len(attendees)
		counters["rows_written"] += written

		save_page_state(
			page_state_file=args.page_state_file,
			page=page,
			next_url=next_url,
			has_next=has_next,
			counters=counters,
			current_seed_external_id=current_seed_id,
		)

		if args.target_contacts and counters["rows_written"] >= args.target_contacts:
			logger.info(
				"target_reached=%s after_seed=%s page=%s total_written=%s",
				args.target_contacts,
				current_seed_id,
				page,
				counters["rows_written"],
			)
			break

		logger.info(
			"seed=%s page=%s fetched=%s known=%s discovered=%s posted=%s attendees=%s written=%s pending_total=%s checked_total=%s total_written=%s",
			current_seed_id,
			page,
			len(matches),
			already_known_count,
			len(new_discovered_ids),
			len(new_discovered_ids),
			len(attendees),
			written,
			len(pending_ids),
			len(checked_set),
			counters["rows_written"],
		)

		if has_next and next_url:
			current_url = next_url
			continue

		if current_seed_id and current_seed_id not in checked_set:
			checked_ids.append(current_seed_id)
			checked_set.add(current_seed_id)
			save_id_list(args.already_checked_ids_file, checked_ids)
			counters["seeds_processed"] += 1
			logger.info(
				"seed_completed=%s seeds_processed=%s pending_total=%s checked_total=%s",
				current_seed_id,
				counters["seeds_processed"],
				len(pending_ids),
				len(checked_set),
			)

		current_seed_id = None
		current_url = None

	print(f"Completed seeds: {counters['seeds_processed']}")
	print(f"Completed pages: {counters['pages_processed']}")
	print(f"New IDs discovered: {counters['new_ids_discovered']}")
	print(f"Total rows written: {counters['rows_written']}")
	print(f"Pending queue file: {args.processed_ids_file}")
	print(f"Already checked file: {args.already_checked_ids_file}")
	print(f"Page checkpoint saved in: {args.page_state_file}")
	print(f"Logs directory: {args.log_dir}")


def main() -> None:
	matchpro_token = os.getenv("MATCHPRO_TOKEN", "")
	gitexafrica_token = os.getenv("GITEXAFRICA_TOKEN", "")
	page_size = int(os.getenv("MATCHPRO_PAGE_SIZE", "200"))
	max_parallel_requests = int(os.getenv("MAX_PARALLEL_REQUESTS", "1"))
	target_contacts_raw = os.getenv("TARGET_CONTACTS", "").strip()
	target_contacts = int(target_contacts_raw) if target_contacts_raw else None
	if max_parallel_requests < 1:
		raise ValueError("MAX_PARALLEL_REQUESTS must be >= 1")
	if target_contacts is not None and target_contacts < 1:
		raise ValueError("TARGET_CONTACTS must be >= 1 when provided")

	if not (matchpro_token and gitexafrica_token):
		raise ValueError("Missing required env values: MATCHPRO_TOKEN and GITEXAFRICA_TOKEN.")

	config = argparse.Namespace(
		resume=True,
		output=OUTPUT_CSV,
		matchpro_token=matchpro_token,
		gitexafrica_token=gitexafrica_token,
		matchpro_base_url=MATCHPRO_BASE_URL,
		gitex_base_url=GITEX_BASE_URL,
		matchpro_event_id=MATCHPRO_EVENT_ID,
		gitex_event_id=GITEX_EVENT_ID,
		page_size=page_size,
		processed_ids_file=TMP_DIR / "processed_external_ids.json",
		already_checked_ids_file=TMP_DIR / "already_checked_external_ids.json",
		page_state_file=TMP_DIR / "page_state.json",
		log_dir=TMP_DIR / "logs",
		timeout=DEFAULT_TIMEOUT,
		max_parallel_requests=max_parallel_requests,
		target_contacts=target_contacts,
	)

	run_api_pipeline(config)


if __name__ == "__main__":
	main()
