
import re
import json
from configparser import ConfigParser
from typing import Any, List
from elasticsearch import Elasticsearch, helpers  # Third-party imports (elasticsearch-py)
import utils as ut # Local utilities 

CONFIG_PATH = "config.ini"         # Path to config file (must exist)
ES_INDEX = "reddit_posts_test"     # Elasticsearch index to query
VERBOSE = False                    # If True, prints per-document debug info

def load_config(path: str) -> ConfigParser:
    cfg = ConfigParser()
    cfg.read(path)
    return cfg


def safe_source(hit: dict) -> dict:
    return hit.get("_source", {}) or {}


def detect_format(hit: dict) -> str:
    src = safe_source(hit)
    if src.get("is_video"):
        return "video"
    url = src.get("url", "") or ""
    # Use a regex anchored at start to detect Reddit-hosted images
    if re.match(r"^https?://i\.redd\.it", url) or src.get("is_gallery"):
        return "image"
    return "text"


def accumulate_ids(ids_list: list, value: Any):
    if value is None:
        return
    if isinstance(value, list):
        ids_list.extend(value)
    else:
        ids_list.append(value)


def main():
    # Load configuration (will raise KeyError later if 'X' missing)
    cfg = load_config(CONFIG_PATH)

    # Create Elasticsearch client
    try:
        client = Elasticsearch(
            f"{cfg['X']['elastic_url']}",
            api_key=f"{cfg['X']['elastic_api_key']}",
        )
    except Exception as e:
        # Fail early 
        print("Error creating Elasticsearch client:", e)
        raise

    try:
        docs = helpers.scan(client, index=ES_INDEX, query={"query": {"match_all": {}}})
    except Exception as e:
        print("Error scanning Elasticsearch index:", e)
        raise

    # Initialize accumulators
    timestamp_list: List[List[Any]] = []   # list of [id, timestamp] pairs
    format_list: List[List[Any]] = []      # list of [id, format]
    landing_pages: List[List[Any]] = []    # list of [id, landing_pages] pairs
    landing_pages2: List[Any] = []         # flat list of landing page URLs
    ids: List[Any] = []                    # collected key ids (flat)
    impressions: List[Any] = []            # list of [id, impression_value] pairs

    # Counters for main formats and impressions total
    v_ct = 0      # video count
    i_ct = 0      # image count
    t_ct = 0      # text count
    imp_ct = 0.0  # impressions numeric aggregate (float to accept decimals)

    doc_count = 0  # number of documents processed

    # Iterate documents yielded by helpers.scan (generator)
    for hit in docs:
        doc_count += 1
        doc_id = hit.get("_id")

        # Timestamp extraction - use ut.timestamp(hit)
        # Defensive: ut.timestamp might raise; catch exceptions and store None
        try:
            ts = ut.timestamp(hit)
        except Exception as e:
            # In verbose mode, show why timestamp failed for a doc
            if VERBOSE:
                print(f"Warning: ut.timestamp failed for doc {doc_id}: {e}")
            ts = None

        timestamp_list.append([doc_id, ts])
        fmt = detect_format(hit)
        format_list.append([doc_id, fmt])

        # Increment appropriate format counter
        if fmt == "video":
            v_ct += 1
        elif fmt == "image":
            i_ct += 1
        else:
            t_ct += 1

        # Landing pages: use ut.landing_pages(hit) if available
        # Defensive: catch exceptions and treat as None
        try:
            filtered_urls = ut.landing_pages(hit)
        except Exception as e:
            if VERBOSE:
                print(f"Warning: ut.landing_pages failed for doc {doc_id}: {e}")
            filtered_urls = None

        if VERBOSE:
            print(f"doc {doc_id} landing_pages: {filtered_urls}")

        # If there are landing pages, record both the per-doc mapping and a flat list
        if filtered_urls:
            landing_pages.append([doc_id, filtered_urls])
            # If filtered_urls is a list, extend; else append single URL
            if isinstance(filtered_urls, list):
                landing_pages2.extend(filtered_urls)
            else:
                landing_pages2.append(filtered_urls)

        # Collect key ids using ut.key_ids
        try:
            key_ids_value = ut.key_ids(hit)
        except Exception as e:
            if VERBOSE:
                print(f"Warning: ut.key_ids failed for doc {doc_id}: {e}")
            key_ids_value = None
        accumulate_ids(ids, key_ids_value)

        # Collect impressions using ut.impressions
        # Defensive: catch exceptions and treat value as None
        try:
            imp_val = ut.impressions(hit)
        except Exception as e:
            if VERBOSE:
                print(f"Warning: ut.impressions failed for doc {doc_id}: {e}")
            imp_val = None

        # Store detail entry: [doc_id, imp_val]
        impressions.append([doc_id, imp_val])

        # Aggregate imp_ct:
        # - If imp_val is numeric-like, add numeric value
        # - If imp_val is truthy but not numeric, count as 1 (best-effort)
        try:
            if imp_val is not None:
                imp_ct += float(imp_val)
        except Exception:
            if imp_val:
                imp_ct += 1

    # Sort timestamp_list; place None timestamps at the end
    # Sorting key: (timestamp is None, timestamp) -> (False, ts) sorts before (True, None)
    timestamp_list.sort(key=lambda x: (x[1] is None, x[1]))

    # Preserve the [id, ts] pair for start_date and last_date
    if timestamp_list:
        start_date = timestamp_list[0]
        non_none = [t for t in timestamp_list if t[1] is not None]
        last_date = non_none[-1] if non_none else timestamp_list[-1]
    else:
        start_date = None
        last_date = None

    # Prepare summary data structure
    data_type = "communications"   # avoid shadowing builtin 'type'
    platform = "Reddit"

    main_format = {
        "video": v_ct,
        "image": i_ct,
        "text": t_ct,
    }

    data = {
        "type": data_type,
        "key_ids": ids,                 # collected ids (flat list)
        "start_date": start_date,       # [id, timestamp] pair
        "last_date": last_date,         # [id, timestamp] pair
        "platform": platform,
        "main_format": main_format,
        "landing_pages": landing_pages2,   # flat list of landing page URLs
        "impressions_total": imp_ct,       # aggregated numeric impressions
        "impressions_detail": impressions, # per-doc impression values
        "doc_count_processed": doc_count,  # number of docs processed
    }

    # Write summary JSON to disk, pretty-printed
    try:
        with open("comms.json", "w") as fout:
            # default=str ensures non-JSON-native types (e.g., datetimes) serialize
            json.dump(data, fout, indent=2, default=str)
    except Exception as e:
        # If writing fails (permissions, disk full, etc.), raise after printing useful message
        print("Error writing comms.json:", e)
        raise

    print(f"Processed {doc_count} documents from index '{ES_INDEX}'.")
    print(f"Date range (as [id, timestamp]): {start_date} -> {last_date}")
    print(f"Formats: {main_format}")
    print(f"Landing pages collected: {len(landing_pages2)}")
    print(f"Impressions (total): {imp_ct}")

# Execute main when run as a script
if __name__ == "__main__":
    main()
    
    
    
