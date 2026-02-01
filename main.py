import os
import re
import sys
import time
from typing import Any, Dict, List, Optional

import pandas as pd
import requests


BASE_URL = os.environ.get("TWENTY_BASE_URL", "http://localhost:3000/")
API_KEY = os.environ.get("TWENTY_API_KEY")
if not API_KEY:
    print("Error: TWENTY_API_KEY environment variable is not set.")
    print("Please set it with: export TWENTY_API_KEY='your-api-key'")
    sys.exit(1)

S = requests.Session()
S.headers.update(
    {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json",
    }
)


def slugify(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    if not s:
        raise ValueError("Cannot slugify empty string")
    if s[0].isdigit():
        s = f"f_{s}"
    return s


def http(method: str, path: str, **kwargs) -> Any:
    url = f"{BASE_URL}{path}"
    r = S.request(method, url, timeout=60, **kwargs)
    if r.status_code >= 400:
        raise RuntimeError(f"{method} {path} -> {r.status_code}\n{r.text}")
    if r.text.strip():
        return r.json()
    return None


# ---------- Metadata: objects ----------


def get_objects() -> List[Dict[str, Any]]:
    res = http("GET", "/rest/metadata/objects")
    data = res.get("data", res)

    # supports both:
    # - {"data": {"objects": [...], "pageInfo": {...}}}
    # - {"data": [...]}
    if isinstance(data, dict) and "objects" in data:
        return data["objects"]
    if isinstance(data, list):
        return data
    raise RuntimeError(f"Unexpected objects payload shape: {res}")


def find_object(name_singular: str) -> Optional[Dict[str, Any]]:
    for obj in get_objects():
        if obj.get("nameSingular") == name_singular:
            return obj
    return None


def create_object(
    name_singular: str,
    name_plural: str,
    label_singular: str,
    label_plural: str,
    description: str,
    icon: str = "IconTable",
) -> Dict[str, Any]:
    payload = {
        "nameSingular": name_singular,
        "namePlural": name_plural,
        "labelSingular": label_singular,
        "labelPlural": label_plural,
        "description": description,
        "icon": icon,
        "labelIdentifierFieldMetadataId": None,
        "imageIdentifierFieldMetadataId": None,
    }
    return http("POST", "/rest/metadata/objects", json=payload)


# ---------- Metadata: fields ----------


def get_fields_for_object(object_metadata_id: str) -> List[Dict[str, Any]]:
    # This is the only part that might need adjustment if the endpoint differs.
    res = http(
        "GET",
        "/rest/metadata/fields",
        params={"objectMetadataId": object_metadata_id},
    )
    data = res.get("data", res)
    if isinstance(data, dict) and "fields" in data:
        return data["fields"]
    if isinstance(data, list):
        return data
    # Some APIs return {"data": {"fields": [...], "pageInfo": {...}}}
    if isinstance(data, dict) and "pageInfo" in data:
        # try common key
        for k in ["fields", "items", "nodes"]:
            if k in data and isinstance(data[k], list):
                return data[k]
    raise RuntimeError(f"Unexpected fields payload shape: {res}")


def create_field(
    object_metadata_id: str,
    name: str,
    label: str,
    field_type: str,
    description: str = "",
    is_nullable: bool = True,
    default_value: Any = None,
    settings: Optional[Dict[str, Any]] = None,
    options: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    payload = {
        "type": field_type,
        "objectMetadataId": object_metadata_id,
        "name": name,
        "label": label,
        "description": description,
        "icon": "",
        "defaultValue": default_value,
        "isNullable": is_nullable,
        "settings": settings or {},
        "options": options or [],
    }
    return http("POST", "/rest/metadata/fields", json=payload)


def infer_twenty_type(series: pd.Series) -> str:
    # Conservative defaults (tune once you see allowed enum values in Playground).
    s = series.dropna()
    if s.empty:
        return "TEXT"

    # try boolean
    as_str = s.astype(str).str.lower()
    if as_str.isin(["true", "false", "0", "1", "yes", "no"]).all():
        return "BOOLEAN"

    # try number
    def is_intlike(x: Any) -> bool:
        try:
            f = float(x)
            return f.is_integer()
        except Exception:
            return False

    def is_floatlike(x: Any) -> bool:
        try:
            float(x)
            return True
        except Exception:
            return False

    if s.map(is_intlike).all():
        return "NUMBER"
    if s.map(is_floatlike).all():
        return "NUMBER"

    # ids
    if series.name and series.name.lower() in ["external_id", "id", "uuid"]:
        return "TEXT"

    # default
    return "TEXT"


def ensure_fields(
    object_metadata_id: str,
    csv_columns: List[str],
    df: pd.DataFrame,
) -> None:
    existing = get_fields_for_object(object_metadata_id)
    existing_names = {f.get("name") for f in existing}

    for col in csv_columns:
        field_name = slugify(col)
        if field_name in existing_names:
            continue

        field_type = infer_twenty_type(df[col])
        print(f"Creating field {field_name} ({field_type})")
        create_field(
            object_metadata_id=object_metadata_id,
            name=field_name,
            label=col,
            field_type=field_type,
            description=f"Imported from CSV column '{col}'",
            is_nullable=True,
        )


# ---------- Data (records) ----------


def list_records(
    name_plural: str,
    external_id_field: str,
    external_id_value: str,
) -> List[Dict[str, Any]]:
    # Filtering syntax may differ; if this fails, we'll adapt to your API.
    res = http(
        "GET",
        f"/rest/{name_plural}",
        params={f"filter[{external_id_field}]": external_id_value, "limit": 1},
    )
    data = res.get("data", res)
    if isinstance(data, dict) and "records" in data:
        return data["records"]
    if isinstance(data, list):
        return data
    # common: {"data": [...], "pageInfo": ...}
    if isinstance(data, dict):
        for k in ["nodes", "items", "people", "companies"]:
            if k in data and isinstance(data[k], list):
                return data[k]
    # fallback: if Twenty returns {"data": {"<plural>": [...]}}
    if isinstance(data, dict) and name_plural in data and isinstance(data[name_plural], list):
        return data[name_plural]
    return []


def create_record(name_plural: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    return http("POST", f"/rest/{name_plural}", json=payload)


def update_record(name_plural: str, record_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    return http("PATCH", f"/rest/{name_plural}/{record_id}", json=payload)


def upsert_record(
    name_plural: str,
    external_id_field: str,
    row: Dict[str, Any],
) -> None:
    ext = str(row.get(external_id_field) or "").strip()
    if not ext:
        raise ValueError(f"Row missing {external_id_field}: {row}")

    matches = list_records(name_plural, external_id_field, ext)
    if matches:
        rec = matches[0]
        rec_id = rec.get("id")
        if not rec_id:
            raise RuntimeError(f"Cannot upsert: record has no id: {rec}")
        update_record(name_plural, rec_id, row)
    else:
        create_record(name_plural, row)


# ---------- Main ----------


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python bootstrap_twenty_csv.py file.csv [singular] [plural]")
        sys.exit(2)

    csv_path = sys.argv[1]
    base = os.path.splitext(os.path.basename(csv_path))[0]
    name_singular = slugify(sys.argv[2]) if len(sys.argv) >= 3 else slugify(base)
    name_plural = slugify(sys.argv[3]) if len(sys.argv) >= 4 else slugify(f"{base}s")

    df = pd.read_csv(csv_path)

    if "external_id" not in df.columns:
        raise ValueError("CSV must include column 'external_id' for idempotent upserts")

    # Ensure object
    obj = find_object(name_singular)
    if not obj:
        print(f"Creating object: {name_singular}/{name_plural}")
        create_object(
            name_singular=name_singular,
            name_plural=name_plural,
            label_singular=base.title(),
            label_plural=f"{base.title()}s",
            description=f"Bootstrapped from {os.path.basename(csv_path)}",
        )
        time.sleep(1.0)
        obj = find_object(name_singular)
        if not obj:
            raise RuntimeError("Object creation did not appear in list after creation")

    object_id = obj["id"]
    print(f"Using object id={object_id}")

    # Ensure fields
    ensure_fields(object_id, list(df.columns), df)

    # Normalize keys to slugified field names
    rename_map = {c: slugify(c) for c in df.columns}
    df = df.rename(columns=rename_map)

    # Upsert data
    for _, r in df.iterrows():
        row = {k: (None if pd.isna(v) else v) for k, v in r.to_dict().items()}
        upsert_record(name_plural, "external_id", row)
        time.sleep(0.02)

    print("Done.")


if __name__ == "__main__":
    main()