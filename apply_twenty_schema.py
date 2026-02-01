import os
import sys
from typing import Any, Dict, List, Optional

import requests
import yaml


BASE_URL = os.environ.get("TWENTY_BASE_URL", "http://localhost:3000").rstrip("/")
API_KEY = os.environ["TWENTY_API_KEY"]

S = requests.Session()
S.headers.update(
    {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json",
    }
)


def http(method: str, path: str, **kwargs) -> Any:
    url = f"{BASE_URL}{path}"
    print(f"  DEBUG: {method} {url}")
    if "json" in kwargs:
        import json
        print(f"  DEBUG: Body = {json.dumps(kwargs['json'], indent=2)}")
    r = S.request(method, url, timeout=60, **kwargs)
    if r.status_code >= 400:
        print(f"  DEBUG: Response = {r.text}")
        print(f"  DEBUG: Status Code = {r.status_code}")
        print(f"  DEBUG: Headers = {dict(r.headers)}")
        raise RuntimeError(f"{method} {path} -> {r.status_code}\n{r.text}")
    if r.text.strip():
        return r.json()
    return None


def list_objects() -> List[Dict[str, Any]]:
    res = http("GET", "/rest/metadata/objects")
    data = res.get("data", res)
    if isinstance(data, dict) and "objects" in data:
        return data["objects"]
    if isinstance(data, list):
        return data
    raise RuntimeError(f"Unexpected objects payload: {res}")


def find_object_by_singular(name_singular: str) -> Optional[Dict[str, Any]]:
    for o in list_objects():
        if o.get("nameSingular") == name_singular:
            return o
    return None


def create_object(obj: Dict[str, Any]) -> Dict[str, Any]:
    payload = {
        "nameSingular": obj["nameSingular"],
        "namePlural": obj["namePlural"],
        "labelSingular": obj["labelSingular"],
        "labelPlural": obj["labelPlural"],
        "description": obj.get("description", ""),
        "icon": obj.get("icon", ""),
        #"labelIdentifierFieldMetadataId": obj.get("labelIdentifierFieldMetadataId"),
        #"imageIdentifierFieldMetadataId": obj.get("imageIdentifierFieldMetadataId"),
    }
    return http("POST", "/rest/metadata/objects", json=payload)

"""
def list_fields(object_metadata_id: str) -> List[Dict[str, Any]]:
    # Assumption (common): filter by objectMetadataId
    res = http(
        "GET",
        "/rest/metadata/fields",
        params={"objectMetadataId": object_metadata_id},
    )
    data = res.get("data", res)

    # Accept a few possible shapes
    if isinstance(data, dict) and "fields" in data and isinstance(data["fields"], list):
        return data["fields"]
    if isinstance(data, dict) and "nodes" in data and isinstance(data["nodes"], list):
        return data["nodes"]
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and "pageInfo" in data:
        for k in ["fields", "items", "nodes"]:
            if k in data and isinstance(data[k], list):
                return data[k]

    raise RuntimeError(
        "Could not parse fields listing response. "
        "Please share GET /rest/metadata/fields?objectMetadataId=... response."
    )
"""

def list_fields(object_metadata_id: str) -> List[Dict[str, Any]]:
    # Your Twenty REST metadata endpoints don't accept query params.
    # So: fetch all fields and filter client-side.
    res = http("GET", "/rest/metadata/fields")

    data = res.get("data", res)

    # Normalize to list
    if isinstance(data, dict) and "fields" in data and isinstance(data["fields"], list):
        fields = data["fields"]
    elif isinstance(data, dict) and "pageInfo" in data:
        for k in ["fields", "items", "nodes"]:
            if k in data and isinstance(data[k], list):
                fields = data[k]
                break
        else:
            raise RuntimeError(f"Unexpected fields payload shape: {res}")
    elif isinstance(data, list):
        fields = data
    else:
        raise RuntimeError(f"Unexpected fields payload shape: {res}")

    return [f for f in fields if f.get("objectMetadataId") == object_metadata_id]

def snake_to_camel(snake_str: str) -> str:
    """Convert snake_case to camelCase"""
    components = snake_str.split('_')
    return components[0] + ''.join(x.title() for x in components[1:])

def create_field(object_metadata_id: str, f: Dict[str, Any]) -> Dict[str, Any]:
    # Convert field name from snake_case to camelCase if needed
    field_name = f["name"]
    if '_' in field_name:
        field_name = snake_to_camel(field_name)
    
    # Build minimal payload
    payload = {
        "type": f["type"],
        "objectMetadataId": object_metadata_id,
        "name": field_name,
        "label": f.get("label", f["name"]),
    }
    
    # Add optional fields only if they have meaningful values
    if f.get("description"):
        payload["description"] = f["description"]
    
    # isNullable - only add if explicitly set to False (default is True)
    if "isNullable" in f:
        payload["isNullable"] = f["isNullable"]
    
    # Only include icon if provided and non-empty
    if f.get("icon"):
        payload["icon"] = f["icon"]
    
    # Only include defaultValue if explicitly provided and not None
    if "defaultValue" in f and f["defaultValue"] is not None:
        payload["defaultValue"] = f["defaultValue"]
    
    # Only include settings if provided and non-empty (not empty dict)
    if f.get("settings") and f["settings"]:
        payload["settings"] = f["settings"]
    
    # Only include options if provided and non-empty (not empty list)
    if f.get("options") and f["options"]:
        payload["options"] = f["options"]
    
    print(f"  DEBUG: Payload = {payload}")
    
    # Try with minimal payload first
    try:
        return http("POST", "/rest/metadata/fields", json=payload)
    except RuntimeError as e:
        # If it fails, try without icon
        if "icon" in payload:
            print(f"  DEBUG: Retrying without icon...")
            payload_no_icon = {k: v for k, v in payload.items() if k != "icon"}
            try:
                return http("POST", "/rest/metadata/fields", json=payload_no_icon)
            except RuntimeError:
                pass
        
        # If still fails, try absolute minimal payload
        print(f"  DEBUG: Retrying with absolute minimal payload...")
        minimal_payload = {
            "type": f["type"],
            "objectMetadataId": object_metadata_id,
            "name": field_name,
            "label": f.get("label", f["name"]),
        }
        return http("POST", "/rest/metadata/fields", json=minimal_payload)


def apply_schema(schema: Dict[str, Any]) -> None:
    objects = schema.get("objects", [])
    if not isinstance(objects, list) or not objects:
        raise ValueError("YAML must have 'objects:' as a non-empty list")

    for obj in objects:
        name_singular = obj["nameSingular"]
        existing = find_object_by_singular(name_singular)

        if not existing:
            print(f"[object] creating {name_singular}")
            create_object(obj)
            existing = find_object_by_singular(name_singular)
            if not existing:
                raise RuntimeError(f"Object {name_singular} not found after create")
        else:
            print(f"[object] exists {name_singular} (id={existing['id']})")

        object_id = existing["id"]

        # Ensure fields
        current_fields = list_fields(object_id)
        current_names = {f.get("name") for f in current_fields}
        print(f"  DEBUG: Existing fields: {sorted(current_names)}")

        for f in obj.get("fields", []):
            fname = f["name"]
            # Convert to camelCase for comparison
            fname_camel = snake_to_camel(fname) if '_' in fname else fname
            
            # Check both snake_case and camelCase versions
            if fname in current_names or fname_camel in current_names:
                print(f"  [field] exists {fname} (as {fname_camel})")
                continue
            print(f"  [field] creating {fname} ({f['type']})")
            create_field(object_id, f)

        # Optional: you might want to set labelIdentifierFieldMetadataId after creating fields
        # That requires an update endpoint for objects (not shown here), so we only create.

    print("Schema applied successfully.")


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python apply_twenty_schema.py schema.yaml")
        sys.exit(2)

    with open(sys.argv[1], "r", encoding="utf-8") as f:
        schema = yaml.safe_load(f)

    apply_schema(schema)


if __name__ == "__main__":
    main()