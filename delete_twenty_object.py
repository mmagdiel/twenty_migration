import os
import sys
from typing import Any, Dict, List, Optional

import requests


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
    r = S.request(method, url, timeout=60, **kwargs)
    if r.status_code >= 400:
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


def find_object(name_singular: str) -> Optional[Dict[str, Any]]:
    for o in list_objects():
        if o.get("nameSingular") == name_singular:
            return o
    return None


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python delete_twenty_object.py <nameSingular> [--hard]")
        sys.exit(2)

    name_singular = sys.argv[1]
    hard = "--hard" in sys.argv[2:]

    obj = find_object(name_singular)
    if not obj:
        print(f"Object '{name_singular}' not found. Nothing to do.")
        return

    obj_id = obj["id"]
    print(f"Found object {name_singular} id={obj_id} isCustom={obj.get('isCustom')}")

    if not obj.get("isCustom"):
        raise RuntimeError(
            "Refusing to delete non-custom object (isCustom=false). "
            "Only custom objects should be deleted by script."
        )

    if hard:
        # Try hard delete
        try:
            http("DELETE", f"/rest/metadata/objects/{obj_id}")
            print("Deleted (hard).")
            return
        except RuntimeError as e:
            print("Hard delete failed. Error:")
            print(e)
            print("Try without --hard to deactivate instead.")
            raise

    # Soft delete / deactivate
    try:
        http("PATCH", f"/rest/metadata/objects/{obj_id}", json={"isActive": False})
        print("Deactivated (isActive=false).")
        return
    except RuntimeError as e:
        # If PATCH endpoint doesn't exist, tell user what we need
        print("Deactivate failed. Your instance may not expose PATCH for objects.")
        print("Please check REST Playground for an 'Update object' endpoint.")
        raise


if __name__ == "__main__":
    main()