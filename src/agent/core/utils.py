import re

def is_effectively_empty(text: str) -> bool:
    if text is None:
        return True
    return re.sub(r"[^A-Za-z0-9]+", "", text) == ""

