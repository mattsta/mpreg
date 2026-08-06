from pathlib import Path
import re

_IMPORT_RE = re.compile(
    r"^\s*(?:from\s+websockets(?:\.|\s)|import\s+websockets)\b",
    re.MULTILINE,
)

def test_no_websockets_outside_transport_layer() -> None:
    """websockets must only be imported under mpreg/core/transport/."""
    root = Path(__file__).resolve().parents[1]
    transport_root = root / "mpreg" / "core" / "transport"
    offenders: list[Path] = []

    for path in (root / "mpreg").rglob("*.py"):
        try:
            path.relative_to(transport_root)
            continue  # entire transport package is the allowed layer
        except ValueError:
            pass
        try:
            text = path.read_text(encoding="utf-8")
        except OSError:
            continue
        if _IMPORT_RE.search(text):
            offenders.append(path)

    assert not offenders, (
        "websockets usage must stay in the transport layer; move any direct "
        f"imports into mpreg/core/transport. Offenders: {offenders}"
    )
