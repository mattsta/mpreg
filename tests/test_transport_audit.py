from pathlib import Path


def test_no_websockets_outside_transport_layer() -> None:
    root = Path(__file__).resolve().parents[1]
    allowed = {root / "mpreg" / "core" / "transport" / "websocket_transport.py"}
    offenders: list[Path] = []

    for path in (root / "mpreg").rglob("*.py"):
        if path in allowed:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except OSError:
            continue
        if "websockets" in text:
            offenders.append(path)

    assert not offenders, (
        "websockets usage must stay in the transport layer; move any direct "
        f"imports into mpreg/core/transport. Offenders: {offenders}"
    )
