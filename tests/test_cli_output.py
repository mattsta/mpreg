from mpreg.cli.output import _jsonable, emit

def test_jsonable_nested() -> None:
    assert _jsonable({"a": [1, {"b": 2}]})["a"][1]["b"] == 2

def test_emit_json(capsys) -> None:
    # rich prints to console; just ensure no throw
    emit({"ok": True}, output_format="json")
