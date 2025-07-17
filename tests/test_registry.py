import pytest

from mpreg.registry import Command, CommandRegistry


def sample_func_1() -> str:
    return "func1_result"


def sample_func_2(arg1: str, arg2: str) -> str:
    return f"func2_result_{arg1}_{arg2}"


def test_command_creation() -> None:
    cmd = Command("test_cmd", sample_func_1)
    assert cmd.name == "test_cmd"
    assert cmd.fun == sample_func_1


def test_command_call() -> None:
    cmd = Command("test_cmd", sample_func_1)
    assert cmd() == "func1_result"

    cmd_args = Command("test_cmd_args", sample_func_2)
    assert cmd_args("a", "b") == "func2_result_a_b"


def test_command_registry_register_and_get() -> None:
    registry = CommandRegistry()
    cmd1 = Command("cmd1", sample_func_1)
    registry.register(cmd1)

    assert registry.get("cmd1") == cmd1


def test_command_registry_register_duplicate() -> None:
    registry = CommandRegistry()
    cmd1 = Command("cmd1", sample_func_1)
    registry.register(cmd1)

    # Re-registering should now succeed (allows overrides)
    registry.register(cmd1)
    assert "cmd1" in registry


def test_command_registry_get_non_existent() -> None:
    registry = CommandRegistry()
    with pytest.raises(ValueError, match="Command non_existent_cmd not found."):
        registry.get("non_existent_cmd")


def test_command_registry_contains() -> None:
    registry = CommandRegistry()
    cmd1 = Command("cmd1", sample_func_1)
    registry.register(cmd1)

    assert "cmd1" in registry
    assert "non_existent_cmd" not in registry
