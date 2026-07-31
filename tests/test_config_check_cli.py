from click.testing import CliRunner

from mpreg.cli.main import cli

def test_config_check_dev_profile() -> None:
    runner = CliRunner()
    result = runner.invoke(
        cli, ["config-check", "mpreg/profiles/dev.toml", "--format", "json"]
    )
    # dev profile may warn about missing monitoring auth → exit 2
    assert result.exit_code in (0, 2)
    assert "groups" in result.output or "identity" in result.output
