"""langgraph.json ``env`` 字段的加载行为测试（对齐官方 dev 语义）。"""

import os
from pathlib import Path

import pytest

from superlanggraph.cli import load_config_env


def _write_config(tmp_path: Path, config: str) -> Path:
    config_path = tmp_path / "langgraph.json"
    config_path.write_text(config, encoding="utf-8")
    return config_path


def test_env_file_values_are_injected(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """字符串形态：相对配置文件目录解析，非空值全部注入。"""
    (tmp_path / "secrets.env").write_text("DEMO_MODEL_KEY=abc\nEMPTY_VAR=\n", encoding="utf-8")
    config_path = _write_config(tmp_path, '{"env": "secrets.env"}')
    monkeypatch.delenv("DEMO_MODEL_KEY", raising=False)
    monkeypatch.delenv("EMPTY_VAR", raising=False)

    try:
        load_config_env(config_path)

        assert os.environ["DEMO_MODEL_KEY"] == "abc"
        # 空值与官方一致保留为空字符串
        assert os.environ["EMPTY_VAR"] == ""
    finally:
        os.environ.pop("DEMO_MODEL_KEY", None)
        os.environ.pop("EMPTY_VAR", None)


def test_env_field_overrides_existing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """与官方 dev 一致：env 字段的值覆盖已有环境变量。"""
    (tmp_path / ".env").write_text("DEMO_MODEL_KEY=from-file\n", encoding="utf-8")
    config_path = _write_config(tmp_path, '{"env": ".env"}')
    monkeypatch.setenv("DEMO_MODEL_KEY", "from-shell")

    try:
        load_config_env(config_path)

        assert os.environ["DEMO_MODEL_KEY"] == "from-file"
    finally:
        monkeypatch.undo()
        os.environ.pop("DEMO_MODEL_KEY", None)


def test_inline_dict_form(tmp_path: Path) -> None:
    """字典形态：内联键值映射，值统一转字符串。"""
    config_path = _write_config(tmp_path, '{"env": {"DEMO_A": 1, "DEMO_B": "x"}}')

    try:
        load_config_env(config_path)

        assert os.environ["DEMO_A"] == "1"
        assert os.environ["DEMO_B"] == "x"
    finally:
        os.environ.pop("DEMO_A", None)
        os.environ.pop("DEMO_B", None)


def test_missing_env_field_is_noop(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """字段缺省：不做任何事，不自动读 cwd 的 .env。"""
    (tmp_path / ".env").write_text("DEMO_UNTOUCHED=1\n", encoding="utf-8")
    config_path = _write_config(tmp_path, '{"graphs": {}}')
    monkeypatch.delenv("DEMO_UNTOUCHED", raising=False)

    load_config_env(config_path)

    assert "DEMO_UNTOUCHED" not in os.environ


def test_missing_env_file_warns_and_continues(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """文件不存在：告警但不报错（对齐官方 deploy 行为）。"""
    config_path = _write_config(tmp_path, '{"env": "nope.env"}')

    load_config_env(config_path)

    assert "不存在" in capsys.readouterr().err


def test_invalid_env_field_type_raises(tmp_path: Path) -> None:
    """类型非法：显式报错（对齐官方 schema 校验行为）。"""
    config_path = _write_config(tmp_path, '{"env": 42}')

    with pytest.raises(ValueError, match="env"):
        load_config_env(config_path)
