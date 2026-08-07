from mpreg.core.discovery_signatures import (
    SIGNATURE_KEY,
    sign_summary,
    verify_summary,
)


def test_sign_and_verify() -> None:
    payload = {"source_cluster": "c1", "summaries": [{"namespace": "ns"}]}
    signed = sign_summary(payload, "s3cret")
    assert SIGNATURE_KEY in signed
    assert verify_summary(signed, "s3cret")
    assert not verify_summary(signed, "wrong")
    tampered = dict(signed)
    tampered["source_cluster"] = "evil"
    assert not verify_summary(tampered, "s3cret")


def test_empty_secret_noop() -> None:
    payload = {"a": 1}
    assert sign_summary(payload, "") == payload
    assert verify_summary(payload, "") is True
