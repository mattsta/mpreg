import orjson

from mpreg.model import RPCCommand, RPCError, RPCRequest, RPCResponse
from mpreg.serialization import JsonSerializer


def test_json_serializer_serialize():
    serializer = JsonSerializer()
    data = {"key": "value", "number": 123, "boolean": True}
    serialized_data = serializer.serialize(data)
    assert isinstance(serialized_data, bytes)
    assert orjson.loads(serialized_data) == data


def test_json_serializer_deserialize():
    serializer = JsonSerializer()
    data_bytes = b'{"key": "value", "number": 123, "boolean": true}'
    deserialized_data = serializer.deserialize(data_bytes)
    assert isinstance(deserialized_data, dict)
    assert deserialized_data == {"key": "value", "number": 123, "boolean": True}


def test_json_serializer_with_pydantic_model():
    serializer = JsonSerializer()
    cmd = RPCCommand(
        name="test", fun="func", args=("a",), kwargs={"b": 1}, locs=frozenset(["loc1"])
    )
    req = RPCRequest(cmds=(cmd,), u="123")

    serialized_req = serializer.serialize(req.model_dump())
    deserialized_req = RPCRequest.model_validate(serializer.deserialize(serialized_req))

    assert deserialized_req == req


def test_json_serializer_with_pydantic_response_error():
    serializer = JsonSerializer()
    error = RPCError(code=100, message="Test Error", details=None)
    response = RPCResponse(r=None, error=error, u="456")

    serialized_response = serializer.serialize(response.model_dump())
    deserialized_response = RPCResponse.model_validate(
        serializer.deserialize(serialized_response)
    )

    assert deserialized_response == response
