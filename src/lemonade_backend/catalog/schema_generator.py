from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, create_model

from lemonade_backend.catalog.loader import ModelCatalog, OperationSpec, ParameterSpec

_TYPE_MAP: dict[str, type] = {
    "Integer": int,
    "Float": float,
    "String": str,
    "Boolean": bool,
    "String[]": list[str],
    "Dataframe": str,
}


def _python_type(param: ParameterSpec) -> Any:
    base = _TYPE_MAP.get(param.type, str)
    if param.required:
        return base
    return base | None


def _field_for_param(param: ParameterSpec) -> tuple[Any, Any]:
    py_type = _python_type(param)
    default = ... if param.required else None
    field_kwargs: dict[str, Any] = {"description": param.description}
    if param.allowed_values:
        field_kwargs["json_schema_extra"] = {"allowedValues": param.allowed_values}
    return (py_type, Field(default, **field_kwargs))


def build_param_model(op: OperationSpec) -> type[BaseModel]:
    fields = {p.name: _field_for_param(p) for p in op.parameters}
    return create_model(f"{op.name}_params", **fields)


def build_all_param_models(catalog: ModelCatalog) -> dict[str, type[BaseModel]]:
    return {op.name: build_param_model(op) for op in catalog.methods}


def operation_to_schema(op: OperationSpec) -> dict[str, Any]:
    props: dict[str, Any] = {}
    required: list[str] = []
    for p in op.parameters:
        type_name = p.type.lower().replace("[]", "_array")
        json_type = {
            "integer": "integer",
            "float": "number",
            "string": "string",
            "boolean": "boolean",
            "string_array": "array",
            "dataframe": "string",
        }.get(type_name, "string")
        prop: dict[str, Any] = {"type": json_type, "description": p.description}
        if p.type == "String[]":
            prop["items"] = {"type": "string"}
        if p.allowed_values:
            prop["enum"] = [v.split(":")[0] if ":" in v else v for v in p.allowed_values]
        props[p.name] = prop
        if p.required:
            required.append(p.name)

    return {
        "name": op.name,
        "operation_id": op.operation_id,
        "description": op.description,
        "signature": op.signature,
        "parameters": {"type": "object", "properties": props, "required": required},
    }
