import re
from typing import Any

import yaml
from pydantic import BaseModel, Field


class ParameterSpec(BaseModel):
    name: str
    description: str = ""
    type: str = "String"
    required: bool = False
    allowed_values: list[str] = Field(default_factory=list, alias="allowedValues")

    model_config = {"populate_by_name": True}


class OperationSpec(BaseModel):
    name: str
    signature: str = ""
    description: str = ""
    operation_id: int | None = None
    parameters: list[ParameterSpec] = Field(default_factory=list)
    returns: str | None = None


class ModelCatalog(BaseModel):
    library: str = ""
    methods: list[OperationSpec] = Field(default_factory=list)


_ID_PATTERN = re.compile(r"\(id=(\d+)\)")


def _parse_allowed_values(raw: Any) -> list[str]:
    if not raw:
        return []
    if isinstance(raw, list):
        return [str(v) for v in raw]
    return []


def _parse_model_yaml(content: str) -> ModelCatalog:
    library_match = re.search(r"^library:\s*(.+)$", content, re.MULTILINE)
    library = library_match.group(1).strip() if library_match else ""

    methods: list[OperationSpec] = []
    current_op: dict[str, Any] | None = None
    current_param: dict[str, Any] | None = None
    in_parameters = False
    collecting_allowed = False

    for line in content.splitlines():
        if line.startswith("    - name:") and not in_parameters:
            if current_op:
                methods.append(_build_operation(current_op))
            current_op = {"name": line.split(":", 1)[1].strip(), "parameters": []}
            current_param = None
            in_parameters = False
            collecting_allowed = False
            continue

        if current_op is None:
            continue

        stripped = line.strip()
        if stripped.startswith("signature:"):
            current_op["signature"] = stripped.split(":", 1)[1].strip()
        elif stripped.startswith("description:") and not in_parameters:
            current_op["description"] = stripped.split(":", 1)[1].strip()
        elif stripped == "parameters:":
            in_parameters = True
        elif stripped == "returns:":
            if current_param:
                current_op["parameters"].append(current_param)
                current_param = None
            in_parameters = False
            collecting_allowed = False
        elif in_parameters and line.startswith("        - name:"):
            if current_param:
                current_op["parameters"].append(current_param)
            current_param = {"name": line.split(":", 1)[1].strip()}
            collecting_allowed = False
        elif in_parameters and current_param is not None:
            if stripped.startswith("description:"):
                current_param["description"] = stripped.split(":", 1)[1].strip()
            elif stripped.startswith("type:"):
                current_param["type"] = stripped.split(":", 1)[1].strip()
            elif stripped.startswith("required:"):
                current_param["required"] = stripped.split(":", 1)[1].strip().lower() == "true"
            elif stripped.startswith("allowedValues:"):
                current_param["allowedValues"] = []
                collecting_allowed = True
            elif collecting_allowed and stripped.startswith("- "):
                current_param.setdefault("allowedValues", []).append(stripped[2:].strip())

    if current_param and current_op:
        current_op["parameters"].append(current_param)
    if current_op:
        methods.append(_build_operation(current_op))

    return ModelCatalog(library=library, methods=methods)


def _build_operation(raw: dict[str, Any]) -> OperationSpec:
    desc = raw.get("description", "")
    op_id = None
    match = _ID_PATTERN.search(desc)
    if match:
        op_id = int(match.group(1))

    params = []
    for p in raw.get("parameters", []):
        params.append(
            ParameterSpec(
                name=p["name"],
                description=p.get("description", ""),
                type=p.get("type", "String"),
                required=bool(p.get("required", False)),
                allowedValues=_parse_allowed_values(p.get("allowedValues")),
            )
        )

    return OperationSpec(
        name=raw["name"],
        signature=raw.get("signature", ""),
        description=desc,
        operation_id=op_id,
        parameters=params,
        returns=raw.get("returns"),
    )


def load_model_catalog(path: str) -> ModelCatalog:
    with open(path, encoding="utf-8") as f:
        content = f.read()
    return _parse_model_yaml(content)
