from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional


def supported_schema_type_strings() -> List[str]:
    return [
        "null",
        "bool",
        "int8",
        "int16",
        "int32",
        "int64",
        "uint8",
        "uint16",
        "uint32",
        "uint64",
        "float32",
        "float64",
        "string",
        "struct",
        "list",
    ]


def compact_function_def(defn: Dict[str, Any]) -> Dict[str, Any]:
    signature = defn.get("signature", {}) or {}
    args = signature.get("args", []) or []
    return {
        "name": defn.get("name", ""),
        "kind": defn.get("kind", ""),
        "aliases": defn.get("aliases", []),
        "arg_count": len(args) if isinstance(args, list) else None,
        "allowed_contexts": defn.get("allowed_contexts", []),
        "requirements": defn.get("requirements", []),
    }


def _minimize_syntax_construct(node: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "id": node.get("id", ""),
        "type": node.get("type", node.get("kind", "")),
        "title": node.get("title", ""),
    }
    if "status" in node and node.get("status") is not None:
        out["status"] = node.get("status")
    placement = node.get("placement")
    if placement:
        out["placement"] = placement
    constraints = node.get("constraints") or []
    if constraints:
        out["constraints"] = constraints
    syntax = node.get("syntax") or []
    if syntax:
        out["syntax"] = syntax
    return out


def filter_syntax_constructs(constructs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    keep_prefixes = ("statement", "select", "from", "expr", "window")

    def keep(node_id: str) -> bool:
        return any(node_id == p or node_id.startswith(p + ".") for p in keep_prefixes)

    def filter_node(node: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        node_id = str(node.get("id", ""))
        children = node.get("children", []) or []
        filtered_children: List[Dict[str, Any]] = []
        for child in children:
            filtered = filter_node(child)
            if filtered is not None:
                filtered_children.append(filtered)

        if keep(node_id) or filtered_children:
            out = _minimize_syntax_construct(node)
            if filtered_children:
                out["children"] = filtered_children
            return out
        return None

    result: List[Dict[str, Any]] = []
    for node in constructs:
        filtered = filter_node(node)
        if filtered is not None:
            result.append(filtered)
    return result


@dataclass(frozen=True)
class CapabilitiesDigest:
    schema_type_strings: List[str]
    functions: List[Dict[str, Any]]
    syntax_capabilities: Dict[str, Any]

    def to_json(self) -> Dict[str, Any]:
        return {
            "schema_type_strings": self.schema_type_strings,
            "functions": self.functions,
            "syntax_capabilities": self.syntax_capabilities,
        }


def build_capabilities_digest(
    functions: List[Dict[str, Any]], syntax_caps: Dict[str, Any]
) -> CapabilitiesDigest:
    compact_functions = [compact_function_def(f) for f in functions]
    constructs = filter_syntax_constructs(syntax_caps.get("constructs", []) or [])
    return CapabilitiesDigest(
        schema_type_strings=supported_schema_type_strings(),
        functions=compact_functions,
        syntax_capabilities={
            "dialect": syntax_caps.get("dialect", ""),
            "ir": syntax_caps.get("ir", ""),
            "constructs": constructs,
        },
    )
