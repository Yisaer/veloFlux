"""Enumerate every type with autosar-data-py, retaining full reference paths."""

import hashlib
import importlib.metadata
import json
import platform
import sys
from pathlib import Path

from autosar_data import AutosarDataError

from autosar_reference import PINNED_VERSION, Reference, child, data, descendants, only


TYPE_ELEMENTS = {
    "APPLICATION-PRIMITIVE-DATA-TYPE",
    "APPLICATION-ARRAY-DATA-TYPE",
    "APPLICATION-RECORD-DATA-TYPE",
    "IMPLEMENTATION-DATA-TYPE",
    "STD-CPP-IMPLEMENTATION-DATA-TYPE",
    "SW-BASE-TYPE",
}


class AdapterGap(ValueError):
    """A source construct for which the inventory adapter has no comparison."""


def optional(element, name):
    found = element.get_sub_element(name)
    return found.character_data if found is not None else None


class InventoryReference(Reference):
    def target(self, reference, role, facts):
        target = self.follow(reference)
        facts["references"].append({"role": role, "path": target.path})
        return target

    def base(self, element):
        return {"bits": int(data(element, "BASE-TYPE-SIZE")),
                "encoding": data(element, "BASE-TYPE-ENCODING")}

    def implementation_member(self, element, role, facts):
        """Describe CP inline members, including nested arrays and structures."""
        category = data(element, "CATEGORY")
        member = {"name": element.item_name, "category": category}
        for name in ("ARRAY-SIZE", "ARRAY-SIZE-SEMANTICS", "ARRAY-SIZE-HANDLING"):
            value = optional(element, name)
            if value is not None:
                member[name] = value
        if category in ("VALUE", "TYPE_REFERENCE"):
            tag = "BASE-TYPE-REF" if category == "VALUE" else "IMPLEMENTATION-DATA-TYPE-REF"
            reference = only(descendants(element, tag), tag)
            member["target"] = self.target(reference, role, facts).path
        elif category in ("STRUCTURE", "ARRAY"):
            member["members"] = [
                self.implementation_member(sub, f"{role}.{index}", facts)
                for index, sub in enumerate(child(element, "SUB-ELEMENTS").sub_elements)
            ]
        else:
            raise AdapterGap(f"CP implementation member category {category}")
        return member

    def describe(self, element):
        category = data(element, "CATEGORY")
        tag = element.element_name
        if tag not in TYPE_ELEMENTS:
            raise AdapterGap(f"type element {tag}")
        facts = {"references": [], "constraints": {}, "base": None}
        name = element.item_name
        if tag == "SW-BASE-TYPE":
            definition = {"kind": "base", **self.base(element)}
        elif tag == "IMPLEMENTATION-DATA-TYPE" and category in ("ARRAY", "STRUCTURE"):
            definition = {"kind": "cp_implementation", "members": [
                self.implementation_member(sub, f"member.{index}", facts)
                for index, sub in enumerate(child(element, "SUB-ELEMENTS").sub_elements)
            ]}
        elif category == "VALUE":
            if tag == "APPLICATION-PRIMITIVE-DATA-TYPE":
                mappings = [ref.parent for ref in self.model.get_references_to(element.path)
                            if ref.element_name == "APPLICATION-DATA-TYPE-REF"]
                targets = [self.target(child(mapping, "IMPLEMENTATION-DATA-TYPE-REF"),
                                       "implementation_mapping", facts) for mapping in mappings]
                unique = {target.path: target for target in targets}
                mapped = only(unique.values(), "implementation mapping")
                normalized, mapped_facts = self.describe(mapped)
                normalized["name"] = name
                facts["references"].extend(mapped_facts["references"])
                facts["base"] = mapped_facts["base"]
                return normalized, facts
            if tag == "STD-CPP-IMPLEMENTATION-DATA-TYPE":
                definition = {"kind": "native", "emitter": optional(element, "TYPE-EMITTER")}
            else:
                reference = only(descendants(element, "BASE-TYPE-REF"), "base type reference")
                base = self.target(reference, "base", facts)
                facts["base"] = self.base(base)
                definition = {"kind": "reference", "target": base.item_name.lower(),
                              "string_size": None}
        elif category == "TYPE_REFERENCE":
            reference = (only(descendants(element, "IMPLEMENTATION-DATA-TYPE-REF"), "implementation reference")
                         if tag == "IMPLEMENTATION-DATA-TYPE"
                         else child(element, "TYPE-REFERENCE-REF"))
            target = self.target(reference, "base", facts)
            definition = {"kind": "reference", "target": target.item_name.lower(),
                          "string_size": optional(element, "ARRAY-SIZE")}
            # Native C++ standard types have no declared SW-BASE-TYPE width.
            # Keep that absence explicit rather than infer expected bits from names.
            if tag == "STD-CPP-IMPLEMENTATION-DATA-TYPE":
                facts["native_base"] = {"path": target.path,
                                        "category": data(target, "CATEGORY")}
        elif category == "STRING":
            definition = {"kind": "reference", "target": "string", "string_size": 0}
            if tag == "APPLICATION-PRIMITIVE-DATA-TYPE":
                props = only(descendants(element, "SW-TEXT-PROPS"), "text properties")
                base = self.target(child(props, "BASE-TYPE-REF"), "base", facts)
                facts["base"] = self.base(base)
                for key in ("ARRAY-SIZE-SEMANTICS", "SW-MAX-TEXT-SIZE"):
                    value = optional(props, key)
                    if value is not None:
                        facts["constraints"][key] = value
            else:
                facts["native_base"] = {"path": element.path, "category": category}
        elif category == "STRUCTURE":
            fields = []
            if tag == "APPLICATION-RECORD-DATA-TYPE":
                members = child(element, "ELEMENTS").sub_elements
                for index, member in enumerate(members):
                    target = self.target(child(member, "TYPE-TREF"), f"field.{index}", facts)
                    fields.append({"name": member.item_name, "target": target.item_name,
                                   "in_place": False})
            else:
                for index, member in enumerate(child(element, "SUB-ELEMENTS").sub_elements):
                    type_ref = child(member, "TYPE-REFERENCE")
                    target = self.target(child(type_ref, "TYPE-REFERENCE-REF"),
                                         f"field.{index}", facts)
                    fields.append({"name": member.item_name, "target": target.item_name,
                                   "in_place": data(type_ref, "INPLACE") is True})
            definition = {"kind": "structure", "fields": fields}
        elif category in ("ARRAY", "VECTOR"):
            if tag == "APPLICATION-ARRAY-DATA-TYPE":
                member = child(element, "ELEMENT")
                target = self.target(child(member, "TYPE-TREF"), "element", facts)
                semantics = data(member, "ARRAY-SIZE-SEMANTICS")
                maximum = int(data(member, "MAX-NUMBER-OF-ELEMENTS"))
                definition = {"kind": "array", "size": 0 if semantics == "VARIABLE-SIZE" else maximum,
                              "element": target.item_name, "in_place": False}
                if semantics == "VARIABLE-SIZE":
                    facts["constraints"]["maximum_elements"] = maximum
            else:
                argument = child(child(element, "TEMPLATE-ARGUMENTS"), "CPP-TEMPLATE-ARGUMENT")
                target = self.target(child(argument, "TEMPLATE-TYPE-REF"), "element", facts)
                definition = {"kind": category.lower(), "element": target.item_name}
                if category == "ARRAY":
                    definition.update(size=int(data(element, "ARRAY-SIZE")),
                                      in_place=data(argument, "INPLACE") is True)
        else:
            raise AdapterGap(f"type category {category} in {tag}")
        return {"name": name, "category": category, "definition": definition}, facts


def inventory(document, parent):
    path = (parent / document["file"]).resolve()
    result = {"id": document["id"], "file": document["file"],
              "sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "types": []}
    try:
        reference = InventoryReference(path, document.get("strict", True))
    except AutosarDataError as error:
        result["load"] = {"status": "error", "error": str(error).replace(str(path), "$INPUT")}
        return result
    result["load"] = {"status": "ok", "warnings": reference.warnings,
                      "schema_version": str(reference.model.files[0].version)}
    # Include new named data-type elements even if describe() cannot handle
    # them yet; otherwise adapter omissions would disappear from coverage.
    types = (e for e in reference.elements if e.element_name == "SW-BASE-TYPE"
             or (e.element_name.endswith("-DATA-TYPE") and e.item_name is not None))
    for element in sorted(types,
                          key=lambda e: e.path):
        row = {"path": element.path, "element": element.element_name,
               "category": optional(element, "CATEGORY") or "UNSPECIFIED"}
        try:
            row["definition"], row["facts"] = reference.describe(element)
            row["status"] = "ok"
        except AdapterGap as error:
            row.update(status="adapter_gap", error=str(error))
        except (AutosarDataError, ValueError) as error:
            row.update(status="reference_error", error=str(error))
        result["types"].append(row)
    return result


def run(path):
    version = importlib.metadata.version("autosar-data")
    if version != PINNED_VERSION:
        raise RuntimeError(f"expected autosar-data {PINNED_VERSION}, installed {version}")
    manifest = json.loads(path.read_text())
    if manifest["version"] != 1:
        raise ValueError("unsupported inventory version")
    return {"metadata": {"autosar_data": version, "python": platform.python_version()},
            "documents": [inventory(document, path.parent) for document in manifest["documents"]]}


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: autosar_inventory.py INVENTORY.json")
    print(json.dumps(run(Path(sys.argv[1]).resolve()), ensure_ascii=True))
