"""Independent ARXML input/output adapter for autosar-data-py.

This adapter follows full model references. Only the comparison output reduces
type references to short names, because the Rust AST discards some CP paths.
"""

import hashlib
import importlib.metadata
import json
import platform
import sys
from pathlib import Path

from autosar_data import AutosarDataError, AutosarModel


PINNED_VERSION = "0.17.0"


def child(element, name):
    result = element.get_sub_element(name)
    if result is None:
        raise ValueError(f"missing {name} in {element.xml_path}")
    return result


def data(element, name):
    return child(element, name).character_data


def descendants(element, name):
    for sub in element.sub_elements:
        if sub.element_name == name:
            yield sub
        yield from descendants(sub, name)


def only(items, description):
    items = list(items)
    if len(items) != 1:
        raise ValueError(f"expected one {description}, found {len(items)}")
    return items[0]


class Reference:
    def __init__(self, path, strict):
        self.model = AutosarModel()
        _, warnings = self.model.load_file(str(path), strict)
        self.warnings = [str(warning) for warning in warnings]
        self.elements = [e for _, e in self.model.elements_dfs]
        self.paths = []

    def follow(self, reference):
        target = reference.reference_target
        self.paths.append({"reference": reference.character_data, "target": target.path})
        return target

    def normalize(self, element):
        category = data(element, "CATEGORY")
        name = element.item_name
        if category == "VALUE":
            if element.element_name == "APPLICATION-PRIMITIVE-DATA-TYPE":
                mappings = [r.parent for r in self.model.get_references_to(element.path)
                            if r.element_name == "APPLICATION-DATA-TYPE-REF"]
                targets = [self.follow(child(m, "IMPLEMENTATION-DATA-TYPE-REF"))
                           for m in mappings]
                paths = {target.path for target in targets}
                if len(paths) != 1:
                    raise ValueError(f"ambiguous or missing implementation mapping: {element.path}")
                result = self.normalize(targets[0])
                result["name"] = name
                return result
            reference = only(descendants(element, "BASE-TYPE-REF"), "base type reference")
            self.follow(reference)
            definition = {"kind": "reference",
                          "target": reference.character_data.rsplit("/", 1)[-1].lower(),
                          "string_size": None}
        elif category == "TYPE_REFERENCE":
            reference = (only(descendants(element, "IMPLEMENTATION-DATA-TYPE-REF"),
                              "implementation reference")
                         if element.element_name == "IMPLEMENTATION-DATA-TYPE"
                         else child(element, "TYPE-REFERENCE-REF"))
            self.follow(reference)
            target = reference.character_data.rsplit("/", 1)[-1].lower()
            size = element.get_sub_element("ARRAY-SIZE")
            definition = {"kind": "reference", "target": target,
                          "string_size": int(size.character_data) if size is not None else None}
        elif category == "STRING":
            definition = {"kind": "reference", "target": "string", "string_size": 0}
        elif category == "STRUCTURE":
            fields = []
            if element.element_name == "APPLICATION-RECORD-DATA-TYPE":
                members = child(element, "ELEMENTS").sub_elements
                for member in members:
                    reference = child(member, "TYPE-TREF")
                    target = self.follow(reference)
                    fields.append({"name": member.item_name, "target": target.item_name,
                                   "in_place": False})
            elif element.element_name == "IMPLEMENTATION-DATA-TYPE":
                for member in child(element, "SUB-ELEMENTS").sub_elements:
                    # Inline composites occupy the field directly; references
                    # remain separate member definitions in the scoped registry.
                    fields.append({"name": member.item_name, "target": member.item_name,
                                   "in_place": data(member, "CATEGORY") in ("STRUCTURE", "ARRAY")})
            else:
                for member in child(element, "SUB-ELEMENTS").sub_elements:
                    type_ref = child(member, "TYPE-REFERENCE")
                    target = self.follow(child(type_ref, "TYPE-REFERENCE-REF"))
                    fields.append({"name": member.item_name, "target": target.item_name,
                                   "in_place": data(type_ref, "INPLACE") is True})
            definition = {"kind": "structure", "fields": fields}
        elif category == "ARRAY":
            if element.element_name == "APPLICATION-ARRAY-DATA-TYPE":
                member = child(element, "ELEMENT")
                target = self.follow(child(member, "TYPE-TREF"))
                dynamic = data(member, "ARRAY-SIZE-SEMANTICS") == "VARIABLE-SIZE"
                size = 0 if dynamic else int(data(member, "MAX-NUMBER-OF-ELEMENTS"))
                in_place = False
            elif element.element_name == "IMPLEMENTATION-DATA-TYPE":
                target = only(child(element, "SUB-ELEMENTS").sub_elements, "array member")
                dynamic = data(target, "ARRAY-SIZE-SEMANTICS") == "VARIABLE-SIZE"
                size = 0 if dynamic else int(data(target, "ARRAY-SIZE"))
                in_place = False
            else:
                argument = child(child(element, "TEMPLATE-ARGUMENTS"), "CPP-TEMPLATE-ARGUMENT")
                target = self.follow(child(argument, "TEMPLATE-TYPE-REF"))
                size = int(data(element, "ARRAY-SIZE"))
                in_place = data(argument, "INPLACE") is True
            definition = {"kind": "array", "size": size,
                          "element": target.item_name, "in_place": in_place}
        elif category == "VECTOR":
            argument = child(child(element, "TEMPLATE-ARGUMENTS"), "CPP-TEMPLATE-ARGUMENT")
            target = self.follow(child(argument, "TEMPLATE-TYPE-REF"))
            definition = {"kind": "vector", "element": target.item_name}
        else:
            raise ValueError(f"unsupported reference category: {category}")
        return {"name": name, "category": category, "definition": definition}

    def resolve_ap(self, service_id, entry_id):
        deployments = [e for e in self.elements
                       if e.element_name == "SOMEIP-SERVICE-INTERFACE-DEPLOYMENT"
                       and data(e, "SERVICE-INTERFACE-ID") == service_id]
        deployment = only(deployments, f"deployment for service {service_id}")
        targets = []
        for event in descendants(deployment, "SOMEIP-EVENT-DEPLOYMENT"):
            if data(event, "EVENT-ID") == entry_id:
                targets.append(self.follow(child(event, "EVENT-REF")))
        for field in descendants(deployment, "SOMEIP-FIELD-DEPLOYMENT"):
            notifier = field.get_sub_element("NOTIFIER")
            if notifier is not None and data(notifier, "EVENT-ID") == entry_id:
                targets.append(self.follow(child(field, "FIELD-REF")))
        event = only(targets, f"event/field {entry_id}")
        return self.follow(child(event, "TYPE-TREF"))

    def resolve_cp(self, service_id, header_id):
        services = [e for e in self.elements if e.element_name == "PROVIDED-SERVICE-INSTANCE"
                    and e.get_sub_element("SERVICE-IDENTIFIER") is not None
                    and data(e, "SERVICE-IDENTIFIER") == service_id]
        if not services:
            raise ValueError(f"service {service_id} not found")
        headers = [e for e in self.elements if e.element_name == "HEADER-ID"
                   and e.character_data == header_id]
        targets = {}
        for header in headers:
            trigger = self.follow(child(header.parent, "PDU-TRIGGERING-REF"))
            pdu = self.follow(child(trigger, "I-PDU-REF"))
            # TP mappings are explicit model references, not name substitutions.
            for reference in self.model.get_references_to(pdu.path):
                if reference.element_name == "TRANSPORT-PDU-REF":
                    pdu = self.follow(child(reference.parent, "TP-SDU-REF"))
                    break
            for mapping in descendants(pdu, "I-SIGNAL-TO-I-PDU-MAPPING"):
                signal = self.follow(child(mapping, "I-SIGNAL-REF"))
                system_signal = self.follow(child(signal, "SYSTEM-SIGNAL-REF"))
                for reference in self.model.get_references_to(system_signal.path):
                    parent = reference.parent
                    if parent.element_name == "SENDER-RECEIVER-TO-SIGNAL-MAPPING":
                        iref = child(parent, "DATA-ELEMENT-IREF")
                        prototype = self.follow(child(iref, "TARGET-DATA-PROTOTYPE-REF"))
                        target = self.follow(child(prototype, "TYPE-TREF"))
                        targets[target.path] = target
                    elif parent.element_name == "CLIENT-SERVER-TO-SIGNAL-MAPPING":
                        iref = child(parent, "CLIENT-SERVER-OPERATION-IREF")
                        operation = self.follow(child(iref, "TARGET-OPERATION-REF"))
                        for argument in child(operation, "ARGUMENTS").sub_elements:
                            if data(argument, "DIRECTION") == "IN":
                                target = self.follow(child(argument, "TYPE-TREF"))
                                targets[target.path] = target
        return only(targets.values(), f"payload type for header {header_id}")

    def inspect(self, query):
        operation = query["operation"]
        if operation in ("type", "cp_type", "implementation_type", "ap_type"):
            target = self.model.get_element_by_path(query["type_path"])
            if target is None:
                raise ValueError(f"type not found: {query['type_path']}")
        elif operation == "resolve_cp":
            target = self.resolve_cp(query["service_id"], query["header_id"])
        elif operation == "resolve":
            is_ap = any(e.element_name == "SOMEIP-SERVICE-INTERFACE-DEPLOYMENT"
                        for e in self.elements)
            if is_ap:
                target = self.resolve_ap(query["service_id"], query["entry_id"])
            else:
                target = self.resolve_cp(query["service_id"],
                                         (query["service_id"] << 16) | query["entry_id"])
        else:
            raise ValueError(f"unknown operation: {operation}")
        return self.normalize(target)


def run(manifest_path):
    version = importlib.metadata.version("autosar-data")
    if version != PINNED_VERSION:
        raise RuntimeError(f"expected autosar-data {PINNED_VERSION}, installed {version}")
    manifest = json.loads(manifest_path.read_text())
    if manifest["version"] != 1:
        raise ValueError("unsupported manifest version")
    report = {"metadata": {"autosar_data": version, "python": platform.python_version()},
              "cases": []}
    models = {}
    for case in manifest["cases"]:
        path = (manifest_path.parent / case["input"]["file"]).resolve()
        strict = case["input"].get("strict", True)
        row = {"id": case["id"], "load": {"strict": strict,
               "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}}
        try:
            key = (path, strict)
            if key not in models:
                models[key] = Reference(path, strict)
            reference = models[key]
            row["load"]["warnings"] = reference.warnings
        except Exception as error:
            row["output"] = {"error": str(error)}
            row["load"]["status"] = "error"
        else:
            row["load"]["status"] = "ok"
            reference.paths = []
            try:
                row["output"] = {"ok": reference.inspect(case["input"]["query"])}
            except (AutosarDataError, ValueError) as error:
                row["output"] = {"error": str(error)}
            row["reference_paths"] = reference.paths
        report["cases"].append(row)
    return report


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: autosar_reference.py MANIFEST.json")
    print(json.dumps(run(Path(sys.argv[1]).resolve()), ensure_ascii=True))
