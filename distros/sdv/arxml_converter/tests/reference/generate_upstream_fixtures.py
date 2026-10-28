"""Generate upstream ARXML happy-path adaptations without downloading sources.

Construction patterns adapted from DanielT/autosar-data-py at
1e064d751eadcb184961f025754e0605099b56eb. Copyright (c) 2021 Daniel Thaler.
Used under the MIT license in fixtures/UPSTREAM-LICENSE-MIT.
"""

import argparse
import hashlib
import importlib.metadata
import json
from pathlib import Path

from autosar_data import AutosarModel, AutosarVersion
from autosar_data.abstraction import AutosarModelAbstraction
from autosar_data.abstraction.datatype import (
    ApplicationArraySize,
    ApplicationPrimitiveCategory,
    BaseTypeEncoding,
    CompuMethodContent,
    ImplementationDataTypeSettings,
)

from autosar_reference import PINNED_VERSION


UPSTREAM = "https://github.com/DanielT/autosar-data-py"
COMMIT = "1e064d751eadcb184961f025754e0605099b56eb"
VERSION = AutosarVersion.AUTOSAR_00054


def model_and_package(filename):
    model = AutosarModelAbstraction.create(filename, version=VERSION)
    return model, model.get_or_create_package("/package")


def base_type(package):
    return package.create_sw_base_type("SwBaseType", 32, BaseTypeEncoding.TwosComplement)


def implementation_array(package):
    base = base_type(package)
    package.create_implementation_data_type(ImplementationDataTypeSettings.Array(
        "ImplArray", length=10,
        element_type=ImplementationDataTypeSettings.Value("ImplValue", base_type=base),
    ))


def implementation_structure(package):
    base = base_type(package)
    package.create_implementation_data_type(ImplementationDataTypeSettings.Structure(
        "ImplStruct",
        elements=[ImplementationDataTypeSettings.Value("ImplValue", base_type=base)],
    ))


def implementation_reference(package):
    target = package.create_implementation_data_type(ImplementationDataTypeSettings.Value(
        "ImplValue", base_type=base_type(package),
    ))
    compu = package.create_compu_method("CompuMethod", CompuMethodContent.Identical())
    constraint = package.create_data_constr("DataConstraint")
    package.create_implementation_data_type(ImplementationDataTypeSettings.TypeReference(
        "ImplRef", reftype=target, compu_method=compu, data_constraint=constraint,
    ))


def add_mapping(package, application):
    """Apply the upstream mapping pattern to otherwise unmapped application VALUEs."""
    implementation = package.create_implementation_data_type(
        ImplementationDataTypeSettings.Value("ImplementationDataType", base_type=base_type(package))
    )
    package.create_data_type_mapping_set("DataTypeMappingSet").create_data_type_map(
        implementation, application
    )


def application_mapping(package):
    application = package.create_application_primitive_data_type(
        "ApplicationPrimitiveDataType", ApplicationPrimitiveCategory.Value
    )
    add_mapping(package, application)


def empty_record(package, name):
    record = package.create_application_record_data_type(name)
    # Explicit empty containers preserve the upstream empty record while allowing
    # the current production parser and reference adapter to inspect its members.
    record.element.create_sub_element("ELEMENTS")
    return record


def application_arrays(package):
    compu = package.create_compu_method("Compumethod", CompuMethodContent.Identical())
    unit = package.create_unit("Unit")
    constraint = package.create_data_constr("DataConstraint")
    primitive = package.create_application_primitive_data_type(
        "ApplicationPrimitiveDataType", ApplicationPrimitiveCategory.Value,
        compu_method=compu, unit=unit, data_constraint=constraint,
    )
    array = package.create_application_array_data_type(
        "ApplicationArrayDataType", primitive, ApplicationArraySize.Fixed(10)
    )
    record = empty_record(package, "ApplicationRecordDataType")
    package.create_application_array_data_type("ArrayOfRecords", record, ApplicationArraySize.Fixed(10))
    package.create_application_array_data_type("ArrayOfArrays", array, ApplicationArraySize.Fixed(10))
    add_mapping(package, primitive)


def application_record(package):
    record = package.create_application_record_data_type("ApplicationRecordDataType")
    primitive = package.create_application_primitive_data_type(
        "Element_ApplicationPrimitiveDataType", ApplicationPrimitiveCategory.Value
    )
    record.create_record_element("Element1", primitive)
    record.create_record_element("Element2", empty_record(package, "Element_ApplicationRecordDataType"))
    array = package.create_application_array_data_type(
        "Element_ApplicationArrayDataType", primitive, ApplicationArraySize.Fixed(10)
    )
    record.create_record_element("Element3", array)
    add_mapping(package, primitive)


def application_array_variable(package):
    primitive = package.create_application_primitive_data_type(
        "ApplicationPrimitiveDataType", ApplicationPrimitiveCategory.Value
    )
    package.create_application_array_data_type(
        "ApplicationArrayDataType", primitive, ApplicationArraySize.VariableLinear(20)
    )
    add_mapping(package, primitive)


def implementation_mixed_members(package):
    base = base_type(package)
    other = package.create_implementation_data_type(ImplementationDataTypeSettings.Value(
        "OtherImplType", base_type=base,
    ))
    # Preserve the order and spelling of the four supported upstream members.
    package.create_implementation_data_type(ImplementationDataTypeSettings.Structure(
        "ImpleDataType",
        elements=[
            ImplementationDataTypeSettings.Value("ImplValue", base_type=base),
            ImplementationDataTypeSettings.Structure("ImplStruct", elements=[
                ImplementationDataTypeSettings.Value("StructImplValue", base_type=base),
            ]),
            ImplementationDataTypeSettings.Array(
                "ImplArray", length=10,
                element_type=ImplementationDataTypeSettings.Value("ArrayImplValue", base_type=base),
            ),
            ImplementationDataTypeSettings.TypeReference("ImplTypeRef", reftype=other),
        ],
    ))


def implementation_array_of_structures(package):
    base = base_type(package)
    package.create_implementation_data_type(ImplementationDataTypeSettings.Array(
        "ImplArray", length=10,
        element_type=ImplementationDataTypeSettings.Structure("ImplStruct", elements=[
            ImplementationDataTypeSettings.Value("ImplValue", base_type=base),
        ]),
    ))


def implementation_composite_aliases(package):
    base = base_type(package)
    array = package.create_implementation_data_type(ImplementationDataTypeSettings.Array(
        "ImplArray", length=10,
        element_type=ImplementationDataTypeSettings.Value("ImplValue", base_type=base),
    ))
    structure = package.create_implementation_data_type(ImplementationDataTypeSettings.Structure(
        "ImplStruct",
        elements=[ImplementationDataTypeSettings.Value("ImplValue", base_type=base)],
    ))
    for name, target in [("ImplArrayRef", array), ("ImplStructRef", structure)]:
        package.create_implementation_data_type(ImplementationDataTypeSettings.TypeReference(
            name, reftype=target,
        ))


SPECS = [
    ("upstream_impl_array", implementation_array, "implementation_data_type_test.py",
     "test_implmentation_data_type_array", []),
    ("upstream_impl_structure", implementation_structure, "implementation_data_type_test.py",
     "test_implmentation_data_type_structure", []),
    ("upstream_impl_type_reference", implementation_reference, "implementation_data_type_test.py",
     "test_implmentation_data_type_type_reference", []),
    ("upstream_application_mapping", application_mapping, "datatype_test.py",
     "test_data_type_mapping_set", []),
    ("upstream_application_arrays", application_arrays, "datatype_test.py",
     "test_application_array_data_type", [
         "Add signed 32-bit SW base, implementation VALUE, and application mapping.",
         "Add an explicit empty ELEMENTS container to the empty record.",
     ]),
    ("upstream_application_record", application_record, "datatype_test.py",
     "test_application_record_data_type", [
         "Add signed 32-bit SW base, implementation VALUE, and application mapping.",
         "Add an explicit empty ELEMENTS container to the nested empty record.",
     ]),
]


BATCH2_SPECS = [
    ("upstream_application_array_variable", application_array_variable, "datatype_test.py",
     "test_application_array_size", [
         "Select the VariableLinear(20) state and omit later size-mode mutations.",
         "Add signed 32-bit SW base, implementation VALUE, and application mapping.",
     ]),
    ("upstream_impl_mixed_members", implementation_mixed_members, "implementation_data_type_test.py",
     "test_implmentation_data_type_element", [
         "Retain only VALUE, STRUCTURE, ARRAY, and TYPE_REFERENCE members in source order.",
         "Omit UNION, DATA_REFERENCE, and FUNCTION_REFERENCE members.",
     ]),
    ("upstream_impl_array_of_structures", implementation_array_of_structures,
     "implementation_data_type_test.py", "test_implmentation_data_type_array", [
         "Replace the inline VALUE element with the STRUCTURE pattern from test_implmentation_data_type_structure.",
     ]),
    ("upstream_impl_composite_aliases", implementation_composite_aliases,
     "implementation_data_type_test.py", "test_implmentation_data_type_type_reference", [
         "Use the ARRAY and STRUCTURE patterns from test_implmentation_data_type_array and test_implmentation_data_type_structure as alias targets.",
         "Name the aliases ImplArrayRef and ImplStructRef; omit optional compu method and constraint.",
     ]),
]


def generate(directory, check, specs, provenance_name):
    version = importlib.metadata.version("autosar-data")
    if version != PINNED_VERSION:
        raise RuntimeError(f"expected autosar-data {PINNED_VERSION}, installed {version}")
    outputs = {}
    provenance = {"version": 1, "upstream": UPSTREAM, "commit": COMMIT,
                  "autosar_data": version, "license": "UPSTREAM-LICENSE-MIT", "fixtures": []}
    for name, construct, source, function, adaptations in specs:
        filename = f"{name}.arxml"
        model, package = model_and_package(filename)
        construct(package)
        arxml = next(iter(model.files()))
        content = arxml.serialize().encode("utf-8")
        reference = AutosarModel()
        _, warnings = reference.load_buffer(content.decode("utf-8"), filename, True)
        if warnings:
            raise ValueError(f"strict reload warnings for {filename}: {warnings}")
        if reference.check_references():
            raise ValueError(f"unresolved references in {filename}")
        outputs[filename] = content
        provenance["fixtures"].append({
            "file": filename, "schema_version": str(arxml.version),
            "sha256": hashlib.sha256(content).hexdigest(),
            "source": f"{UPSTREAM}/blob/{COMMIT}/test/abstraction/{source}",
            "function": function,
            "state": "Constructed definitions before name/settings mutations; omit API round-trip assertions.",
            "adaptations": adaptations,
        })
    outputs[provenance_name] = (
        json.dumps(provenance, indent=2) + "\n"
    ).encode("utf-8")
    for filename, content in outputs.items():
        path = directory / filename
        if check:
            if not path.exists() or path.read_bytes() != content:
                raise ValueError(f"generated fixture differs: {path}")
        else:
            path.write_bytes(content)
        print(f"{'CHECKED' if check else 'WROTE'} {filename}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="verify committed bytes without writing")
    arguments = parser.parse_args()
    directory = Path(__file__).resolve().parent / "fixtures"
    for specs, provenance_name in [
        (SPECS, "upstream_batch1_provenance.json"),
        (BATCH2_SPECS, "upstream_batch2_provenance.json"),
    ]:
        generate(directory, arguments.check, specs, provenance_name)
