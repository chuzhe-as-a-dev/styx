"""
Visitor classes for the Styx transpiler.
"""

import libcst as cst
import libcst.matchers as m


class EntityDiscoveryVisitor(cst.CSTVisitor):
    """
    Discovers entity classes marked with @entity decorator.
    """

    def __init__(self):
        super().__init__()
        self.entities = {}
        self.entity_keys = {}  # Stores list of attribute names: {"Item": ["item_name"], "Stock": ["w_id", "i_id"]}
        self.entity_key_types = {}  # e.g. {"Item": "str"}
        self.entity_init_params = {}  # e.g. {"Item": {"item_name": "str", "price": "int"}}

    def visit_ClassDef(self, node: cst.ClassDef):
        for decorator in node.decorators:
            if m.matches(decorator, m.Decorator(decorator=m.Name("entity"))):
                class_name = node.name.value
                self.entities[class_name] = class_name.lower()

                for item in node.body.body:
                    # Find the __key__ method to identify the key field(s)
                    if isinstance(item, cst.FunctionDef) and item.name.value == "__key__":
                        # Record the return type of __key__ if explicitly annotated
                        if item.returns and isinstance(item.returns.annotation, cst.Name):
                            self.entity_key_types[class_name] = item.returns.annotation.value

                        for stmt in item.body.body:
                            if m.matches(stmt, m.SimpleStatementLine(body=[m.Return()])):
                                ret_val = stmt.body[0].value
                                if ret_val is None:
                                    continue

                                # Case 1: return self.attr
                                if m.matches(ret_val, m.Attribute(value=m.Name("self"), attr=m.Name())):
                                    self.entity_keys[class_name] = [ret_val.attr.value]

                                # Case 2: return (self.a, self.b, ...)
                                elif isinstance(ret_val, cst.Tuple):
                                    attrs = []
                                    is_valid = True
                                    for element in ret_val.elements:
                                        if m.matches(element.value, m.Attribute(value=m.Name("self"), attr=m.Name())):
                                            attrs.append(element.value.attr.value)
                                        else:
                                            is_valid = False
                                            break
                                    if is_valid and attrs:
                                        self.entity_keys[class_name] = attrs

                    # Find __init__ to record parameter names and types (excluding 'self')
                    if isinstance(item, cst.FunctionDef) and item.name.value == "__init__":
                        params = {}
                        for p in item.params.params:
                            if p.name.value != "self":
                                type_str = None
                                if p.annotation and isinstance(p.annotation.annotation, cst.Name):
                                    type_str = p.annotation.annotation.value
                                params[p.name.value] = type_str
                        self.entity_init_params[class_name] = params
