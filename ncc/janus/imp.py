# Mock imp module for python 3.12+ compatibility with old Waf versions
import sys
import types

def new_module(name):
    return types.ModuleType(name)

def get_tag():
    try:
        return sys.implementation.cache_tag
    except AttributeError:
        return "py3"
