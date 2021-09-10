import sys
from time import sleep
from typing import Collection, Type
from pygments.console import colorize

def delay():
    sleep(1)

def info(msg: str) -> None:
    return colorize('green', msg)

def warning(msg: str) -> None:
    return colorize('yellow', msg)
    
def error(msg: str) -> None:
    return colorize('red', msg)

def progress(filename, size, sent):
    sys.stdout.write("%s's progress: %.2f%%   \r" % (filename, float(sent)/float(size)*100))
    if float(sent)/float(size) >= 1:
        sys.stdout.write("\n")

def progress4(filename, size, sent, peername):
    sys.stdout.write("(%s:%s) %s's progress: %.2f%%   \r" % (peername[0], peername[1], filename, float(sent)/float(size)*100) )
    if float(sent)/float(size) >= 1:
        sys.stdout.write("\n")

def type_check(obj: object, obj_type: Type, obj_name: str, is_allow_none: bool) -> None:
    if obj_type is callable:
        if not callable(obj):
            if not is_allow_none:
                raise TypeError(f"Parameter '{obj_name}' should be a callable, but a {type(obj)}")
            else:
                if not (obj == None):
                    raise TypeError(f"Parameter '{obj_name}' should be a callable or None, but a {type(obj)}")
    else:
        if not isinstance(obj, obj_type):
            if not is_allow_none:
                raise TypeError(f"Parameter '{obj_name}' should be {obj_type}, but a {type(obj)}")
            else:
                if not (obj == None):
                    raise TypeError(f"Parameter '{obj_name}' should be {obj_type} or None, but a {type(obj)}")

def update(d: dict, u: dict) -> dict:
    for k, v in u.items():
        if isinstance(v, Collection.abc.Mapping):
            d[k] = update(d.get(k, {}), v)
        else:
            d[k] = v
    return d