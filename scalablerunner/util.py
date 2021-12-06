import logging
import sys
from typing import Collection, Type
import collections.abc

from pygments.console import colorize

def debug(msg: str) -> None:
    return msg

def info(msg: str) -> None:
    return colorize('green', msg)

def warning(msg: str) -> None:
    return colorize('yellow', msg)
    
def error(msg: str) -> None:
    return colorize('red', msg)

def critical(msg: str) -> None:
    return colorize('brightcyan', msg)

class UtilLogger():
    # Log level
    NONE = -1
    DEBUG = 0
    INFO = 1
    WARNING = 2
    ERROR = 3
    CRITICAL = 4

    def __init__(self, module: str, submodule: str, verbose: int) -> None:
        self.module = module
        self.submodule = submodule
        self.verbose = verbose
        self.level_map = {
            self.DEBUG: logging.DEBUG,
            self.INFO: logging.INFO,
            self.WARNING: logging.WARNING,
            self.ERROR: logging.ERROR,
            self.CRITICAL: logging.CRITICAL
        }

        self.logger = logging.getLogger(f"{self.module}.{self.submodule}")
        # self.logger.setLevel(level=logging.CRITICAL)
        # self.logger.propagate = False
    
    def debug(self, msg: str) -> None:
        if msg is not None:
            if self.verbose <= self.DEBUG:
                print(f"[{self.submodule}] Debug: {info(msg)}")
            self.logger.debug(msg)

    def info(self, msg: str) -> None:
        if msg is not None:
            if self.verbose <= self.INFO:
                print(f"[{self.submodule}] Info: {info(msg)}")
            self.logger.info(msg)

    def warning(self, msg: str) -> None:
        if msg is not None:
            if self.verbose <= self.WARNING:
                print(f"[{self.submodule}] Warning: {warning(msg)}")
            self.logger.warning(msg)
        
    def error(self, msg: str) -> None:
        if msg is not None:
            if self.verbose <= self.ERROR:
                print(f"[{self.submodule}] Error: {error(msg)}")
            self.logger.error(msg)

    def critical(self, msg: str) -> None:
        if msg is not None:
            if self.verbose <= self.CRITICAL:
                print(f"[{self.submodule}] Critical: {error(msg)}")
            self.logger.critical(msg)

    def set_verbose(self, verbose: int):
        self.verbose = verbose
    
    def output_log(self, file_name: str, level: int=logging.INFO, filemode: str='a', format: str='%(asctime)s:%(msecs)d PID.%(process)d %(processName)s - %(name)s %(levelname)s %(message)s', datefmt: str='%Y-%m-%d %H:%M:%S'):
        logging.basicConfig(filename=file_name, 
                            filemode=filemode,
                            format=format,
                            datefmt=datefmt,
                            level=level)

def progress(filename, size, sent):
    sys.stdout.write("%s's progress: %.2f%%   \r" % (filename, float(sent)/float(size)*100))
    if float(sent)/float(size) >= 1:
        sys.stdout.write("\n")

def progress4(filename, size, sent, peername):
    sys.stdout.write("(%s:%s) %s's progress: %.2f%%   \r" % (peername[0], peername[1], filename, float(sent)/float(size)*100) )
    if float(sent)/float(size) >= 1:
        sys.stdout.write("\n")

def type_check(obj: object, obj_type: Type, obj_name: str, is_allow_none: bool) -> None:
    """
    Check type according to the given type

    :param object obj: The object need to be check
    :param Type obj_type: The type of the object
    :param str obj_name: The name of the object
    :param bool is_allow_none: Allow the obj is None or not
    """
    if obj_type is callable:
        if not callable(obj):
            if not is_allow_none:
                raise TypeError(f"{obj_name} should be a callable, but a {type(obj)}")
            else:
                if not (obj == None):
                    raise TypeError(f"{obj_name} should be a callable or None, but a {type(obj)}")
    else:
        if not isinstance(obj, obj_type):
            if not is_allow_none:
                raise TypeError(f"{obj_name} should be {obj_type}, but a {type(obj)}")
            else:
                if not (obj == None):
                    raise TypeError(f"{obj_name} should be {obj_type} or None, but a {type(obj)}")

def update(d: dict, u: dict) -> dict:
    for k, v in u.items():
        if isinstance(v, collections.abc.Mapping):
            d[k] = update(d.get(k, {}), v)
        else:
            d[k] = v
    return d

class BaseClass():
    def __init__(self) -> None:
        pass

    def _set_UtilLogger(self, module: str, submodule: str, verbose: int) -> UtilLogger:
        self.logger = UtilLogger(module=module, submodule=submodule, verbose=verbose)
        return self.logger

    def set_verbose(self, verbose: int) -> None:
        self.logger.set_verbose(verbose=verbose)
    
    def output_log(self, file_name: str, level: int=logging.INFO, filemode: str='a', format: str='%(asctime)s:%(msecs)d PID.%(process)d %(processName)s - %(name)s %(levelname)s %(message)s', datefmt: str='%Y-%m-%d %H:%M:%S'):
        self.logger.output_log(file_name=file_name, level=level, filemode=filemode, format=format, datefmt=datefmt)

    def _info(self, *args, **kwargs) -> None:
        self.logger.info(*args, **kwargs)

    def _warning(self, *args, **kwargs) -> None:
        self.logger.warning(*args, **kwargs)
        
    def _error(self, *args, **kwargs) -> None:
        self.logger.error(*args, **kwargs)

    def _type_check(self, *args, **kwargs) -> None:
        type_check(*args, **kwargs)

    def _not_implement_error(self, name: str) -> None:
        raise NotImplementedError(f"{name} isn't implemented yet.")