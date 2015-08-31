from __future__ import absolute_import, division, print_function, unicode_literals

from future.standard_library import install_aliases

install_aliases()
from builtins import *

from os import path as osp
import re
import socket
import urllib2
import urlparse

# taken from a SO answer by Paul Manta
# (http://stackoverflow.com/questions/31875/is-there-a-simple-elegant-way-to-define-singletons-in-python)
class Singleton:
    """
    A non-thread-safe helper class to ease implementing singletons.
    This should be used as a decorator -- not a metaclass -- to the
    class that should be a singleton.

    The decorated class can define one `__init__` function that
    takes only the `self` argument. Other than that, there are
    no restrictions that apply to the decorated class.

    To get the singleton instance, use the `Instance` method. Trying
    to use `__call__` will result in a `TypeError` being raised.

    Limitations: The decorated class cannot be inherited from.

    """

    def __init__(self, decorated):
        self._decorated = decorated

    def instance(self):
        """
        Returns the singleton instance. Upon its first call, it creates a
        new instance of the decorated class and calls its `__init__` method.
        On all subsequent calls, the already created instance is returned.

        """
        try:
            return self._instance
        except AttributeError:
            self._instance = self._decorated()
            return self._instance

    def __call__(self):
        raise TypeError('Singletons must be accessed through `Instance()`.')

    def __instancecheck__(self, inst):
        return isinstance(inst, self._decorated)


ARCHIVE_SUFFIX_PATTERN = re.compile('^(.*?)(\.(?:(?:bz2)|(?:gz)))$')
RDF_SERIALISAION_PATTERN = re.compile(
    '^(.*?)(\.(?:(?:nt)|(?:ttl)|(?:nq)|(?:rdf)|(?:owl)|(?:jsonld)|(?:json)|(?:xml)))$')
YAML_FILETYPE_PATTERN = re.compile('^(.*?)(\.(?:(?:yml)|(?:yaml)))$')


class FilenameOps(object):
    @staticmethod
    def strip_ld_and_compession_extensions(filename):
        res = filename
        res = FilenameOps.__strip_when_match(ARCHIVE_SUFFIX_PATTERN, res)
        res = FilenameOps.__strip_when_match(RDF_SERIALISAION_PATTERN, res)
        return res

    @staticmethod
    def strip_compression_extensions(filename):
        return FilenameOps.__strip_when_match(ARCHIVE_SUFFIX_PATTERN, filename)

    @staticmethod
    def graph_file_name(filename):
        return FilenameOps.strip_compression_extensions(filename) + ".graph"

    @staticmethod
    def basename(filepath):
        _, basename = osp.split(filepath)
        return basename

    @staticmethod
    def strip_config_suffixes(filename):
        res = filename
        res = FilenameOps.__strip_when_match(YAML_FILETYPE_PATTERN, res)
        if res.endswith('-dld'):
            res = res[:-4]
        return res

    @staticmethod
    def __strip_when_match(pattern, string):
        match_attempt = pattern.match(string)
        return match_attempt and match_attempt.group(1) or string


class HeadRequest(urllib2.Request):
    def get_method(self):
        return "HEAD"


class ComposeConfigDefaultDict(dict):
    _list_keys = frozenset(['links', 'volumes', 'volumes_from', 'ports'])

    _dict_keys = frozenset(['environment'])

    _recurse_keys = frozenset(['store', 'load'])

    _recurse_prefixes = frozenset(['present'])

    @classmethod
    def _should_recurse(cls, key):
        if key in cls._recurse_keys:
            return True
        match_prefix = lambda prefix: key.startswith(prefix)
        return any(map(match_prefix, cls._recurse_prefixes))

    def __missing__(self, key):
        if key in self.__class__._list_keys:
            default_val = list()
        elif key in self.__class__._dict_keys:
            default_val = dict()
        elif self.__class__._should_recurse(key):
            default_val = ComposeConfigDefaultDict()
        else:
            raise RuntimeError("undefined default for key: " + key)
        self[key] = default_val
        return default_val


def alpha_gen(start='A', end='Z'):
    mem = start
    assert ord(start) < ord(end)
    yield mem
    while mem != end:
        mem = chr(ord(mem) + 1)
        yield mem


def adjusted_socket_timeout(timeout=60):
    class SockerTimeoutAdjustment(object):
        def __enter__(self):
            self.previous_timeout = socket.getdefaulttimeout()
            socket.setdefaulttimeout(timeout)

        def __exit__(self, *args):
            socket.setdefaulttimeout(self.previous_timeout)

    return SockerTimeoutAdjustment()

class LocationError(Exception):
    pass

def check_http_url(location_str):
    parsed_url = urlparse.urlparse(location_str)
    if parsed_url.scheme not in ('http', 'https'):
        msg = "location does not appear to be a http(s)-URL:\n{loc}".format(loc=location_str)
        raise LocationError(msg)

DICT_LIKE_ATTRIBUTES = ('keys', 'iterkeys', 'get', 'update')
LIST_LIKE_ATTRIBUTES = ('insert', 'reverse', 'sort', 'pop')

def _signature_testing(obj, expected_attributes):
    attr_checks = map(lambda attr: hasattr(obj, attr), expected_attributes)
    return all(attr_checks)

def is_dict_like(obj):
    return _signature_testing(obj, DICT_LIKE_ATTRIBUTES)


def is_list_like(obj):
    return _signature_testing(obj, LIST_LIKE_ATTRIBUTES)
