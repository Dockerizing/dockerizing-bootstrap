from __future__ import absolute_import, division, print_function, unicode_literals

from future.standard_library import install_aliases

install_aliases()
from builtins import *

import logging
import os
from os import path as osp
import shutil
import threading
from urllib import urlretrieve
import urllib2
import urlparse

from config import DLDConfig
from tools import adjusted_socket_timeout, FilenameOps, HeadRequest

class DatasetMemory(object):
    """
    Remembers the files that were added to or retained in the models dir.
    """
    lock = threading.RLock()
    _added = set()
    _retained = set()
    _adding = set()
    log = logging.getLogger('dld.DatasetMemory')

    @classmethod
    def added_file(cls, stripped_basename):
        with cls.lock:
            cls._added.add(stripped_basename)

    @classmethod
    def retained_file(cls, stripped_basename):
        with cls.lock:
            cls._retained.add(stripped_basename)

    @classmethod
    def was_added(cls, stripped_basename):
        with cls.lock:
            return stripped_basename in cls._added

    @classmethod
    def was_retained(cls, stripped_basename):
        with cls.lock:
            return stripped_basename in cls._retained

    @classmethod
    def was_added_or_retained(cls, stripped_basename):
        with cls.lock:
            return any((stripped_basename in s) for s in (cls._added, cls._retained))

    @classmethod
    def adding_token(cls, stripped_basename):
        class AddingDatasetToken(object):
            def __enter__(self):
                with cls.lock:
                    if stripped_basename in cls._adding:
                        raise DatasetAlreadyBeingAddedError("dataset {ds} is already being added")
                    else:
                        cls._adding.add(stripped_basename)

            def __exit__(self, *args):
                cls._adding.remove(stripped_basename)

        return AddingDatasetToken()


class DatasetAlreadyBeingAddedError(RuntimeError):
    def __init__(self, *args, **kwargs):
        RuntimeError.__init__(*args, **kwargs)


class AbstractDatasetSpec(object):
    default_graph_name = None

    def __init__(self, source, graph_name=None):
        self.source = source
        self.graph_name = graph_name
        self.skip = False
        self.log = logging.getLogger('dld.' + self.__class__.__name__)

    @property
    def basename(self):
        return self._extract_basename()

    @property
    def stripped_basename(self):
        return FilenameOps.strip_ld_and_compession_extensions(self.basename)

    @property
    def target_path(self):
        return osp.join(DLDConfig.models_dir, self.basename)

    @property
    def graph_file_path(self):
        return osp.join(DLDConfig.models_dir, FilenameOps.graph_file_name(self.basename))

    def add_to_import_data(self):
        def duplicate_error():
            msg_tmpl = "duplicate source '{src}' (stripped: '{str}')"
            error = RuntimeError(msg_tmpl.format(src=self.source, str=self.stripped_basename))
            self.log.error(error)
            self._set_skip()

        if DatasetMemory.was_added_or_retained(self.stripped_basename):
            duplicate_error()

        if not self.skip:
            try:
                with DatasetMemory.adding_token(self.stripped_basename):
                    # TODO: catch errors and delete dataset and target graph files on error to clean up
                    self._ensure_copy()
                    self._ensure_graph_file()

            except DatasetAlreadyBeingAddedError as dabae:
                self.log.error(dabae)
                self._set_skip()

    def _ensure_graph_file(self):
        if not any((self.graph_name, DLDConfig.default_graph_name)):
            raise RuntimeError("No destination graph name defined for {bn}".format(bn=self.basename))

        # write a graph file if destination graph name differs from default destination graph name
        if self.graph_file_path and (self.graph_name != DLDConfig.default_graph_name):
            with open(self.graph_file_path, "w") as graph_fd:
                graph_fd.write(self.graph_name + "\n")

        # check if a previously written graph file is outdated or no longer required
        if ((not self.graph_name) or (self.graph_name == DLDConfig.default_graph_name)) and \
                osp.isfile(self.graph_file_path):
            os.remove(self.graph_file_path)

    def _ensure_copy(self):
        pass

    def _extract_basename(self):
        pass

    def _set_skip(self):
        if not self.skip:
            self.log.info("skipping dataset from: {src}".format(src=self.source))
        self.skip = True

    @staticmethod
    def _ensure_default_graph_name():
        if not isinstance(AbstractDatasetSpec.default_graph_name, (str, unicode)):
            raise RuntimeError("default graph name not set")


class FileDatasetSpec(AbstractDatasetSpec):
    def __init__(self, source_path, graph_name=None):
        AbstractDatasetSpec.__init__(self, source_path, graph_name)

    @property
    def source_path(self):
        return self.source

    def _ensure_copy(self):
        if osp.isfile(self.target_path) and osp.getsize(self.source_path) == osp.getsize(self.target_path):
            self.log.info("{tp} appears to be identical to {sp} - skipping copy"
                          .format(sp=self.source_path, tp=self.target_path))
            DatasetMemory.retained_file(self.stripped_basename)
        else:
            shutil.copyfile(self.source_path, self.target_path)
            DatasetMemory.added_file(self.stripped_basename)

    def _extract_basename(self):
        return FilenameOps.basename(self.source)


class HTTPLocationDatesetSpec(AbstractDatasetSpec):
    def __init__(self, source_location, graph_name=None):
        AbstractDatasetSpec.__init__(self, source_location, graph_name)

    @property
    def source_location(self):
        return self.source

    def _ensure_copy(self):
        def get_content_size():
            try:
                response = urllib2.urlopen(HeadRequest(self.source_location), timeout=60)
                length_str = response.headers.getheader('content-length')
                return (length_str is not None) and long(length_str) or None
            except:
                self.log.exception("error getting HEAD for {u}".format(u=self.source_location))
                return None

        skip_download = False
        if osp.isfile(self.target_path):
            if get_content_size() == osp.getsize(self.target_path):
                self.log.info("{tp} seems to be complete download of {u} -- skipping (re-)download"
                              .format(tp=self.target_path, u=self.source_location))
                skip_download = True

        if skip_download:
            DatasetMemory.retained_file(self.stripped_basename)
        else:
            DatasetMemory.added_file(self.stripped_basename)
            # this (hopefully) sets a sensible 1 minute default for connection inactivity
            with adjusted_socket_timeout(60):
                self.log.info("starting download: {u}".format(u=self.source_location))
                urlretrieve(self.source_location, self.target_path)
                self.log.info("download finished: {u}".format(u=self.source_location))

    def _extract_basename(self):
        parsed_url = urlparse.urlparse(self.source)
        if parsed_url.scheme not in ['http', 'https']:
            self.skip = True
            error = RuntimeError("location does not appear to be a http(s)-URL:\n{loc}".format(loc=self.source))
            self.log.error(error)
            self._set_skip()
            return

        basename = parsed_url.path and parsed_url.path.split('/')[-1] or parsed_url.netloc
        return basename

class SourceListMixin(object):
    def handle_list(self):
        """
        :param handler: callable with (basename, graph_name) parameters
        """
        try:
            with open(self.source) as src:
                for line in src:
                    line.strip() and self.atomic_spec_factory(line).add_to_import_data()
        except IOError as ex:
            raise RuntimeError('Unable to open source specification list at {p} due to: {ex}' \
                               .format(p=self.source, ex=ex))

    def atomic_spec_factory(self, source_description):
        return None


class FileListDatasetSpec(AbstractDatasetSpec, SourceListMixin):
    def __init__(self, source, graph_name=None):
        AbstractDatasetSpec.__init__(self, source, graph_name)

    def add_to_import_data(self):
        self.handle_list()

    def atomic_spec_factory(self, source_description):
        return FileDatasetSpec(source_description, self.graph_name)


class HTTPLocationListDatasetSpec(AbstractDatasetSpec, SourceListMixin):
    def __init__(self, source, graph_name=None):
        AbstractDatasetSpec.__init__(self, source, graph_name)

    def add_to_import_data(self):
        self.handle_list()

    def atomic_spec_factory(self, source_description):
        return HTTPLocationDatesetSpec(source_description, self.graph_name)

DATASET_SPEC_TYPE_BY_KEYWORD = {
    'file': FileDatasetSpec,
    'location': HTTPLocationDatesetSpec,
    'file_list': FileListDatasetSpec,
    'location_list': HTTPLocationListDatasetSpec
}
