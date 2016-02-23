import logging
import os
from os import path as osp
import shutil
import threading
from urllib.request import urlretrieve, urlopen
import urllib
from glob import glob

from tools import adjusted_socket_timeout, FilenameOps, HeadRequest

class ImportsCollector(object):
    log = logging.getLogger('dld.DatasetImportCollector')

    def __init__(self, dld_config):
        """
        :param dld_config: DLDConfig instance
        """
        self.memory = DatasetMemory()
        self.dld_config = dld_config

    def prepare(self, datasets_config_fragment):
        self._write_default_graph_name()
        for dataset_name, dataset_config in datasets_config_fragment.items():
            keys = frozenset(dataset_config.keys())
            source_spec_keywords = keys.intersection(DATASET_SPEC_FACTORY_BY_KEYWORD.keys())
            if len(source_spec_keywords) is not 1:
                msg_tmpl = "None or several data source specifications ({opts} keys) defined for dataset:\n{ds}"
                raise RuntimeError(msg_tmpl.format(ds=dataset_config,
                                                   opts=" or ".join(DATASET_SPEC_FACTORY_BY_KEYWORD.keys())))

            spec_keyword = next(iter(source_spec_keywords))
            graph_name = dataset_config.get('graph_name')  # might be None
            factory = DATASET_SPEC_FACTORY_BY_KEYWORD[spec_keyword]
            source_spec = dataset_config[spec_keyword]
            dataset_spec = factory(source_spec, self.dld_config, self.memory, graph_name)
            dataset_spec.add_to_import_data()
        self._prune_target_directory()

    def _write_default_graph_name(self):
        if self.dld_config.default_graph_name:
            with open(osp.join(self.dld_config.models_dir, "global.graph"), "w") as graph_file:
                graph_file.write(self.dld_config.default_graph_name + "\n")

    def _prune_target_directory(self):
        for dircontent in glob(osp.join(self.dld_config.models_dir, '*')):
            if osp.isdir(dircontent):  # dld.py does not create subdirectories of the models directory
                os.removedirs(dircontent)
            elif osp.isfile(dircontent) and not dircontent.endswith('.graph'):
                basename = FilenameOps.basename(dircontent)
                stripped_ds_basename = FilenameOps.strip_ld_and_compession_extensions(basename)

                if not self.memory.was_added_or_retained(stripped_ds_basename):
                    self.log.debug("removing extraneous import data file: {f}".format(f=dircontent))
                    os.remove(dircontent)
                    # delete corresponding graph file, if it exists
                    graph_file = osp.join(self.dld_config.models_dir, FilenameOps.graph_file_name(basename))
                    if osp.isfile(graph_file):
                        os.remove(graph_file)


class DatasetMemory(object):
    """
    Remembers the files that were added to or retained in the models dir.
    """
    log = logging.getLogger('dld.DatasetMemory')

    def __init__(self):
        self._lock = threading.RLock()
        self._added = set()
        self._retained = set()
        self._adding = set()

    def added_file(self, stripped_basename):
        with self._lock:
            self._added.add(stripped_basename)

    def retained_file(self, stripped_basename):
        with self._lock:
            self._retained.add(stripped_basename)

    def was_added(self, stripped_basename):
        with self._lock:
            return stripped_basename in self._added

    def was_retained(self, stripped_basename):
        with self._lock:
            return stripped_basename in self._retained

    def was_added_or_retained(self, stripped_basename):
        with self._lock:
            return any((stripped_basename in s) for s in (self._added, self._retained))

    def adding_token(self, stripped_basename):
        """
        Creates a context object, trying to obtain a lock for adding the named dataset.
        The Lock will be released when the context is left and an exception will be thrown
        then attempting to enter the context then an adding token was already given away
        for the named dataset an not yet returned."""

        outer_self = self

        class AddingDatasetToken(object):
            def __enter__(self):
                with outer_self._lock:
                    if stripped_basename in outer_self._adding:
                        raise DatasetAlreadyBeingAddedError("dataset {ds} is already being added")
                    else:
                        outer_self._adding.add(stripped_basename)

            def __exit__(self, *args):
                with outer_self._lock:
                    outer_self._adding.remove(stripped_basename)

        return AddingDatasetToken()


class DatasetAlreadyBeingAddedError(RuntimeError):
    def __init__(self, *args, **kwargs):
        RuntimeError.__init__(*args, **kwargs)


class AbstractDatasetSpec(object):
    def __init__(self, source, dld_config, dataset_memory, graph_name=None):
        """
        ""
        :param source: string describing the source
        :param dld_config: a DLDConfig instance
        :param dataset_memory: the DatasetMemory used by DatasetImportPreparator
        :param graph_name: target named graph
        :return:
        """

        self.source = source
        self.config = dld_config
        self.memory = dataset_memory
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
        return osp.join(self.config.models_dir, self.basename)

    @property
    def graph_file_path(self):
        return osp.join(self.config.models_dir, FilenameOps.graph_file_name(self.basename))

    def add_to_import_data(self):
        def duplicate_error():
            msg_tmpl = "duplicate source '{src}' (stripped: '{str}')"
            error = RuntimeError(msg_tmpl.format(src=self.source, str=self.stripped_basename))
            self.log.error(error)
            self._set_skip()

        if self.memory.was_added_or_retained(self.stripped_basename):
            duplicate_error()

        if not self.skip:
            try:
                with self.memory.adding_token(self.stripped_basename):
                    # TODO: catch errors and delete dataset and target graph files on error to clean up
                    self._ensure_copy()
                    self._ensure_graph_file()

            except DatasetAlreadyBeingAddedError as dabae:
                self.log.error(dabae)
                self._set_skip()

    def _ensure_graph_file(self):
        if not any((self.graph_name, self.config.default_graph_name)):
            raise RuntimeError("No destination graph name defined for {bn}".format(bn=self.basename))

        # write a graph file if destination graph name differs from default destination graph name
        if self.graph_name and (self.graph_name != self.config.default_graph_name):
            with open(self.graph_file_path, "w") as graph_fd:
                graph_fd.write(self.graph_name + "\n")

        # check if a previously written graph file is outdated or no longer required
        if ((not self.graph_name) or (self.graph_name == self.config.default_graph_name)) and \
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


class FileDatasetSpec(AbstractDatasetSpec):
    def __init__(self, source_path, dld_config, dataset_memory, graph_name=None):
        AbstractDatasetSpec.__init__(self, source_path, dld_config, dataset_memory, graph_name)

    @property
    def source_path(self):
        return self.source

    def _ensure_copy(self):
        if osp.isfile(self.target_path) and osp.getsize(self.source_path) == osp.getsize(self.target_path):
            self.log.info("{tp} appears to be identical to {sp} - skipping copy"
                          .format(sp=self.source_path, tp=self.target_path))
            self.memory.retained_file(self.stripped_basename)
        else:
            self.log.debug("starting copying for: {src}".format(src=self.source_path))
            shutil.copyfile(self.source_path, self.target_path)
            self.log.debug("finished copying for: {src}".format(src=self.source_path))
            self.memory.added_file(self.stripped_basename)

    def _extract_basename(self):
        return FilenameOps.basename(self.source)


class HTTPLocationDatasetSpec(AbstractDatasetSpec):
    def __init__(self, source_location, dld_config, dataset_memory, graph_name=None):
        AbstractDatasetSpec.__init__(self, source_location, dld_config, dataset_memory, graph_name)

    @property
    def source_location(self):
        return self.source

    def _ensure_copy(self):
        def get_content_size():
            try:
                response = urllib.request.urlopen(HeadRequest(self.source_location), timeout=60)
                length_str = response.headers.getheader('content-length')
                return (length_str is not None) and int(length_str) or None
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
            self.memory.retained_file(self.stripped_basename)
        else:
            self.memory.added_file(self.stripped_basename)
            # this (hopefully) sets a sensible 1 minute default for connection inactivity
            with adjusted_socket_timeout(60):
                self.log.info("starting download: {u}".format(u=self.source_location))
                urlretrieve(self.source_location, self.target_path)
                self.log.info("download finished: {u}".format(u=self.source_location))

    def _extract_basename(self):
        parsed_url = urllib.parse.urlparse(self.source)
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
                    line.strip() and self.atomic_spec_factory(line.strip()).add_to_import_data()
        except IOError as ex:
            raise RuntimeError('Unable to open source specification list at {p} due to: {ex}' \
                               .format(p=self.source, ex=ex))

    def atomic_spec_factory(self, source_description):
        return None


class FileListDatasetSpec(AbstractDatasetSpec, SourceListMixin):
    def __init__(self, source, dld_config, dataset_memory, graph_name=None):
        AbstractDatasetSpec.__init__(self, source, dld_config, dataset_memory, graph_name)

    def add_to_import_data(self):
        self.handle_list()

    def atomic_spec_factory(self, source_description):
        return FileDatasetSpec(source_description, self.config, self.memory, self.graph_name)


class HTTPLocationListDatasetSpec(AbstractDatasetSpec, SourceListMixin):
    def __init__(self,  source, dld_config, dataset_memory, graph_name=None):
        AbstractDatasetSpec.__init__(self, source, dld_config, dataset_memory, graph_name)

    def add_to_import_data(self):
        self.handle_list()

    def atomic_spec_factory(self, source_description):
        return HTTPLocationDatasetSpec(source_description, self.config, self.memory, self.graph_name)

DATASET_SPEC_FACTORY_BY_KEYWORD = {
    'file': FileDatasetSpec,
    'location': HTTPLocationDatasetSpec,
    'file_list': FileListDatasetSpec,
    'location_list': HTTPLocationListDatasetSpec
}
