#! /usr/bin/env python

from __future__ import print_function, division
import sys
import os
from os import path as osp
from glob import glob
import getopt
import re
import logging
import logging.config
from collections import defaultdict
from textwrap import dedent

try:
    import urlparse
except ImportError:
    from urllib import parse as urlparse
import tempfile

import yaml
import httplib2
from docker import Client

if __name__ != '__main__':
    from dldbase import DEV_MODE

from data import datasets
from config import DLDConfig
from tools import FilenameOps, ComposeConfigDefaultDict, HeadRequest, alpha_gen

LAST_WORD_PATTERN = re.compile('[a-zA-Z0-9]+$')

PROJECT_DIR = osp.dirname(osp.realpath(__file__))
DLD_LOG = logging.getLogger('dld')

py2list = list
#py2list = list._builtin_list

# TODO: check if we can use copy.deepcopy instead
def ddict2dict(d):
    for k, v in d.items():
        if is_dict_like(v):
            d[k] = ddict2dict(v)
        elif is_list_like(v):
            d[k] = py2list(v)
    return dict(d)


def ensure_dir_exists(dir, log):
    if not os.path.exists(dir):
        os.makedirs(dir)
    else:
        DEV_MODE and log.warning("The given path '{d}' already exists.".format(d=dir))


DICT_LIKE_ATTRIBUTES = ('keys', 'iterkeys', 'get', 'update')
LIST_LIKE_ATTRIBUTES = ('insert', 'reverse', 'sort', 'pop')

def _signature_testing(obj, expected_attributes):
    attr_checks = map(lambda attr: hasattr(obj, attr), expected_attributes)
    return all(attr_checks)


def is_dict_like(obj):
    return _signature_testing(obj, DICT_LIKE_ATTRIBUTES)

def is_list_like(obj):
    return _signature_testing(obj, LIST_LIKE_ATTRIBUTES)

class ComposeConfigGenerator(object):
    def __init__(self, configuration, working_directory):
        self.configuration = configuration
        self.working_directory = working_directory
        self.log = logging.getLogger('dld.' + self.__class__.__name__)
        self.compose_config = ComposeConfigDefaultDict()
        self._steps_done = defaultdict(lambda: False)
        self.log.debug("init - passed configuration:\n{}".format(self.configuration))
        ensure_dir_exists(self.working_directory, self.log)
        DLDConfig.models_dir = osp.join(self.working_directory, 'models')
        ensure_dir_exists(DLDConfig.models_dir, self.log)

    def run(self):
        # self.pull_images(self.configuration) #not using image metadata yet
        self.create_compose_config()
        self.prepare_import_data(self.configuration["datasets"])

    def pull_images(self, config):
        docker = Client()
        images = docker.images(filters={"label": "org.aksw.dld"})

        # docker.inspect_image

    def _add_global_settings(self, component_settings):
        global_settings = self.configuration.get('settings', dict())
        component_settings = is_dict_like(component_settings) and component_settings.copy() or dict()
        component_settings.update(global_settings)
        return component_settings

    @classmethod
    def _generic_settings_handler(cls, component_settings, compose_container_spec):
        if 'default_graph' in component_settings:
            if 'environment' not in compose_container_spec:
                compose_container_spec['environment'] = dict()
            compose_container_spec['environment']['DEFAULT_GRAPH'] = component_settings['default_graph']

    def _configure_singleton_component(self, component_name, settings_handler=None, additional_config_thunk=None):
        if component_name not in self.configuration['components']:
            return
        component_config = self.configuration['components'][component_name]
        compose_container_spec = self.compose_config[component_name]

        self._update_container_config(component_config, compose_container_spec,
                                      settings_handler, additional_config_thunk)

    @staticmethod
    def _extract_component_image(component_config):
        if is_dict_like(component_config):
            try:
                return component_config['image']
            except KeyError:
                raise RuntimeError("missing image declaration for component:\n{}".format(component_config))
        elif isinstance(component_config, str):
            return component_config

    @staticmethod
    def _valid_dataset_source_spec_keys():
        return frozenset(['file', 'file_list', 'location', 'location_list'])

    def _update_container_config(self, component_config, compose_container_spec,
                                 settings_handler=None, additional_config_thunk=None):

        compose_container_spec['image'] = self._extract_component_image(component_config)

        if is_dict_like(component_config):
            try:
                component_config = component_config.copy()  # copy to keep input config unaltered
                component_settings = component_config.pop('settings')
            except KeyError:
                component_settings = None
            compose_container_spec.update(component_config)
        elif isinstance(component_config, str):
            component_settings = None
        else:
            msg_templ = "don't know how to merge component config of type {t}:\n{conf}"
            raise RuntimeError(msg_templ.format(t=type(component_config), conf=component_config))
        merged_settings = self._add_global_settings(component_settings)
        self._generic_settings_handler(merged_settings, compose_container_spec)
        if callable(settings_handler):
            settings_handler(merged_settings, compose_container_spec)
        if callable(additional_config_thunk):
            additional_config_thunk(compose_container_spec)

    def create_compose_config(self):
        self.configure_compose()
        # transformation back to standard dict required to keep pyyaml from serialising class metadata
        docker_compose_config = ddict2dict(self.compose_config)
        DLD_LOG.debug(yaml.dump(docker_compose_config))
        with open(osp.join(self.working_directory, 'docker-compose.yml'), mode='w') as compose_fd:
            yaml.dump(docker_compose_config, compose_fd)

    def configure_compose(self):
        self.configure_store()
        self.configure_load()
        self.configure_present()

    @property
    def wd_ready_message(self):
        msg_tmpl = dedent(""" \
        Your docker-compose configuration and import data has been set up at '{wd}'.
        Now change into that directory and invoke `docker-compose up -d` to start  the components
        (possibly triggering also the data import process)

        `docker-compose ps` will give you an overview for the component containers and tell you
        which host ports to use to reach your components.

        `docker-compose logs` will allow you to inspect the state of the setup processes.

        Have fun!""")
        abs_wd = osp.realpath(self.working_directory)
        if DEV_MODE:
            return "set up completed at {wd}".format(wd=abs_wd)
        else:
            return msg_tmpl.format(wd=abs_wd)

    def configure_store(self):
        if not self._steps_done['store']:
            self._configure_singleton_component('store')
            self._steps_done['store'] = True

    def configure_load(self):
        def additional_config(compose_container_spec):
            compose_container_spec['links'].append('store')
            # TODO This might also be done by reading the labels of the load container resp. for the other categories.
            # A stub for loding the images and reading the labels is in pull_images
            compose_container_spec['volumes_from'].append('store')
            compose_container_spec['volumes'] = [osp.abspath(DLDConfig.models_dir) + ":/import"]

        if not self._steps_done['load']:
            self._configure_singleton_component('load', additional_config_thunk=additional_config)
            self._steps_done['load'] = True

    def _extract_last_word(str, fallback=None):
        try:
            return next(LAST_WORD_PATTERN.finditer(str)).group(0)
        except StopIteration:
            if isinstance(fallback, str):
                return fallback
            raise RuntimeError("unable to find last word")

    def _choose_present_container_name(self, image_name):
        try:
            return "present" + next(LAST_WORD_PATTERN.finditer(image_name)).group(0)
        except StopIteration:  # unable to find proper image suffix
            try:  # lazy init for __present_suffix_gen
                self.__present_suffix_gen
            except AttributeError:
                self.__present_suffix_gen = alpha_gen()
            return "present" + next(self.__present_suffix_gen)

    def configure_present(self):
        def additional_config(compose_container_spec):
            compose_container_spec['links'].append('store')

        if ('present' not in self.configuration["components"]) or self._steps_done['present']:
            return
        present_component_group = self.configuration["components"]['present']
        if is_dict_like(present_component_group):
            for comp_name, comp_config in present_component_group.items():
                compose_name = 'present' + comp_name
                compose_container_spec = self.compose_config.get(comp_name, ComposeConfigDefaultDict())
                self.compose_config[compose_name] = compose_container_spec
                self._update_container_config(comp_config, compose_container_spec,
                                              additional_config_thunk=additional_config)
        elif isinstance(present_component_group, list):
            for image_desc in present_component_group:
                if isinstance(image_desc, str):
                    image_name = image_desc
                elif is_dict_like(image_desc):
                    try:
                        image_name = image_desc['image']
                    except KeyError:
                        raise RuntimeError('No image defined in component specification:\n{}'.format(image_desc))
                else:
                    raise RuntimeError('Unexpected type for component description')

                compose_name = self._choose_present_container_name(image_name)
                compose_container_spec = self.compose_config.get(compose_name, ComposeConfigDefaultDict())
                self.compose_config[compose_name] = compose_container_spec
                self._update_container_config(image_desc, compose_container_spec,
                                              additional_config_thunk=additional_config)
        self._steps_done['present'] = True

    @classmethod
    def _http_client(cls, **kwargs):
        # TODO: set up proper HTTP-caching directory and test caching facilities
        cache_dir = osp.join(tempfile.gettempdir(), '.dld-http-caching')
        return httplib2.Http(cache=cache_dir, **kwargs)

    def prepare_import_data(self, datasets_fragment):
        # self#configure_store must have been run before this method
        if not self._steps_done['store']:
            raise RuntimeError('[internal] cannot prepare import data before store configuration')

        if DLDConfig.default_graph_name:
            with open(osp.join(DLDConfig.models_dir, "global.graph"), "w") as graph_file:
                graph_file.write(DLDConfig.default_graph_name + "\n")

        for dataset_name, dataset_config in datasets_fragment.items():
            keys = frozenset(dataset_config.keys())
            source_spec_keywords = keys.intersection(datasets.DATASET_SPEC_TYPE_BY_KEYWORD.keys())
            if len(source_spec_keywords) is not 1:
                msg_tmpl = "None or several data source specifications ({opts} keys) defined for dataset:\n{ds}"
                raise RuntimeError(msg_tmpl.format(ds=dataset_config,
                                                   opts=" or ".join(self._valid_dataset_source_spec_keys())))

            spec_keyword = next(iter(source_spec_keywords))
            graph_name = dataset_config.get('graph_name')  # might be None
            factory = datasets.DATASET_SPEC_TYPE_BY_KEYWORD[spec_keyword]
            dataset_spec = factory(dataset_config[spec_keyword], graph_name)
            dataset_spec.add_to_import_data()

        # finally, remove dump files in the models dir that were there before but were not
        # requested by the config
        for dircontent in glob(osp.join(DLDConfig.models_dir, '*')):
            if osp.isdir(dircontent):  # dld.py does not create subdirectories of the models directory
                os.removedirs(dircontent)
            elif osp.isfile(dircontent) and not dircontent.endswith('.graph'):
                basename = FilenameOps.basename(dircontent)
                stripped_ds_basename = FilenameOps.strip_ld_and_compession_extensions(basename)

                if not datasets.DatasetMemory.was_added_or_retained(stripped_ds_basename):
                    DLD_LOG.debug("removing extranous import data file: {f}".format(f=dircontent))
                    os.remove(dircontent)
                    # delete corresponding graph file, if it exists
                    graph_file = osp.join(DLDConfig.models_dir, FilenameOps.graph_file_name(basename))
                    if osp.isfile(graph_file):
                        os.remove(graph_file)


def usage():
    DLD_LOG.warning("please read at http://dld.aksw.org/ for further instructions")


def main(args=sys.argv[1:]):
    try:
        opts, args = getopt.getopt(args, "hc:w:u:f:l:",
                                   ["help", "config=", "workingdirectory=", "uri=", "file=", "location="])
    except getopt.GetoptError as err:
        # print help information and exit:
        DLD_LOG.error(err)  # will print something like "option -a not recognized"
        usage()
        sys.exit(2)

    config_file = "dld.yml"
    wd_from_cli = None
    uri_from_cli = None
    location_from_cli = None
    file_from_cli = None

    for opt, opt_val in opts:
        if opt in ("-h", "--help"):
            usage()
            sys.exit()
        elif opt in ("-c", "--config"):
            config_file = opt_val
        elif opt in ("-w", "--workingdirectory"):
            wd_from_cli = opt_val
        elif opt in ("-u", "--uri"):
            uri_from_cli = opt_val
        elif opt in ("-f", "--file"):
            file_from_cli = opt_val
        elif opt in ("-l", "--location"):
            location_from_cli = opt_val
        else:
            assert False, "unhandled option"
    # read configuration file

    if not wd_from_cli:
        wd_from_cli = 'wd-' + FilenameOps.strip_config_suffixes(osp.basename(config_file))

    with open(config_file, 'r') as config_fd:
        user_config = yaml.load(config_fd)

    # Add command line arguments to configuration
    if any((uri_from_cli, file_from_cli, location_from_cli)):
        if uri_from_cli and (file_from_cli or location_from_cli):
            if "datasets" not in user_config:
                user_config["datasets"] = {}
            if "settings" not in user_config:
                user_config["settings"] = {}
            if "cli" not in user_config["datasets"]:
                user_config["datasets"]["cli"] = {}
            else:
                raise RuntimeError("Reserved dataset key 'cli' used in configuration file")

            user_config["datasets"]["cli"]["graph_name"] = uri_from_cli
            if file_from_cli:
                user_config["datasets"]["cli"]["file"] = file_from_cli
            elif location_from_cli:
                user_config["datasets"]["cli"]["location"] = location_from_cli
        else:
            DLD_LOG.error("only the combinations uri and file or uri and location are permitted")
            usage()
            sys.exit(2)

    if is_dict_like(user_config.get("settings")):
        DLDConfig.default_graph_name = user_config["settings"].get("default_graph")
    if uri_from_cli:
        DLDConfig.default_graph_name = uri_from_cli

    if "datasets" not in user_config or "components" not in user_config:
        DLD_LOG.error("dataset and component configuration is needed")
        usage()
        sys.exit(2)

    # start dld process
    datasets.DatasetMemory.reset()  # TODO make this obsolete by embedding a DatasetMemory instance per config process
    configurator = ComposeConfigGenerator(user_config, wd_from_cli)
    configurator.run()
    DLD_LOG.info(configurator.wd_ready_message)


if __name__ == "__main__":
    import sys

    sys.path.append(osp.join(PROJECT_DIR, 'baselibs', 'python'))
    if os.getcwd() != PROJECT_DIR:
        sys.path.append(PROJECT_DIR)

    from dldbase.logging import logging_init
    from dldbase import DEV_MODE

    logging_init(osp.join(PROJECT_DIR, 'logs'))
    main()
