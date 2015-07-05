#! /usr/bin/env python

from __future__ import print_function, division
import sys
import os
from os import path as osp
from glob import glob
import getopt
import shutil
import re
import logging

try:
    import urlparse
except ImportError:
    from urllib import parse as urlparse
import tempfile

import yaml
import httplib2
from docker import Client

LAST_WORD_PATTERN = re.compile('[a-zA-Z0-9]+$')
ARCHIVE_SUFFIX_PATTERN = re.compile('^(.*?)(\.(?:(?:bz2)|(?:gz)))$')
RDF_SERIALISAION_PATTERN = re.compile(
    '^(.*?)(\.(?:(?:nt)|(?:ttl)|(?:nq)|(?:rdf)|(?:owl)|(?:jsonld)|(?:json)|(?:xml)))$')
YAML_FILETYPE_PATTERN = re.compile('^(.*?)(\.(?:(?:yml)|(?:yaml)))$')


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


def ddict2dict(d):
    for k, v in d.items():
        if is_dict_like(v):
            d[k] = ddict2dict(v)
    return dict(d)


def ensure_dir_exists(dir, log):
    if not os.path.exists(dir):
        os.makedirs(dir)
    else:
        log.warning("The given path '{d}' already exists.".format(d=dir))


def is_dict_like(obj):
    return hasattr(obj, 'keys') and callable(obj.keys)


def alpha_gen():
    mem = 'A'
    yield mem
    while mem is not 'Z':
        mem = chr(ord(mem) + 1)
        yield mem


def _strip_when_match(pattern, str):
    match_attempt = pattern.match(str)
    return match_attempt and match_attempt.group(1) or str


def strip_ld_dump_type_suffixes(filename):
    res = filename
    res = _strip_when_match(ARCHIVE_SUFFIX_PATTERN, res)
    res = _strip_when_match(RDF_SERIALISAION_PATTERN, res)
    return res


def strip_compression_suffixes(filename):
    return _strip_when_match(ARCHIVE_SUFFIX_PATTERN, filename)


def _strip_config_suffixes(filename):
    res = filename
    res = _strip_when_match(YAML_FILETYPE_PATTERN, res)
    if res.endswith('-dld'):
        res = res[:-4]
    return res


class ComposeConfigGenerator(object):
    def __init__(self, configuration, working_directory, log=logging.getLogger()):
        self.configuration = configuration
        self.working_directory = working_directory
        self.log = log
        self.compose_config = ComposeConfigDefaultDict()
        self._dataset_basenames = set()
        print("init - passed configuration:\n{}".format(self.configuration))
        ensure_dir_exists(self.working_directory, self.log)
        self.models_volume_dir = osp.join(self.working_directory, 'models')
        ensure_dir_exists(self.models_volume_dir, self.log)

        self.pull_images(self.configuration)
        self.configure_compose()
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

    def configure_compose(self):
        self.configure_store()
        self.configure_load()
        self.configure_present()

    def configure_store(self):
        self._configure_singleton_component('store')

    def configure_load(self):
        def additional_config(compose_container_spec):
            compose_container_spec['links'].append('store')
            # TODO This might also be done by reading the labels of the load container resp. for the other categories.
            # A stub for loading the images and reading the labels is in pull_images
            compose_container_spec['volumes_from'].append('store')
            compose_container_spec['volumes'] = [osp.abspath(self.models_volume_dir) + ":/import"]

        self._configure_singleton_component('load', additional_config_thunk=additional_config)

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

        if not "present" in self.configuration["components"]:
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

    @classmethod
    def _http_client(cls, **kwargs):
        # TODO: set up proper HTTP-caching directory and test caching facilities
        cache_dir = osp.join(tempfile.gettempdir(), '.dld-http-caching')
        return httplib2.Http(cache=cache_dir, **kwargs)

    def prepare_import_data(self, datasets):
        def check_for_duplicate_import(basename, src_locator):
            stripped_name = strip_ld_dump_type_suffixes(basename)
            if stripped_name in self._dataset_basenames:
                msg_tmpl = "duplicate source '{src}' (stripped: '{str}')"
                raise RuntimeError(msg_tmpl.format(src=src_locator, str=strip_ld_dump_type_suffixes(basename)))
            else:
                self._dataset_basenames.add(stripped_name)

        for prev_content in glob(osp.join(self.models_volume_dir, '*')):
            if osp.isdir(prev_content):
                os.removedirs(prev_content)
            else:
                os.remove(prev_content)

        # ramification of the following line self#configure_store must be run before this method
        default_graph_name = self.compose_config['store'].get('environment').get('DEFAULT_GRAPH')
        if default_graph_name:
            with open(osp.join(self.models_volume_dir, "global.graph"), "w") as graph_file:
                graph_file.write(default_graph_name + "\n")

        for dataset_name, dataset_config in datasets.items():
            if 'file' in dataset_config:
                filepath = dataset_config['file']
                dirname, basename = osp.split(filepath)
                check_for_duplicate_import(basename, filepath)
                shutil.copyfile(filepath, osp.join(self.models_volume_dir, basename))
            elif "location" in dataset_config:
                loc_url = dataset_config['location']
                parsed_url = urlparse.urlparse(loc_url)
                if parsed_url.scheme not in ['http', 'https']:
                    msg_tmpl = "location given for dataset {ds} does not appear to be a http(s)-URL:\n{loc}"
                    raise RuntimeError(msg_tmpl.format(ds=dataset_name, loc=loc_url))

                basename = parsed_url.path and parsed_url.path.split('/')[-1] or parsed_url.netloc
                check_for_duplicate_import(basename, loc_url)
                http_client = self._http_client(timeout=10)
            else:
                msg_tmpl = "No data source ('file' or 'location' key) defined for dataset:\n{ds}"
                raise RuntimeError(msg_tmpl.format(ds=dataset_config))

            graph_name = dataset_config.get('graph_name')
            if graph_name and graph_file is not default_graph_name:
                # Virtuoso expects graph for triples.nt.gz in triples.nt.graph, hence the following line
                basename_no_archive = strip_compression_suffixes(basename)
                with open(osp.join(self.models_volume_dir, basename_no_archive + ".graph"), "w") as graph_file:
                    graph_file.write(graph_name + "\n")


def usage():
    print("please read at http://dld.aksw.org/ for further instructions")


def main(args=sys.argv[1:]):
    try:
        opts, args = getopt.getopt(args, "hc:w:u:f:l:",
                                   ["help", "config=", "workingdirectory=", "uri=", "file=", "location="])
    except getopt.GetoptError as err:
        # print help information and exit:
        print(err)  # will print something like "option -a not recognized"
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
        wd_from_cli = 'wd-' + _strip_config_suffixes(config_file)

    with open(config_file, 'r') as config_fd:
        user_config = yaml.load(config_fd)

    # Add command line arguments to configuration
    if any((uri_from_cli, file_from_cli, location_from_cli)):
        if uri_from_cli and (file_from_cli or location_from_cli):
            if "datasets" not in user_config:
                user_config["datasets"] = {}
            if "settings" not in user_config:
                user_config["settings"] = {}
            if "default" not in user_config["datasets"]:
                user_config["datasets"]["cli"] = {}
            user_config["settings"]["default_graph"] = uri_from_cli
            user_config["datasets"]["cli"]["uri"] = uri_from_cli
            if file_from_cli:
                user_config["datasets"]["cli"]["file"] = file_from_cli
            elif location_from_cli:
                user_config["datasets"]["cli"]["location"] = location_from_cli
        else:
            print("only the combinations uri and file or uri and location are permitted")
            usage()
            sys.exit(2)

    if "datasets" not in user_config or "components" not in user_config:
        print("dataset and setup configuration is needed")
        usage()
        sys.exit(2)

    # start dld process
    configurator = ComposeConfigGenerator(user_config, wd_from_cli)
    # transformation back to standard dict required to keep pyyaml from serialising class metadata
    docker_compose_config = ddict2dict(configurator.compose_config)
    print(yaml.dump(docker_compose_config))
    with open(osp.join(configurator.working_directory, 'docker-compose.yml'), mode='w') as compose_fd:
        yaml.dump(docker_compose_config, compose_fd)


if __name__ == "__main__":
    main()
