#! /usr/bin/env python
import sys
import os
from os import path as osp
import argparse as ap
import re
import logging
import logging.config
from collections import defaultdict
from textwrap import dedent
import tempfile

import yaml
import httplib2
from docker import Client

from data.datasets import ImportsCollector
from tools import http_url, is_dict_like, is_list_like

#non-dererred import when this is not run as main script (e.g. through nosetests)
if __name__ != '__main__':
    from dldbase import DEV_MODE
    from config import DLDConfig

from tools import FilenameOps, ComposeConfigDefaultDict, HeadRequest, alpha_gen

LAST_WORD_PATTERN = re.compile('[a-zA-Z0-9]+$')

PROJECT_DIR = osp.dirname(osp.realpath(__file__))
DLD_LOG = logging.getLogger('dld')

# TODO: check if we can use copy.deepcopy instead
def ddict2dict(d):
    for k, v in d.items():
        if is_dict_like(v):
            d[k] = ddict2dict(v)
        elif is_list_like(v):
            d[k] = list(v)
    return dict(d)


def ensure_dir_exists(directory, log, warn_exists=True):
    if not os.path.exists(directory):
        os.makedirs(directory)
    else:
        DEV_MODE and warn_exists and log.warning("The given path '{d}' already exists.".format(d=directory))


class ComposeConfigGenerator(object):
    def __init__(self, yaml_config, dld_config):
        self.yaml_config = yaml_config
        self.dld_config = dld_config
        self.log = logging.getLogger('dld.' + self.__class__.__name__)
        self.compose_config = ComposeConfigDefaultDict()
        self._steps_done = defaultdict(lambda: False)
        self.log.debug("init - passed configuration:\n{}".format(self.yaml_config))

    def run(self):
        # self.pull_images(self.configuration) #not using image metadata yet
        self.create_compose_config()
        self.prepare_import_data(self.yaml_config["datasets"])

    def pull_images(self, config):
        docker = Client()
        images = docker.images(filters={"label": "org.aksw.dld"})

        # docker.inspect_image

    def _add_global_settings(self, component_settings):
        global_settings = self.yaml_config.get('settings', dict())
        component_settings = is_dict_like(component_settings) and component_settings.copy() or dict()
        component_settings.update(global_settings)
        return component_settings

    @classmethod
    def _generic_settings_handler(cls, component_settings, compose_container_spec):
        if 'default_graph' in component_settings:
            compose_container_spec['environment']['DEFAULT_GRAPH'] = component_settings['default_graph']

    def _configure_singleton_component(self, component_name, settings_handler=None, additional_config_thunk=None):
        if component_name not in self.yaml_config['components']:
            return
        component_config = self.yaml_config['components'][component_name]
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
        if self.dld_config.additional_volumes_from:
            self.log.info("adding volumes from meta-container: {l}".format(l=self.dld_config.additional_volumes_from))
            compose_container_spec['volumes_from'] += self.dld_config.additional_volumes_from

    def create_compose_config(self):
        self.configure_compose()
        # transformation back to standard dict required to keep pyyaml from serialising class metadata
        docker_compose_config = ddict2dict(self.compose_config)
        DLD_LOG.debug("\n" + yaml.safe_dump(docker_compose_config))
        ensure_dir_exists(self.dld_config.working_dir, self.log, warn_exists=False)
        with open(osp.join(self.dld_config.working_dir, 'docker-compose.yml'), mode='w') as compose_fd:
            yaml.safe_dump(docker_compose_config, compose_fd)

    def configure_compose(self):
        self.configure_store()
        self.configure_load()
        self.configure_present()

    @property
    def wd_ready_message(self):
        msg_tmpl = dedent("""\
        Your docker-compose configuration and import data has been set up at '{wd}'.
        Now change into that directory and invoke `docker-compose up -d` to start  the components
        (possibly triggering also the data import process)

        `docker-compose ps` will give you an overview for the component containers and tell you
        which host ports to use to reach your components.

        `docker-compose logs` will allow you to inspect the state of the setup processes.

        Have fun!""")
        abs_wd = osp.realpath(self.dld_config.working_dir)
        if DEV_MODE:
            return "setup completed at {wd}".format(wd=abs_wd)
        else:
            return msg_tmpl.format(wd=abs_wd)

    def configure_store(self):
        if not self._steps_done['store']:
            self._configure_singleton_component('store')
            self._steps_done['store'] = True

    def configure_load(self):
        def additional_config(load_component_spec):
            load_component_spec['links'].append('store')
            # TODO This might also be done by reading the labels of the load container resp. for the other categories.
            # A stub for loading the images and reading the labels is in pull_images
            load_component_spec['volumes_from'].append('store')
            import_vol_dest = self.dld_config.import_volume_destination
            load_component_spec['environment']['IMPORT_SRC'] = import_vol_dest
            if not self.dld_config.internal_import_volume:
                if self.dld_config.selinux_volumes_tweaks_supported:
                    import_vol_dest += ':z'
                load_component_spec['volumes'] = [osp.abspath(self.dld_config.models_dir) + ":" + import_vol_dest]

        if not self._steps_done['load']:
            self._configure_singleton_component('load', additional_config_thunk=additional_config)
            self._steps_done['load'] = True

    def _extract_last_word(string, fallback=None):
        try:
            return next(LAST_WORD_PATTERN.finditer(string)).group(0)
        except StopIteration:
            if isinstance(fallback, string):
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

        if ('present' not in self.yaml_config["components"]) or self._steps_done['present']:
            return
        present_component_group = self.yaml_config["components"]['present']
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

        ensure_dir_exists(self.dld_config.models_dir, self.log)
        collector = ImportsCollector(self.dld_config)
        collector.prepare(datasets_fragment)


def build_argument_parser():
    helptexts = {
        'app_descr': "DLD command line tool to orchestrate Linked Data tools.",
        'app_epilog': "See http://dld.aksw.org/ for further explanation and instructions.",
        'config-file': "the *-dld.yml file specifying the desired LD tool orchestration (defaults to 'dld.yml')",
        'working-dir': "target directory for compose configuration and collected LD dumps for import",
        'target-named-graph': "named graph as destination for LD to import specified with the -f or -l option",
        'dump-file': "LD dump file to import into RDF storage solution",
        'dump-location': "location (as URL) of dump file to download and import into RDF storage solution",
        'do-up' : "let this script run 'docker-compose up' after successful preperation of the DLD setup",
        'help': "print this usage/help info"
    }

    def file_realpath(path_str):
        try:
            assert osp.isfile(path_str)
            return osp.realpath(path_str)
        except AssertionError:
            raise RuntimeError("Cannot find a file at '{p}'".format(p=path_str))

    parser = ap.ArgumentParser(prog='dld.py', description=helptexts['app_descr'], epilog=helptexts['app_epilog'])
    parser.add_argument("-c", "--config-file", default='dld.yml', help=helptexts['config-file'])
    parser.add_argument("-w", "--working-dir", default=None, help=helptexts['working-dir'])
    parser.add_argument("-g", "--target-named-graph", default=None,
                        help=helptexts['target-named-graph'])
    parser.add_argument("-f", "--dump-file", type=file_realpath, default=None,
                        help=helptexts['dump-file'])
    parser.add_argument("-l", "--dump-location", default=None, type=http_url,
                        help=helptexts['dump-location'])
    parser.add_argument("-u", "--do-up", action='store_true',
                        help=helptexts['do-up'])
    parser.set_defaults(do_up=False)


    return parser

def run_compose(*cli_args):
    prev_argv = sys.argv
    try:
        sys.argv = ['docker-compose'] + list(cli_args)
        from compose.cli import main as docker_main
        docker_main.main()
    finally:
        sys.argv = prev_argv


def main(args=sys.argv[1:]):
    argparser = build_argument_parser()
    args_ns = argparser.parse_args(args)

    #deferring DLDConfig import until here to allow getting CLI --help also when the Docker deamon is not accessible
    from config import DLDConfig
    dld_config = DLDConfig()
    dld_config.working_dir = args_ns.working_dir

    if not dld_config.working_dir:
        dld_config.working_dir = 'wd-' + FilenameOps.strip_config_suffixes(
            osp.basename(args_ns.config_file))

    with open(args_ns.config_file, 'r') as config_fd:
        yaml_config = yaml.load(config_fd)

    # Add command line arguments to configuration
    if any((args_ns.target_named_graph, args_ns.dump_file, args_ns.dump_location)):
        if args_ns.target_named_graph and (args_ns.dump_file or args_ns.dump_location):
            if "datasets" not in yaml_config:
                yaml_config["datasets"] = {}
            if "settings" not in yaml_config:
                yaml_config["settings"] = {}
            if "cli" not in yaml_config["datasets"]:
                yaml_config["datasets"]["cli"] = {}
            else:
                raise RuntimeError("Reserved dataset key 'cli' used in configuration file")

            yaml_config["datasets"]["cli"]["graph_name"] = args_ns.target_named_graph
            if args_ns.dump_file:
                yaml_config["datasets"]["cli"]["file"] = args_ns.dump_file
            elif args_ns.dump_location:
                yaml_config["datasets"]["cli"]["location"] = args_ns.dump_location
        else:
            DLD_LOG.error("only the combinations (graph uri and file) or (graph uri and location) are permitted")
            argparser.print_usage()
            sys.exit(2)

    if is_dict_like(yaml_config.get("settings")):
        dld_config.default_graph_name = yaml_config["settings"].get("default_graph")
    if args_ns.target_named_graph:
        dld_config.default_graph_name = args_ns.target_named_graph

    if "datasets" not in yaml_config or "components" not in yaml_config:
        DLD_LOG.error("dataset and component configuration is needed")
        argparser.print_usage()
        sys.exit(2)

    dld_config.ensure_required_settings()
    # start dld process
    configurator = ComposeConfigGenerator(yaml_config, dld_config)
    configurator.run()
    if args_ns.do_up:
        msg_templ = "Finished preparing compose setup. Changing to '{wd}' and performing 'docker-compose up'..."
        DLD_LOG.info(msg_templ.format(wd = dld_config.working_dir))
        os.chdir(dld_config.working_dir)
        run_compose("up")
    else:
        DLD_LOG.info(configurator.wd_ready_message)


if __name__ == "__main__":
    import sys

    sys.path.append(osp.join(PROJECT_DIR, 'baselibs', 'python'))
    if os.getcwd() != PROJECT_DIR:
        sys.path.append(PROJECT_DIR)

    # deferred imports, since PYTHONPATH needed to be tweaked before (interim solution)
    from dldbase import DEV_MODE, logutil

    logutil.logging_init(osp.join(PROJECT_DIR, 'logs'))
    main()
