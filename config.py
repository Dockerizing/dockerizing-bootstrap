import re
from os import path as osp

from dldbase import dockerutil

SELINUX_VOLUME_ADJUSTMENT_DOCKER_VERSIONS_PATTERN = re.compile('^(1\.([7-9]|(\d\d)))|(2\.)')

class DLDConfig(object):
    __required_settings = {'working_dir': str, 'models_dir': str}

    def __init__(self):
        self.__models_dir = None
        self.default_graph_name = None

    # we allow the models dir to be specified explicitly, if it is not, we derive it
    @property
    def models_dir(self):
        if self.__models_dir:
            return self.__models_dir
        else:
            if self.working_dir:
                return osp.join(self.working_dir, 'models')
            else:
                raise RuntimeError("working directory not set")

    with dockerutil.docker_client() as dc:
        __docker_engine_version = dc.version()['Version']

    @property
    def docker_engine_version(self):
        return DLDConfig.__docker_engine_version

    @property
    def selinux_volumes_tweaks_supported(self):
        return SELINUX_VOLUME_ADJUSTMENT_DOCKER_VERSIONS_PATTERN.match(self.docker_engine_version) is not None

    @models_dir.setter
    def set_models_dir(self, models_dir):
        self.__models_dir = models_dir

    def ensure_required_settings(self):
        def ensure_required_settings(self):
            for attr_name, exp_type in DLDConfig._required_settings.items():
                if not isinstance(getattr(self, attr_name, None), exp_type):
                    raise RuntimeError("required settings is missing: {an}".format(an=attr_name))
