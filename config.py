import re
from os import path as osp, environ as env

from dldbase import dockerutil

SELINUX_VOLUME_ADJUSTMENT_DOCKER_VERSIONS_PATTERN = re.compile('^(1\.([7-9]|(\d\d)))|(2\.)')
ADDITIONAL_VOLUMES_FROM_ENV_VAR = "DLD_VOLUMES_FROM"
INTERNAL_IMPORT_VOLUME_ENV_VAR = "DLD_INTERNAL_IMPORT"
IMPORT_VOLUME_MOUNTPOINT_ENV_VAR = "DLD_IMPORT_MOUNT"


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

    @property
    def additional_volumes_from(self):
        env_value = env.get(ADDITIONAL_VOLUMES_FROM_ENV_VAR, '')
        splitted = env_value.split(',')
        if len(splitted) == 1 and (not splitted[0]):
            return None
        else:
            return splitted

    @property
    def internal_import_volume(self):
        env_value = env.get(INTERNAL_IMPORT_VOLUME_ENV_VAR, '')
        if env_value == "0" or "false".startswith(env_value.lower()):
            return False
        else:
            return bool(env_value)

    @property
    def import_volume_destination(self):
        return env.get(IMPORT_VOLUME_MOUNTPOINT_ENV_VAR, '/import')

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
