import os
import sys
from os.path import abspath, dirname, join

from dynaconf import Dynaconf


# Make pwd the same as project root for correct paths in settings.
os.chdir(abspath(join(dirname(__file__), '..')))


# We are using environments, since it's required for dynaconf_aws_loader.
# But for the local settings please use only 'default' namespace.
settings = Dynaconf(
    root_path='pdf2image',
    settings_files=['defaults.yaml'],
    environments=True,
    env_switcher='APP_ENVIRONMENT',
    envvar_prefix='APP',
    LOADERS_FOR_DYNACONF=[
        "dynaconf_aws_loader.loader",
        "dynaconf.loaders.env_loader",
    ],
    SSM_PARAMETER_PROJECT_PREFIX_FOR_DYNACONF=''
)

for source, loaded in settings.loaded_by_loaders.items():
    if source.loader == 'set_method':
        continue
    print(
        f'Settings loaded by {source.loader}',
        f'[env={source.env} source={source.identifier}]:',
        ", ".join(loaded.keys())
    )

settings.populate_obj(sys.modules[__name__])
