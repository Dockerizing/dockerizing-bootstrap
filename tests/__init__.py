import sure
import sys
from os import path as osp

PROJECT_DIR = osp.dirname(osp.dirname(osp.realpath(__file__)))
sys.path.append(osp.join(PROJECT_DIR, 'baselibs', 'python'))

from dldbase.logging import logging_init
logging_init(osp.join(PROJECT_DIR, 'logs'))
