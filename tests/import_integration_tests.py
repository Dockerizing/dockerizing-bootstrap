from __future__ import print_function, division
import tempfile
import shutil
import os
from os import path as osp
from glob import glob
import re
import sys
import time
import threading
import logging
from subprocess import Popen, PIPE

from invoke import run
from SPARQLWrapper import SPARQLWrapper, JSON

if __name__ != '__main__':
    import dld

TEST_DIR = osp.dirname(osp.realpath(__file__))
TEST_LOG = logging.getLogger('dld.test')
TEST_TEMP_DIR = os.environ.get('DLD_TEST_TMP')
VOS_IMPORT_COMPLETED_PATTERN = re.compile(r'done loading graphs \(start hanging around idle\)')


def test_simple_config_with_dataset_from_cli_args():
    """
        test scenario for:
            * only a single local file specified to import as CLI option
            * absolute path for config and file to import
    """

    test_name = 'test_simple_config_with_dataset_from_cli_args'
    graph_name = 'http://dld.aksw.org/testing#'
    import_file = osp.join(TEST_DIR, 'single_triple.ttl')
    config_file = osp.join(TEST_DIR, 'simple-dld.yml')
    osp.isfile(import_file).should.be(True)
    dld_args = ['-f', import_file, '-u', graph_name, '-c', config_file]
    expected_counts = {'http://dld.aksw.org/testing#': 1}
    with ImportIntegrationTest(test_name, dld_args, expected_triple_counts=expected_counts,
                               keep_tmpdir=True) as test:
        test.run()


def test_simple_config_default_config_name_wd_is_cwd():
    """
        test scenario for:
            * only a single local file specified to import as CLI option
            * relative path for default config and file to import
            * configuration name not specified -> default to 'dld.yml'
            * do no generate separate working subdirectory
    """
    test_name = 'test_simple_config_default_config_name_wd_is_cwd'
    graph_name = 'http://dld.aksw.org/testing#'
    import_file_src = osp.join(TEST_DIR, 'single_triple.ttl')
    config_file_src = osp.join(TEST_DIR, 'simple-dld.yml')
    dld_args = ['-f', 'single_triple.ttl', '-u', graph_name, '-w', '.']
    expected_counts = {'http://dld.aksw.org/testing#': 1}
    with ImportIntegrationTest(test_name, dld_args, expected_triple_counts=expected_counts) as test:
        shutil.copy(import_file_src, test.tmpdir)
        shutil.copy(config_file_src, osp.join(test.tmpdir, 'dld.yml'))
        test.run()


def test_simple_config_no_default_graph():
    """
        test scenario for:
            * only a single local file specified in config
            * do default graph specified
            * relative path for default config and file to import
            * do no generate separate working subdirectory
    """
    test_name = 'test_simple_config_no_default_graph'
    import_file_src = osp.join(TEST_DIR, 'single_triple.ttl')
    config_file_src = osp.join(TEST_DIR, 'simple-graph-defined-dld.yml')
    dld_args = ['-w', '.', '-c', config_file_src]
    expected_counts = {'http://dld.aksw.org/testing#': 1}
    with ImportIntegrationTest(test_name, dld_args, expected_triple_counts=expected_counts) as test:
        shutil.copy(import_file_src, test.tmpdir)
        test.run()


def test_dbpedia_local_archives():
    """
        test scenario for:
            * local archives to import defined in config
            * conversion bz2 -> gz required (at least for VOS)
            * different rdf serialisation formats among files to import (nt and ttl)
    """
    test_name = 'test_dbpedia_local_archives'
    config_file = osp.join(TEST_DIR, 'dbpedia-local-dld.yml')
    dld_args = ['-c', config_file]
    expected_counts = {'http://dbpedia.org': (791040, 791048)
                       }
    with ImportIntegrationTest(test_name, dld_args, expected_triple_counts=expected_counts,
                                  import_timeout=300) as test:
        for archive in ['homepages_en.ttl.bz2', 'old_interlanguage_links_en.nt.bz2']:
            shutil.copy(osp.join(TEST_DIR, archive), test.tmpdir)
        test.run()


def test_dbpedia_local_archives_list():
    """
        test scenario for:
            * local archives to import defined in listing file
            * conversion bz2 -> gz required (at least for VOS)
            * different rdf serialisation formats among files to import (nt and ttl)
    """
    test_name = 'test_dbpedia_local_archives_list'
    config_file = osp.join(TEST_DIR, 'dbpedia-local-list-dld.yml')
    dld_args = ['-c', config_file]
    expected_counts = {'http://dbpedia.org': (791040, 791048)
                       }
    with ImportIntegrationTest(test_name, dld_args, expected_triple_counts=expected_counts,
                                  import_timeout=300) as test:
        with open(osp.join(test.tmpdir, 'dbpedia_sample_datasets_local.list'), 'w') as file_list:
            for archive in ["homepages_en.ttl.bz2", 'old_interlanguage_links_en.nt.bz2', ]:
                filepath = osp.join(TEST_DIR, archive)
                osp.isfile(filepath).should.be(True)
                file_list.write(filepath + '\n')
        test.run()


def test_dbpedia_download_archives():
    """
        test scenario for:
            * download archives from official download server
            * conversion bz2 -> gz required (at least for VOS)
            * different rdf serialisation formats among files to import (nt and ttl)
    """
    test_name = 'test_dbpedia_download_archives'
    config_file = osp.join(TEST_DIR, 'dbpedia-download-dld.yml')
    dld_args = ['-c', config_file]
    expected_counts = {'http://dbpedia.org': (791040, 791048)
                       }
    with ImportIntegrationTest(test_name, dld_args, expected_triple_counts=expected_counts,
                                  import_timeout=300) as test:
        test.run()


def test_dbpedia_download_archives_list():
    """
        test scenario for:
            * download archives from official download server
            * conversion bz2 -> gz required (at least for VOS)
            * different rdf serialisation formats among files to import (nt and ttl)
    """
    test_name = 'test_dbpedia_download_archives_list'
    config_file = osp.join(TEST_DIR, 'dbpedia-download-list-dld.yml')
    list_file = osp.join(TEST_DIR, 'dbpedia_sample_datasets_download.list')
    dld_args = ['-c', config_file]
    expected_counts = {'http://dbpedia.org': (791040, 791048)
                       }
    with ImportIntegrationTest(test_name, dld_args, expected_triple_counts=expected_counts,
                                  import_timeout=300) as test:
        shutil.copy(list_file, osp.join(test.tmpdir, 'dbpedia_sample_datasets_download.list'))
        test.run()


class ImportIntegrationTest(object):
    def __init__(self, test_name='import_test', dld_args=[], import_timeout=10,
                 store_port=8891, expected_triple_counts=dict(), keep_tmpdir=False,
                 keep_containers=False):
        self.log = logging.getLogger('dld.test.' + self.__class__.__name__)
        self._import_timed_out = False
        self.keep_tmpdir = keep_tmpdir
        self.keep_containers = keep_containers
        self.test_name = test_name
        self.compose_name = test_name.replace('_', '')
        self.dld_args = dld_args
        self.store_port = store_port
        self.import_timeout = import_timeout
        self.expected_triple_counts = expected_triple_counts
        self._containers_created = False
        self.tmpdir = None

    def __enter__(self):
        self.tmpdir = tempfile.mkdtemp('wd', self.test_name, dir=TEST_TEMP_DIR)
        self.dld_args += ['-w', self.tmpdir]
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._containers_created and (self.keep_containers is not True):
            os.chdir(self.tmpdir)
            self.log.debug('cleaning up containers')
            run("docker-compose -p {pn} kill".format(pn=self.compose_name), hide=False, warn=True)
            run("docker-compose -p {pn} rm -f".format(pn=self.compose_name), hide=False, warn=True)
        if self.keep_tmpdir is not True:
            if self.tmpdir:
                shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _ensure_context(self):
        if self.tmpdir is None:
            raise RuntimeError("use it as a context!")

    def run_dld(self):
        self._ensure_context()
        os.chdir(self.tmpdir)
        dld.main(self.dld_args)

    def run_compose_up(self):
        self._ensure_context()
        os.chdir(self.tmpdir)
        res = run("docker-compose -p {pn} up -d".format(pn=self.compose_name), hide=False)
        if res.ok:
            self._containers_created = True

    @classmethod
    def _import_completed_pattern(cls):
        return VOS_IMPORT_COMPLETED_PATTERN

    @classmethod
    def _is_import_completed_msg(cls, line):
        try:
            next(cls._import_completed_pattern().finditer(line))
            return True
        except StopIteration:
            return False

    def _endpooint_url(self):
        return "http://localhost:{port}/sparql".format(port=self.store_port)

    def wait_for_completed_import(self):
        self._ensure_context()
        timed_out = threading.Event()

        logtail = Popen(
            "docker logs -f {}_load_1".format(self.compose_name),
            shell=True,
            stdout=PIPE,
            stderr=None,
            cwd=self.tmpdir,
            bufsize=1
        )

        def look_for_completion_message():
            for line in iter(logtail.stdout.readline, b''):
                TEST_LOG.debug("({t}) read line: {l}".format(l=line, t=time.time()))
                if timed_out.is_set():
                    return False
                elif self._is_import_completed_msg(line):
                    return True

        def set_timed_out():
            TEST_LOG.debug("set timed_out event")
            timed_out.set()

        timer = threading.Timer(self.import_timeout, set_timed_out)
        timer.daemon = True
        timer.start()
        completed = look_for_completion_message()
        timer.cancel()

        if not completed:
            raise RuntimeError("import timed out")

    def verify_imported_triple_counts(self):
        def bindings_to_dict(bindings):
            return dict((b['graph']['value'], int(b['count']['value'])) for b in bindings)

        sparql = SPARQLWrapper(self._endpooint_url(), returnFormat=JSON)
        sparql.setQuery('''
          SELECT (SAMPLE(?g) AS ?graph) (count(*) AS ?count) {
            GRAPH ?g { ?s ?p ?o }
          } GROUP BY ?g
        ''')
        result = sparql.queryAndConvert()

        actual_counts = bindings_to_dict(result['results']['bindings'])

        self.log.debug(actual_counts)
        for graph_name, expected_count in self.expected_triple_counts.items():
            if isinstance(expected_count, tuple) and len(expected_count) is 2:
                actual_counts.get(graph_name, 0).should.be.within(*expected_count)
            elif isinstance(expected_count, int):
                actual_counts.get(graph_name, 0).should.equal(expected_count)
        self.log.debug('finished verifying counts')

    def run(self):
        self.run_dld()
        self.run_compose_up()
        self.wait_for_completed_import()
        self.verify_imported_triple_counts()

if __name__ == '__main__':
    PROJECT_DIR = osp.dirname(osp.dirname(osp.realpath(__file__)))
    sys.path.append(PROJECT_DIR)
    sys.path.append(osp.join(PROJECT_DIR, 'baselibs', 'python'))

    from dldbase.logging import logging_init

    logging_init(osp.join(PROJECT_DIR, 'logs'))

    import dld
    import sure

    test_simple_config_with_dataset_from_cli_args()
    test_simple_config_no_default_graph()
    # test_dbpedia_download_archives()
    # test_dbpedia_download_archives_list()
