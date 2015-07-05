from __future__ import print_function, division
import tempfile
import dld
import shutil
import os
from os import path as osp
from glob import glob
import re
import time
import threading
from subprocess import Popen, PIPE
from invoke import run
import logging as log
from SPARQLWrapper import SPARQLWrapper, JSON

TEST_DIR = osp.dirname(osp.realpath(__file__))
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
    with _import_integration_test(test_name, dld_args, expected_triple_counts=expected_counts) as test:
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
    with _import_integration_test(test_name, dld_args, expected_triple_counts=expected_counts) as test:
        shutil.copy(import_file_src, test.tmpdir)
        shutil.copy(config_file_src, osp.join(test.tmpdir, 'dld.yml'))
        test.run()


def test_dbpedia_local_archives():
    """
        test scenario for:
            * local archives to import defined in config
            * conversion bz2 -> gz required (at least for VOS)
            * different rdf serialisation formats among files to import (nt and ttl)
    """
    test_name = 'test_simple_config_default_config_name_wd_is_cwd'
    config_file = osp.join(TEST_DIR, 'dbpedia-local-dld.yml')
    dld_args = ['-c', config_file]
    expected_counts = {'http://dbpedia.org': (791040, 791048)
                       }
    with _import_integration_test(test_name, dld_args, expected_triple_counts=expected_counts,
                                  import_timeout=300) as test:
        log.debug("glob results: {}".format(glob(osp.join(TEST_DIR, '{homepages,old_interlanguage}*bz2'))))
        for archive in ['homepages_en.ttl.bz2', 'old_interlanguage_links_en.nt.bz2']:
            src = osp.join(TEST_DIR, archive)
            log.debug("copying {a} to {tmp}".format(a=src, tmp=test.tmpdir))
            shutil.copy(src, test.tmpdir)
        test.run()


class ImportIntegrationTest(object):
    def __init__(self, test_name='import_test', dld_args=[], import_timeout=10,
                 store_port=8891, expected_triple_counts=dict(), keep_tmpdir=False, ):
        self._import_timed_out = False
        self.keep_tmpdir = keep_tmpdir
        self.test_name = test_name
        self.compose_name = test_name.replace('_', '')
        self.dld_args = dld_args
        self.store_port = store_port
        self.import_timeout = import_timeout
        self.expected_triple_counts = expected_triple_counts
        self._containers_created = False
        self.tmpdir = None

    def __enter__(self):
        self.tmpdir = tempfile.mkdtemp('wd', self.test_name)
        self.dld_args += ['-w', self.tmpdir]
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._containers_created:
            os.chdir(self.tmpdir)
            print('cleaning up containers')
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
                log.debug("({t}) read line: {l}".format(l=line, t=time.time()))
                if timed_out.is_set():
                    return False
                elif self._is_import_completed_msg(line):
                    return True

        def set_timed_out():
            log.debug("set timed_out event")
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

        print(actual_counts)
        for graph_name, expected_count in self.expected_triple_counts.items():
            if isinstance(expected_count, tuple) and len(expected_count) is 2:
                actual_counts.get(graph_name, 0).should.be.within(*expected_count)
            elif isinstance(expected_count, int):
                actual_counts.get(graph_name, 0).should.equal(expected_count)
        print('finished verifying counts')

    def run(self):
        self.run_dld()
        self.run_compose_up()
        self.wait_for_completed_import()
        self.verify_imported_triple_counts()


def _import_integration_test(*args, **kwargs):
    return ImportIntegrationTest(*args, **kwargs)
