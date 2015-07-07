# Bootstrap Dockerizing

## requirements (for users)
The required python libraries are listed in `requirements.txt`. This tool has
been tested against Python 2.7.10 with recent versions:

docker-py==1.2.3
httplib2==0.9.1
PyYAML==3.11

To use pip to ensure up-to-date versions of the requirements installed, invoke

    pip install -Ur requirements.txt

[pyenv](https://github.com/yyuu/pyenv) can be useful to be able to obtain a
more recent Python version not provisioned by your package manager on *nix.
It also helps to circumvent interferences by providing an easy possibilty
to set up an isolated `virtualenv` for the use of `dld.py`. 
(The [pyenv installer](https://github.com/yyuu/pyenv-installer) is an easy
 way to set pyenv up.) 


## notes for developers
Please find an additional set of requirements for tests etc. specified in
`requirements-dev.txt`.

Use `nosetests` to run some coarse integrations tests defined in the `tests`
directory. Some tests depend von DBpedia dump data that can be retrieved by
invoking the `tests/download_dbpedia_samples.sh` script.

This tool utilized the Python `logging` libraries. By default, only selective
message with lean log formatting is put to stdout for non-developer usage.
You can trigger complete logging of all log messages to the `logs/` directory
and also debug messages to stdout my setting the `DLD_DEV` environment variable
(to any value different from the null-string).
