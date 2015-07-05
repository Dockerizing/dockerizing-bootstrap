# Bootstrap Dockerizing

## requirements (for users)
The required python libraries are listed in `requirements.txt`. This tool has
been tested against Python 2.7.10 with recent versions:

docker-py==1.2.3
httplib2==0.9.1
PyYAML==3.11

To use pip to ensure up-to-date versions of the requirements installed, invoke
    pip install -Ur requirements.txt


## notes for developers
Please find an additional set of requirements for tests etc. specified in
`requirements-dev.txt`.

Use `nosetests` to run some coarse integrations tests defined in the `tests`
directory. Some tests depend von DBpedia dump data that can be retrieved by
invoking the `tests/download_dbpedia_samples.sh` script.


**Not finished, yet**.
