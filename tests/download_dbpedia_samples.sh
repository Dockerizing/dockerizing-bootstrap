#!/usr/bin/env bash

set -e

for ds in http://data.dws.informatik.uni-mannheim.de/dbpedia/2014/en/{homepages_en.ttl.bz2,old_interlanguage_links_en.nt.bz2}; do
  wget $ds
done
