This is on example of a [docker compose](http://docs.docker.com/compose/) setup which fetches the neccessary images and starts containers for an OntoWiki setup with virtuoso.

If you have `docker-compose` installed, just run

    docker-compose up

in this folder.

Following other images are available:

# Triple Store Containers

Triple store containers provide a graph database as service. For triple stores the alias `triplestore` should be used when linking to them.

* [aksw/dld-store-virtuoso7](https://registry.hub.docker.com/u/aksw/dld-store-virtuoso7/) Virtuoso 7 triple store
* …

# Load and Back-Up Containers

* …

# Exploration and Presentation Containers

* [aksw/dld-present-ontowiki](https://registry.hub.docker.com/u/aksw/dld-present-ontowiki/) OntoWiki presentation (only works with Virtuoso)
* …
