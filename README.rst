=========
inpe_stac
=========

STAC (SpatioTemporal Asset Catalog) implementation for INPE Catalog.


Run using a Python virtual environment
======================================

Install a specific Python version using pyenv:

.. code-block:: shell

        pyenv install 3.7.0


Create a Python environment with the Python version above through pyenv:

.. code-block:: shell

        pyenv virtualenv 3.7.0 inpe_stac


Activate the virtual environment:

.. code-block:: shell

        pyenv activate inpe_stac


Install the requirements:

.. code-block:: shell

        pip install -r requirements.txt


Export the environment variables:

.. code-block:: shell

        set -a && source environment.dev.env && set +a


Run the service:

.. code-block:: shell

        flask run --host=0.0.0.0 --port=5001


Run using a Docker image
========================

Build a new image using the Dockerfile (development or production):

.. code-block:: shell

        docker build -t inpe-cdsr-inpe_stac -f dev.Dockerfile . --no-cache
        docker build -t registry.dpi.inpe.br/dgi/inpe_stac:0.0.5 -f prod.Dockerfile . --no-cache


Inside ``docker-compose.yml`` file there are two services
(development or production), choose which one you like to run.
Then, run the Docker compose:

.. code-block:: shell

        docker-compose -f docker-compose.yml up


If you have credentials, then push the image to your registry:

.. code-block:: shell

        docker push registry.dpi.inpe.br/dgi/inpe_stac:0.0.5
