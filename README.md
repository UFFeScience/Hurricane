### Hurricane - a Dataflow-oriented Data Service for Smart Cities Applications

Code | [![PyPI Version](https://img.shields.io/pypi/v/bioprov)](https://pypi.org/project/bioprov/) | [![lint](https://github.com/vinisalazar/BioProv/workflows/lint/badge.svg?branch=master)](https://github.com/vinisalazar/BioProv/actions?query=workflow%3Alint) | [![Code style](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black) 
---------------|--|--|--
Tests | [![Build Status](https://travis-ci.org/vinisalazar/BioProv.svg?branch=master)](https://travis-ci.org/vinisalazar/BioProv) |  [![tests](https://github.com/vinisalazar/bioprov/workflows/tests/badge.svg?branch=master)](https://github.com/vinisalazar/bioprov/actions?query=workflow%3Atests) | [![Coverage Status](https://coveralls.io/repos/github/vinisalazar/BioProv/badge.svg?branch=master&service=github)](https://coveralls.io/github/vinisalazar/BioProv?branch=master&service=github)
Docs | [![Docs status](https://readthedocs.org/projects/bioprov/badge/?version=latest)](https://bioprov.readthedocs.io/en/latest/?badge=latest) | [![License](https://img.shields.io/github/license/vinisalazar/bioprov)](https://github.com/vinisalazar/BioProv/blob/master/LICENSE) | [![binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/vinisalazar/bioprov/master?filepath=docs%2Ftutorials%2F) 


Hurricane is a configurable and extensible data service for smart city applications. 
Hurricane is designed on top of the dataflow abstraction for data management and processing, i.e., the entire processing is executed after the instantiation of multiple dataflows, where each step of the data management is monitored and has its data and metadata captured. 
Each dataflow has a specific goal and it is automatically instantiated on Apache Airflow framework based on previously defined configurations set by the user. 
Apache {irflow provides several fundamental features such as parallel and distributed processing that can be applied depending on the data volume that needs to be processed by Hurricane. 


### Quickstart
