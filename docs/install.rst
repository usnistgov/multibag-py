.. _multibag-py-install:

**********************
Installing multibag-py
**********************

Currently, multibag-py is only installable via source downloaded from the
GitHub repository at https://github.com/usnistgov/multibag-py.

Source Installation
-------------------

Retrieving the code
^^^^^^^^^^^^^^^^^^^

The latest source code is available from the repository on GitHub:

.. code-block:: bash

   git clone https://github.com/usnistgov/multibag-py
   cd multibag-py


Requirements
^^^^^^^^^^^^

This code has the following run-time requirements

* python v2.7, v3.5+
* bagit (v1.6.0+)
* fs (v2.0.0+)
* funcsigs (v1.0.2+, when running under python v2.7)

Building multibag-py from source additionally requires:

* setuptools
* sphinx (v1.8.0+, if building documentation)

These requirements can be install using ``pip``:

.. code-block:: bash

   pip install -r requirements.txt

Building and Installing
^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: bash

   python setup.py install


