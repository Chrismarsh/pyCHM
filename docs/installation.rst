Installation
=================================

The CHM python tools (herein pyCHM) can be installed via

::
   
   pip install CHM


Supported versions of Python are 3.7 and 3.8.

.. warning::
   
   This is unrelated to the Python helpfile tool called `pyCHM`!

pyCHM requires the ESMF python package, `pyESMF <https://github.com/Chrismarsh/pyESMF>`_. Because this unofficial
ESMFpy package builds its own binary (this avoids having to use conda) into each ``venv``, pip's cache can sometimes mess things up. If
upon importing ``CHM`` there is an error about finding a ``.mk`` file do the following in the ``venv`` that you've install pyCHM to:

::

    pip uninstall pyESMF
    pip install --no-cache pyESMF


