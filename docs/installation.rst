Installation
=================================
.. warning::

   This is unrelated to the Python helpfile tool called `pyCHM`!


The CHM python tools (herein pyCHM) can be installed via

::
   
   pip install CHM


However, it requires the non-pip available
`esmpy package <https://earthsystemmodeling.org/esmpy_doc/release/latest/html/install.html>`_.
Therefore either use this with conda (not tested) or install via spack. Please follow the instructions
`here <https://chm.readthedocs.io/en/develop/build.html#chm-with-spack>`_ for setting up the CHM repository with
spack. Then:

::

    spack install py-pychm



