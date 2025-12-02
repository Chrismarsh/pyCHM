# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys
sys.path.insert(0, os.path.abspath('../'))
sys.path.insert(0, os.path.abspath('../src'))

import types
def _ensure_module(name, attrs=None, decorator=False):
    """Install a lightweight stub module if missing or mocked."""
    mod = sys.modules.get(name)
    is_mock = mod and mod.__class__.__name__.endswith("Mock")
    if mod is None or is_mock:
        stub = types.ModuleType(name)
        if decorator:
            def _register_dataset_accessor(_):
                def dec(cls): return cls
                return dec
            stub.register_dataset_accessor = _register_dataset_accessor
        if attrs:
            for k, v in attrs.items():
                setattr(stub, k, v)
        sys.modules[name] = stub

_ensure_module(
    "xarray",
    attrs={
        "DataArray": type("DataArray", (), {}),
        "Dataset": type("Dataset", (), {}),
    },
    decorator=True,
)
_ensure_module(
    "uxarray",
    attrs={
        "Grid": type("Grid", (), {"from_structured": classmethod(lambda cls, *a, **k: cls()), "to_xarray": lambda self: {}}),
        "UxDataset": type("UxDataset", (), {}),
    },
)

import sphinx_autosummary_accessors
from sphinx.locale import _
read_the_docs_build = os.environ.get('READTHEDOCS', None) == 'True'


# -- Project information -----------------------------------------------------

project = 'pyCHM'
copyright = '2020, Chris Marsh'
author = 'Chris Marsh'


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx_autosummary_accessors",
    "sphinx.ext.intersphinx",
    "sphinx_design",
    "myst_nb",
]
autosummary_generate = True
autosummary_generate = True
# Mock heavy optional dependencies so autodoc can import modules without them
autodoc_mock_imports = [
    'numpy',
    'geopandas',
    'osgeo',
    'osgeo_utils',
    'rioxarray',
    'rasterio',
    'esmpy',
    'zarr',
    'mpi4py',
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates', sphinx_autosummary_accessors.templates_path]

# Disable notebook execution on RTD to avoid failures from missing data/deps
nb_execution_mode = "off"

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

source_suffix = {
    '.rst': 'restructuredtext',
    '.ipynb': 'myst-nb',
    '.myst': 'myst-nb',
}

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = 'sphinx_book_theme'


master_doc = 'index'

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']

# Extensions to theme docs
def setup(app):
    from sphinx.domains.python import PyField
    from sphinx.util.docfields import Field

    app.add_object_type(
        'confval',
        'confval',
        objname='configuration value',
        indextemplate='pair: %s; configuration value',
        doc_field_types=[
            PyField(
                'type',
                label=_('Type'),
                has_arg=False,
                names=('type',),
                bodyrolename='class'
            ),
            Field(
                'default',
                label=_('Default'),
                has_arg=False,
                names=('default',),
            ),
        ]
    )
