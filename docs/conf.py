import os
import shutil
import sys

# -- Path setup --------------------------------------------------------------

__location__ = os.path.dirname(__file__)

sys.path.insert(0, os.path.join(__location__, "../libs/foundry-dev-tools/src"))

# -- Run sphinx-apidoc -------------------------------------------------------

from sphinx.ext import apidoc

output_dir = os.path.join(__location__, "api")
foundry_dev_tools_module_dir = os.path.join(__location__, "../libs/foundry-dev-tools/src/foundry_dev_tools")
try:
    shutil.rmtree(output_dir)
except FileNotFoundError:
    pass

try:
    args = ["--implicit-namespaces", "-M", "-T", "-f", "-e", "-o", output_dir]

    apidoc.main([*args, foundry_dev_tools_module_dir, foundry_dev_tools_module_dir + "/__init__.py",foundry_dev_tools_module_dir+"/resources/__init__.py"])
except Exception as e:
    print(f"Running `sphinx-apidoc` failed!\n{e}")

# -- General configuration ---------------------------------------------------

extensions = [
    "myst_parser",
    "sphinx.ext.autodoc",
    "sphinx.ext.intersphinx",
    "sphinx.ext.todo",
    "sphinx.ext.autosummary",
    "sphinx.ext.viewcode",
    "sphinx.ext.coverage",
    "sphinx.ext.doctest",
    "sphinx.ext.ifconfig",
    "sphinx.ext.mathjax",
    "sphinx.ext.napoleon",
    "sphinx.ext.autosectionlabel",
    "sphinx_tippy",
    "sphinxcontrib.mermaid",
    "sphinx_inline_tabs",
]

autoclass_content = "init"
autodoc_default_options = {
    "undoc-members": True,
}
autodoc_typehints = "description"
autodoc_class_signature = "separated"
autodoc_member_order = "bysource"
autosectionlabel_prefix_document = True

tippy_skip_anchor_classes = ("headerlink", "sd-stretched-link", "sd-rounded-pill")
tippy_anchor_parent_selector = "article.bd-article"

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# -- MyST-Parser --------------------------------------------------------------

myst_enable_extensions = [
    "amsmath",
    "colon_fence",
    "deflist",
    "dollarmath",
    "html_image",
    "linkify",
    "replacements",
    "smartquotes",
    "substitution",
    "tasklist",
    "attrs_block",
]
myst_fence_as_directive = ["mermaid"]

napoleon_google_docstring = True

# The suffix of source filenames.
source_suffix = [".rst", ".md"]

# The master toctree document.
master_doc = "index"

# General information about the project.
project = "foundry-dev-tools"
copyright = "2023, (Merck KGaA, Darmstadt, Germany)"

try:
    from foundry_dev_tools import __version__ as version
except ImportError:
    version = ""

release = version

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store", ".venv"]

# If true, '()' will be appended to :func: etc. cross-reference text.
add_function_parentheses = True

# The name of the Pygments (syntax highlighting) style to use. Commented out it uses the themes default
pygments_dark_style = "github-dark"

# If this is True, todo emits a warning for each TODO entries. The default is False.
todo_emit_warnings = True


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
html_theme = "sphinx_book_theme"

html_theme_options = {
    "show_navbar_depth": 2,
    "show_toc_level": 4,
    "announcement":"Note: This is the documentation for Foundry DevTools v2",
}

# The name for this set of Sphinx documents.  If None, it defaults to
# "<project> v<release> documentation".
# html_title = None
html_title = "foundry-dev-tools"
# A shorter title for the navigation bar.  Default is the same as html_title.
# html_short_title = None

# The name of an image file (relative to this directory) to place at the top
# of the sidebar.
# html_logo = ""

# The name of an image file (within the static path) to use as favicon of the
# docs.  This file should be a Windows icon file (.ico) being 16x16 or 32x32
# pixels large.
# html_favicon = None

# If not '', a 'Last updated on:' timestamp is inserted at every page bottom,
# using the given strftime format.
html_last_updated_fmt = "%H:%M %b %d, %Y"

# If true, SmartyPants will be used to convert quotes and dashes to
# typographically correct entities.
# html_use_smartypants = True

# Custom sidebar templates, maps document names to template names.
# html_sidebars = {}

# Additional templates that should be rendered to pages, maps page names to
# template names.
# html_additional_pages = {}

# If false, no module index is generated.
# html_domain_indices = True

# If false, no index is generated.
# html_use_index = True

# If true, the index is split into individual pages for each letter.
# html_split_index = False

# If true, links to the reST sources are added to the pages.
# html_show_sourcelink = True

# If true, "Created using Sphinx" is shown in the HTML footer. Default is True.
# html_show_sphinx = True

# If true, "(C) Copyright ..." is shown in the HTML footer. Default is True.
# html_show_copyright = True

# If true, an OpenSearch description file will be output, and all pages will
# contain a <link> tag referring to it.  The value of this option must be the
# base URL from which the finished HTML is served.
# html_use_opensearch = ''

# This is the file name suffix for HTML files (e.g. ".xhtml").
# html_file_suffix = None

# Output file base name for HTML help builder.
htmlhelp_basename = "foundry_dev_tools-doc"

# -- Options for LaTeX output ------------------------------------------------

# latex_elements = {
    # The paper size ("letterpaper" or "a4paper").
    # "papersize": "letterpaper",
    # The font size ("10pt", "11pt" or "12pt").
    # "pointsize": "10pt",
    # Additional stuff for the LaTeX preamble.
    # "preamble": "",
# }

# Grouping the document tree into LaTeX files. List of tuples
# (source start file, target name, title, author, documentclass [howto/manual]).
latex_documents = [
    (
        "index",
        "user_guide.tex",
        "Foundry DevTools Documentation",
        "(Merck KGaA, Darmstadt, Germany)",
        "manual",
    )
]

# -- External mapping --------------------------------------------------------
python_version = ".".join(map(str, sys.version_info[0:2]))
intersphinx_mapping = {
    "spark": ("https://spark.apache.org/docs/latest/api/python", None),
    "pandas": ("https://pandas.pydata.org/docs", None),
    "pyarrow": ("https://arrow.apache.org/docs", None),
    "tornado": ("https://www.tornadoweb.org/en/stable", None),
    "werkzeug": ("https://werkzeug.palletsprojects.com/en/latest", None),
    "sphinx": ("https://www.sphinx-doc.org/en/master", None),
    "python": ("https://docs.python.org/" + python_version, None),
    "matplotlib": ("https://matplotlib.org/stable/", None),
    "numpy": ("https://numpy.org/doc/stable", None),
    "sklearn": ("https://scikit-learn.org/stable", None),
    "pandas": ("https://pandas.pydata.org/pandas-docs/stable", None),
    "scipy": ("https://docs.scipy.org/doc/scipy/", None),
    "setuptools": ("https://setuptools.pypa.io/en/stable/", None),
    "requests": ("https://requests.readthedocs.io/en/latest", None),
}

print(f"loading configurations for {project} {version} ...", file=sys.stderr)
