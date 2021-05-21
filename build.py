from pybuilder.core import use_plugin, init

use_plugin("python.core")
use_plugin("python.unittest")
use_plugin("python.flake8")
use_plugin("python.distutils")
use_plugin("copy_resources")

name = "Nikhil-Data-Extractor"
version = "1.0.0"
summary = "Nikhil Veeresh Data Extractor"
description = "Takes historical (processed) datasets and prepares them for " \
              "input to modeling"
license = "Nikhil Veeresh"
default_task = ["clean", "analyze", "publish"]


@init
def set_properties(project):
    project.set_property("flake8_break_build", True)
    project.set_property("flake8_include_scripts", True)
    project.set_property("flake8_include_test_sources", True)
    project.set_property("flake8_max_line_length", "99")
    project.set_property("flake8_verbose_output", True)


@init
def add_dependencies(project):
    project.depends_on_requirements("requirements.txt", declaration_only=True)
