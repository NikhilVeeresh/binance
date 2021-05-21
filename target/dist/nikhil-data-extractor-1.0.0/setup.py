#!/usr/bin/env python
#   -*- coding: utf-8 -*-

from setuptools import setup
from setuptools.command.install import install as _install

class install(_install):
    def pre_install_script(self):
        pass

    def post_install_script(self):
        pass

    def run(self):
        self.pre_install_script()

        _install.run(self)

        self.post_install_script()

if __name__ == '__main__':
    setup(
        name = 'Nikhil-Data-Extractor',
        version = '1.0.0',
        description = 'Nikhil Veeresh Data Extractor',
        long_description = 'Takes historical (processed) datasets and prepares them for input to modeling',
        long_description_content_type = None,
        classifiers = [
            'Development Status :: 3 - Alpha',
            'Programming Language :: Python'
        ],
        keywords = '',

        author = '',
        author_email = '',
        maintainer = '',
        maintainer_email = '',

        license = 'Nikhil Veeresh',

        url = '',
        project_urls = {},

        scripts = ['scripts/config.py'],
        packages = [],
        namespace_packages = [],
        py_modules = [],
        entry_points = {},
        data_files = [],
        package_data = {},
        install_requires = [
            'pybuilder>=0.12.10',
            'pandas>=1.2.4',
            'binance>=0.3'
        ],
        dependency_links = [],
        zip_safe = True,
        cmdclass = {'install': install},
        python_requires = '',
        obsoletes = [],
    )
