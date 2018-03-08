from __future__ import (absolute_import, division, print_function)
import versioneer

import setuptools

no_git_reqs = []
with open('requirements.txt') as f:
    required = f.read().splitlines()
    for r in required:
        if not (r.startswith('git') or r.startswith('#') or r.strip() == ''):
            no_git_reqs.append(r)

setuptools.setup(
    name='qastools',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    license="BSD (3-clause)",
    url="https://github.com/NSLS-II-QAS/qastools",
    packages=setuptools.find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
    ],
    install_requires=no_git_reqs,
)
