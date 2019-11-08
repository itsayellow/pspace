# setup for tictoc package

import os.path
from setuptools import setup

here = os.path.abspath(os.path.dirname(__file__))

setup(
    name="pspace",
    version="0.1",
    description="Support for running jobs on Paperspace cloud computing",
    author="Matthew Clapp",
    author_email="itsayellow+dev@gmail.com",
    license="MIT",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
    ],
    keywords="cloud paperspace",
    url="https://github.com/itsayellow/pspace",
    packages=["pspace"],
    install_requires=["paperspace<0.2.0", "pyyaml"],
    entry_points={"console_scripts": ["pspace=pspace.cli:cli"]},
    python_requires=">=3",
)
