"""
This file configures the Python package with entrypoints used for future runs on Databricks

"""

from setuptools import find_packages, setup

setup(
    name="general_funcs",
    packages=find_packages("src"), 
    package_dir={"": "src"},
    setup_requires=["wheel"],
    version= "0.1.1",
    description="",
    author="",
)