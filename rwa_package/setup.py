"""Minimal setup file for tasks project."""

from setuptools import setup, find_packages

setup(
    name='rwa',
    version='0.1.0',
    license='proprietary',
    description='Project RWA Calculation',

    author='Adam Szpilewicz',
    author_email='adam.szpilewicz@gmail.com',
    url='https://github.com/adamszpilewicz/job/edit/master/rwa_package/',

#     packages=find_packages(where='common'),
#     package_dir={'': 'common'},

   # install_requires=['click', 'tinydb', 'six'],
   #extras_require={'mongo': 'pymongo'},

    # entry_points={
    #     'console_scripts': [
    #         'tasks = tasks.cli:tasks_cli',
    #     ]
    # },
)
