from setuptools import setup, find_packages
import os

version = '0.0.1'

setup(name='django-simpledb',
      version=version,
      description="Lazy signup for Django",
      long_description=open("README.rst").read() + "\n" +
                       open(os.path.join("docs", "HISTORY.rst")).read(),
      # Get more strings from http://www.python.org/pypi?%3Aaction=list_classifiers
      classifiers=[
        "Programming Language :: Python",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Framework :: Django",
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: BSD License"
        ],
      keywords='django nonrel nosql simpledb amazon',
      author='Dan Fairs',
      author_email='dan@fezconsulting.com',
      url='http://github.com/danfairs/django-simpledb',
      license='BSD',
      packages=find_packages(exclude=['ez_setup']),
      namespace_packages=[],
      include_package_data=True,
      zip_safe=False,
      install_requires=[
          'setuptools',
          #'Django', nonrel
      ],
      entry_points="""
# -*- Entry points: -*-
""",
      )