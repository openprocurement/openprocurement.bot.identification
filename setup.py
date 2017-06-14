from setuptools import setup, find_packages
import os

version = '1.0'

requires = [
    'setuptools',
]

test_requires = requires + [
    'webtest',
    'python-coveralls',
    'mock==1.0.1',
    'requests_mock==1.3.0',
    'bottle'
]

databridge_requires = requires + [
    'PyYAML',
    'gevent',
    'redis',
    'LazyDB',
    'ExtendedJournalHandler',
    'requests',
    'openprocurement_client>=1.0b2'
]

entry_points = {
    'console_scripts': [
        'bot_identification_bridge = openprocurement.bot.identification.databridge:main'
    ]
}

setup(name='openprocurement.bot.identification',
      version=version,
      description="",
      long_description=open("README.rst").read(),
      classifiers=[
        "Framework :: Pylons",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python",
        "Topic :: Internet :: WWW/HTTP",
        "Topic :: Internet :: WWW/HTTP :: WSGI :: Application"
        ],
      keywords="web services",
      author='Quintagroup, Ltd.',
      author_email='info@quintagroup.com',
      license='Apache License 2.0',
      url='https://github.com/openprocurement/openprocurement.bot.identification',
      packages=find_packages(exclude=['ez_setup']),
      namespace_packages=['openprocurement', 'openprocurement.bot'],
      include_package_data=True,
      zip_safe=False,
      install_requires=requires,
      tests_require=test_requires,
      extras_require={'databridge': databridge_requires,
                      'test': test_requires},
      entry_points=entry_points,
      )
