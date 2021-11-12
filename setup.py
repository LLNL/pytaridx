from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(name='pytaridx',
      description='A package for creating, reading from, and writing to indexed tar archives.',
      long_description=long_description,
      long_description_content_type="text/markdown",
      version='1.0.2',
      author='Tomas Oppelstrup',
      author_email='oppelstrup2@llnl.gov',
      # SPDX-License-Identifier: MIT
      license='MIT',
      entry_points={
        'console_scripts': [
            'pytaridx = pytaridx.main:main',
        ]
      },
      ## Put final released URL here:
      url='https://github.com/LLNL/pytaridx',
      packages=find_packages(),
      install_requires=[],
      classifiers=[
        'Development Status :: 4 - Beta',
        "License :: OSI Approved :: MIT License",
        'Intended Audience :: Developers',
        'Operating System :: Unix',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        ],
      )
