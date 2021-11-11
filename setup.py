from setuptools import setup, find_packages

setup(name='pytaridx',
      description='A package for creating, reading from, and wrting to indexed tar archives.',
      version='1.0.0',
      author=[
        'Tomas Oppelstrup',
        'Francesco Di Natale',
      ],
      author_email=[
        'oppelstrup2@llnl.gov',
        'dinatale3@llnl.gov',
      ],
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
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Operating System :: Unix',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        ],
      )
