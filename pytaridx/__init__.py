## Written by Francesco DiNatale as part of the DOE/NCI Pilot-2 project.

# SPDX-License-Identifier: MIT

# Turns pytaridx.py into a (proper) python module

#from pytaridx.pytaridx import IndexedTarFile
from .pytaridx import IndexedTarFile

__all__ = ("IndexedTarFile")
