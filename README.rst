========================================
Shove - It follows the Captain's orders.
========================================

Shove is the second half of `Captain Shove`_. It listens for commands from the
Captain frontend and executes them.

.. _Captain Shove: https://wiki.mozilla.org/Websites/Captain_Shove

Project details
===============

:Code:          https://github.com/mozilla/shove
:Documentation: Ha!
:Issue tracker: https://github.com/mozilla/shove/issues
:IRC:           ``#capshove`` on irc.mozilla.org
:License:       Mozilla Public License v2


To hack on Shove
================

Required:

* pip
* virtualenv
* python: 2.6 or 2.7

Steps:

1. ``git clone https://github.com/mozilla/shove``
2. ``cd shove``
3. ``virtualenv venv``
4. ``source venv/bin/activate``
5. ``python setup.py develop``
6. ``cp shove/settings.py-dist shove/settings.py``
7. Edit ``shove/settings.py``. The comments tell you what
   you need to change.
8. Set up rabbitmq and fill in the details in ``shove/settings.py``.
9. ``shove``

That'll launch shove, but it won't really do anything until you start
passing it orders. You pass it orders using Captain.


To test
=======

After cloning and setting up a virtualenv using the steps above:

1. ``pip install -r requirements.txt``
2. ``python setup.py nosetests``

Tests are located in the ``tests`` subfolder.
