import os
from subprocess import PIPE, STDOUT
from unittest import TestCase

from mock import Mock, patch
from nose.tools import eq_, ok_

from shove import Order, Shove

from . import CONTAINS, JSON


def path(*a):
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), *a)


class ShoveTests(TestCase):
    def test_parse_order_invalid_json(self):
        """If the given order isn't valid JSON, return None."""
        shove = Shove({}, Mock())
        eq_(shove.parse_order('{dfinval=3523}}}}]'), None)

    def test_parse_order_missing_key(self):
        """If the given order is missing a required key, return None."""
        shove = Shove({}, Mock())
        eq_(shove.parse_order('{"project": "asdf"}'), None)

    def test_parse_order_valid(self):
        """If the given order is valid, return an Order namedtuple with the correct values."""
        shove = Shove({}, Mock())
        order = shove.parse_order('{"project": "asdf", "command": "qwer", "log_key": 77, '
                                  '"log_queue": "zxcv"}')
        eq_(order.project, 'asdf')
        eq_(order.command, 'qwer')
        eq_(order.log_key, 77)
        eq_(order.log_queue, 'zxcv')

    def test_execute_invalid_project(self):
        """If no project with the given name is found, return an error tuple."""
        shove = Shove({'myproject': '/foo/bar/baz'}, Mock())
        order = Order(project='nonexistant', command='foo', log_key=5, log_queue='asdf')
        eq_(shove.execute(order), (1, 'No project `nonexistant` found.'))

    def test_execute_no_procfile(self):
        """If no procfile is found for the given project, return an error tuple."""
        shove = Shove({'myproject': path('nonexistant')}, Mock())
        order = Order(project='myproject', command='foo', log_key=5, log_queue='asdf')
        eq_(shove.execute(order), (1, CONTAINS('Error loading procfile for project `myproject`')))

    def test_execute_invalid_command(self):
        """If the given command could not be found for the given project, return an error tuple."""
        shove = Shove({'myproject': path('test_project')}, Mock())
        order = Order(project='myproject', command='foo', log_key=5, log_queue='asdf')

        procfile_path = path('test_project', 'bin', 'commands.procfile')
        eq_(shove.execute(order), (1, 'No command `foo` found in {0}'.format(procfile_path)))

    def test_execute_valid_order(self):
        shove = Shove({'myproject': path('test_project')}, Mock())
        order = Order(project='myproject', command='pwd', log_key=5, log_queue='asdf')

        with patch('shove.base.Process') as Process:
            p = Process.return_value
            p.communicate.return_value = 'command output', None
            p.returncode = 0

            return_code, output = shove.execute(order)

        Process.assert_called_with('pwd', cwd=path('test_project'), stdout=PIPE, stderr=STDOUT)
        p.communicate.assert_called_with()
        eq_(return_code, 0)
        eq_(output, 'command output')

    def test_process_order_invalid(self):
        """If parse_order returns None, do not execute the order."""
        shove = Shove({}, Mock())
        shove.parse_order = Mock(return_value=None)
        shove.execute = Mock()

        shove.process_order('{"project": "asdf"}')
        ok_(not shove.execute.called)
        ok_(not shove.adapter.send_log.called)

    def test_process_order_valid(self):
        """If parse_order returns a valid order, execute it and send logs back to Captain."""
        shove = Shove({}, Mock())
        order = Order(project='asdf', command='qwer', log_key=23, log_queue='zxcv')
        shove.parse_order = Mock(return_value=order)
        shove.execute = Mock(return_value=(0, 'output'))

        shove.process_order('{"project": "asdf"}')
        shove.execute.assert_called_with(order)
        shove.adapter.send_log.assert_called_with('zxcv', JSON({
            'version': '1.0',
            'log_key': 23,
            'return_code': 0,
            'output': 'output'
        }))
