from __future__ import print_function
import sys
if sys.version_info < (2, 7):
    import unittest2 as unittest
else:
    import unittest

import contextlib
import copy
import getpass
import json
import os
import shutil
import socket
import stat
import subprocess
import time
import tempfile

from ..configuration import IrodsConfig
from ..controller import IrodsController
from .. import test
from . import session
from . import settings
from .. import lib
from . import resource_suite

class Test_Igroupadmin(resource_suite.ResourceBase, unittest.TestCase):

    def setUp(self):
        super(Test_Igroupadmin, self).setUp()

    def tearDown(self):
        super(Test_Igroupadmin, self).tearDown()

    def test_atg(self):
        self.admin.assert_icommand(['iadmin', 'moduser', self.user_sessions[0].username, 'type', 'groupadmin'])
        try :
            group_name = 'test_group'
            self.user_sessions[0].assert_icommand(['igroupadmin', 'mkgroup', group_name])
            self.user_sessions[0].assert_icommand(['igroupadmin', 'lg'], 'STDOUT_SINGLELINE', group_name)
            self.user_sessions[0].assert_icommand(['igroupadmin', 'lgd', group_name], 'STDOUT_SINGLELINE', 'rodsgroup')
            try :
                self.user_sessions[0].assert_icommand(['igroupadmin', 'atg', group_name, self.user_sessions[0].username])
                self.user_sessions[0].assert_icommand(['igroupadmin', 'atg', group_name, self.user_sessions[1].username])
                self.user_sessions[0].assert_icommand(['igroupadmin', 'lg', group_name], 'STDOUT_SINGLELINE', self.user_sessions[0].username)
                self.user_sessions[0].assert_icommand(['igroupadmin', 'lg', group_name], 'STDOUT_SINGLELINE', self.user_sessions[1].username)
                self.admin.assert_icommand(['iadmin', 'lg', group_name], 'STDOUT_SINGLELINE', self.user_sessions[0].username)
                self.admin.assert_icommand(['iadmin', 'lg', group_name], 'STDOUT_SINGLELINE', self.user_sessions[1].username)
                self.user_sessions[0].assert_icommand(['igroupadmin', 'rfg', group_name, self.user_sessions[1].username])

            finally :
                self.admin.assert_icommand(['iadmin', 'rmgroup', group_name])
        finally :
            self.admin.assert_icommand(['iadmin', 'moduser', self.user_sessions[0].username, 'type', 'rodsuser'])

    def test_group_with_zone_name(self):
        user_session = self.user_sessions[0]
        zone_name = self.user_sessions[0].zone_name
        self.admin.assert_icommand(['iadmin', 'moduser', user_session.username, 'type', 'groupadmin'])
        try :
            group_name = 'test_group'
            fq_group_name = group_name + '#' + zone_name
            user_session.assert_icommand(['igroupadmin', 'mkgroup', fq_group_name])
            user_session.assert_icommand(['igroupadmin', 'lg'], 'STDOUT_SINGLELINE', group_name)
            user_session.assert_icommand(['igroupadmin', 'lgd', group_name], 'STDOUT_SINGLELINE', 'rodsgroup')
            try:
                self.admin.assert_icommand(['iadmin', 'rmgroup', fq_group_name])
                user_session.assert_icommand(['igroupadmin', 'mkgroup', fq_group_name])
            finally:
                self.admin.assert_icommand(['iadmin', 'rmgroup', group_name])
        finally :
            self.admin.assert_icommand(['iadmin', 'moduser', user_session.username, 'type', 'rodsuser'])
