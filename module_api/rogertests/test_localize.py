# -*- coding: utf-8 -*-

import mock

from roger import localize
import rogertests


class BaseTestCase(rogertests.RogerTestCase):
    def setUp(self):
        super(BaseTestCase, self).setUp()


class Strings(BaseTestCase):
    def test_get_call_en(self):
        code = '233'
        receiver = '+1242323424'
        text = localize.get_string('call.challenge_code', args={'code': code}, receiver=receiver)
        self.assertIn(code, text)
        self.assertIn(u'code', text)
        # all non-localized countries get en-us
        code = '233'
        receiver = '+7242323424'
        text = localize.get_string('call.challenge_code', args={'code': code}, receiver=receiver)
        self.assertIn(code, text)
        self.assertIn('code', text)

    def test_get_call_es(self):
        code = '233'
        receiver = '+342342323424'
        text = localize.get_string('call.challenge_code', args={'code': code}, receiver=receiver)
        self.assertIn(code, text)
        self.assertIn(u'verificación', text)

    def test_get_call_pt(self):
        code = '233'
        receiver = '+5542323424'
        text = localize.get_string('call.challenge_code', args={'code': code}, receiver=receiver)
        self.assertIn(code, text)
        self.assertIn(u'verificação', text)

    @mock.patch('roger.localize._get_country')
    def test_get_email_en(self, get_country_mock):
        get_country_mock.return_value = 'US'
        code = '233'
        receiver = 'roguer@murica.com'
        subject = localize.get_string('email.challenge_code.subject', args={'code': code}, receiver=receiver)
        self.assertIn(code, subject)
        self.assertIn(u'verification', subject)
        body = localize.get_string('email.challenge_code.subject', args={'code': code}, receiver=receiver)
        self.assertIn(code, body)
        # all non-localized countries get en-us
        get_country_mock.return_value = 'CN'
        code = '233'
        receiver = 'roguer@murica.com'
        subject = localize.get_string('email.challenge_code.subject', args={'code': code}, receiver=receiver)
        self.assertIn(code, subject)
        self.assertIn(u'verification', subject)
        body = localize.get_string('email.challenge_code.subject', args={'code': code}, receiver=receiver)
        self.assertIn(code, body)

    @mock.patch('roger.localize._get_country')
    def test_get_email_es(self, get_country_mock):
        get_country_mock.return_value = 'MX'
        code = '233'
        receiver = 'el_roger@tacos.mx'
        subject = localize.get_string('email.challenge_code.subject', args={'code': code}, receiver=receiver)
        self.assertIn(code, subject)
        self.assertIn(u'verificación', subject)
        body = localize.get_string('email.challenge_code.subject', args={'code': code}, receiver=receiver)
        self.assertIn(code, body)

    @mock.patch('roger.localize._get_country')
    def test_get_email_pt(self, get_country_mock):
        get_country_mock.return_value = 'BR'
        code = '233'
        receiver = 'o_velho_rogerr@galera.com.br'
        subject = localize.get_string('email.challenge_code.subject', args={'code': code}, receiver=receiver)
        self.assertIn(code, subject)
        self.assertIn(u'verificação', subject)
        body = localize.get_string('email.challenge_code.subject', args={'code': code}, receiver=receiver)
        self.assertIn(code, body)

    def test_get_sms_en(self):
        code = '233'
        receiver = '+1242323424'
        text = localize.get_string('sms.challenge_code', args={'code': code}, receiver=receiver)
        self.assertIn(code, text)
        self.assertIn(u'verification', text)
        # all non-localized countries get en-us
        code = '233'
        receiver = '+7242323424'
        text = localize.get_string('sms.challenge_code', args={'code': code}, receiver=receiver)
        self.assertIn(code, text)
        self.assertIn(u'verification', text)

    def test_get_sms_es(self):
        code = '233'
        receiver = '+342342323424'
        text = localize.get_string('sms.challenge_code', args={'code': code}, receiver=receiver)
        self.assertIn(code, text)
        self.assertIn(u'verificación', text)

    def test_get_sms_pt(self):
        code = '233'
        receiver = '+5542323424'
        text = localize.get_string('sms.challenge_code', args={'code': code}, receiver=receiver)
        self.assertIn(code, text)
        self.assertIn(u'verificação', text)
