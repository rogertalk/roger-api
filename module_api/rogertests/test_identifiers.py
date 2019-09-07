# -*- coding: utf-8 -*-

import unittest

from roger_common import errors, identifiers
import rogertests


class BaseTestCase(rogertests.RogerTestCase):
    def setUp(self):
        super(BaseTestCase, self).setUp()


class ParserTester9000(BaseTestCase):
    def test_parse_invalid(self):
        self.assertRaises(errors.InvalidArgument, identifiers.parse, '')
        self.assertRaises(errors.InvalidArgument, identifiers.parse, None)
        self.assertRaises(errors.InvalidArgument, identifiers.parse, '@a.c!')
        self.assertRaises(errors.InvalidArgument, identifiers.parse, '@a.com!')
        self.assertRaises(errors.InvalidArgument, identifiers.parse, '@abc.com!')
        self.assertRaises(errors.InvalidArgument, identifiers.parse, 'Abc.example.com!')
        self.assertRaises(errors.InvalidArgument, identifiers.parse, 'A@b@c@example.com')
        self.assertRaises(errors.InvalidArgument, identifiers.parse, 'admin@mailserver1')
        self.assertRaises(errors.InvalidArgument, identifiers.parse, '+a')
        self.assertRaises(errors.InvalidArgument, identifiers.parse, '-a')
        self.assertRaises(errors.InvalidArgument, identifiers.parse, '_a')
        self.assertRaises(errors.InvalidArgument, identifiers.parse, 'a+')
        self.assertRaises(errors.InvalidArgument, identifiers.parse, 'a#')
        self.assertRaises(errors.InvalidArgument, identifiers.parse, 'a@')
        self.assertRaises(errors.InvalidArgument, identifiers.parse, '+abc')
        self.assertRaises(errors.InvalidArgument, identifiers.parse, '+12345')
        self.assertRaises(errors.InvalidArgument, identifiers.parse, '+aaa12345')
        self.assertRaises(errors.InvalidArgument, identifiers.parse, '+12345aaa')
        self.assertRaises(errors.InvalidArgument, identifiers.parse, '+1(23)45')
        self.assertRaises(errors.InvalidArgument, identifiers.parse, '!@#$%^')
        self.assertRaises(errors.InvalidArgument, identifiers.parse, 'cação')
        self.assertRaises(errors.InvalidArgument, identifiers.parse, '123abc')
        self.assertRaises(errors.InvalidArgument, identifiers.parse, '2pac')
        self.assertRaises(errors.InvalidArgument, identifiers.parse, '阿里巴巴集团')
        self.assertRaises(errors.InvalidArgument, identifiers.parse, 'กรุงเทพมหานคร')
        self.assertRaises(errors.InvalidArgument, identifiers.parse, '❤💔💗💓💕💖💞💘💌💋💍💎👤👥💬👣💭')
        self.assertRaises(errors.InvalidArgument, identifiers.parse, '✂✒✏')

    def test_parse_account_id(self):
        self.assertEquals((1, 'account_id'), identifiers.parse(1))
        self.assertEquals((123, 'account_id'), identifiers.parse(123))
        self.assertEquals((1, 'account_id'), identifiers.parse('1'))
        self.assertEquals((111, 'account_id'), identifiers.parse('111'))
        self.assertEquals((111, 'account_id'), identifiers.parse(' 111'))
        self.assertEquals((111, 'account_id'), identifiers.parse(' 111'))
        self.assertEquals((111, 'account_id'), identifiers.parse('111 '))
        self.assertEquals((111, 'account_id'), identifiers.parse(' 111 '))
        self.assertEquals((123456789012345678901234567890123456789012345678901234567890, 'account_id'), identifiers.parse('123456789012345678901234567890123456789012345678901234567890'))

    def test_parse_channel(self):
        self.assertEquals(('#yolo', 'channel'), identifiers.parse('#yolo'))
        self.assertEquals(('#123', 'channel'), identifiers.parse('#123'))
        self.assertEquals(('#2pac', 'channel'), identifiers.parse('#2pac'))
        self.assertEquals(('#pac2', 'channel'), identifiers.parse('#pac2'))
        self.assertEquals(('#2', 'channel'), identifiers.parse('#2'))
        self.assertEquals(('#a', 'channel'), identifiers.parse('#a'))
        self.assertEquals(('#+123', 'channel'), identifiers.parse('#+123'))
        self.assertEquals(('#+1239415487', 'channel'), identifiers.parse('#+1239415487'))
        self.assertEquals(('#email@domain.com', 'channel'), identifiers.parse('#email@domain.com'))
        self.assertEquals(('#✂✒✏', 'channel'), identifiers.parse('#✂✒✏'))
        self.assertEquals(('#@!-k', 'channel'), identifiers.parse('#@!-k'))
        self.assertEquals(('#阿里巴巴集团', 'channel'), identifiers.parse('#阿里巴巴集团'))
        self.assertEquals(('#❤💔💗💓💕💖💞💘💌💋💍💎👤👥💬👣💭', 'channel'), identifiers.parse('#❤💔💗💓💕💖💞💘💌💋💍💎👤👥💬👣💭'))

    def test_parse_email(self):
        self.assertEquals(('email:example.com/niceandsimple', 'email'), identifiers.parse('niceandsimple@example.com'))
        self.assertEquals(('email:example.com/very.common', 'email'), identifiers.parse('very.common@example.com'))
        self.assertEquals(('email:dept.example.com/a.little.lengthy.but.fine', 'email'), identifiers.parse('a.little.lengthy.but.fine@dept.example.com'))
        self.assertEquals(('email:example.com/disposable.style.email.with%2Bsymbol', 'email'), identifiers.parse('disposable.style.email.with+symbol@example.com'))
        self.assertEquals(('email:example.com/other.email-with-dash', 'email'), identifiers.parse('other.email-with-dash@example.com'))
        self.assertEquals(('email:example.com/%22much.moreunusual%22', 'email'), identifiers.parse('"much.more unusual"@example.com'))
        self.assertEquals(('email:example.com/%22very.unusual.%40.unusual.com%22', 'email'), identifiers.parse('"very.unusual.@.unusual.com"@example.com'))
        self.assertEquals(('email:strange.example.com/%22very.%28%29%2C%3A%3B%3C%3E%5B%5D%22.very.%22very%40%5C%22very%22.unusual%22', 'email'), identifiers.parse('"very.(),:;<>[]\".VERY.\"very@\\ \"very\".unusual"@strange.example.com'))
        self.assertEquals(('email:example.org/%21%23%24%25%26%27%2A%2B-%2F%3D%3F%5E_%60%7B%7D%7C%7E', 'email'), identifiers.parse('!#$%&\'*+-/=?^_`{}|~@example.org'))
        self.assertEquals(('email:example.org/%22%28%29%3C%3E%5B%5D%3A%2C%3B%40%5C%22%21%23%24%25%26%27%2A%2B-%2F%3D%3F%5E_%60%7B%7D%7C%7E.a%22', 'email'), identifiers.parse('"()<>[]:,;@\\\"!#$%&\'*+-/=?^_`{}| ~.a"@example.org'))
        self.assertEquals(('email:example.org/%22%22', 'email'), identifiers.parse('" "@example.org'))
        self.assertEquals(('email:example.com/%C3%BC%C3%B1%C3%AE%C3%A7%C3%B8%C3%B0%C3%A9', 'email'), identifiers.parse('üñîçøðé@example.com'))
        self.assertEquals(('email:%C3%BC%C3%B1%C3%AE%C3%A7%C3%B8%C3%B0%C3%A9.com/%C3%BC%C3%B1%C3%AE%C3%A7%C3%B8%C3%B0%C3%A9', 'email'), identifiers.parse('üñîçøðé@üñîçøðé.com'))
        self.assertEquals(('email:b.c/a', 'email'), identifiers.parse('a@b.c'))
        self.assertEquals(('email:2.3/1', 'email'), identifiers.parse('1@2.3'))
        self.assertEquals(('email:b.c/a', 'email'), identifiers.parse(' a@b.c '))
        self.assertEquals(('email:b.c/a', 'email'), identifiers.parse(' a@B.C '))
        self.assertEquals(('email:b.com/a%2B1', 'email'), identifiers.parse('a+1@b.com'))
        self.assertEquals(('email:weird-long-host.com.br/this-is-a-really-long-email-with%2Bfilters', 'email'), identifiers.parse('this-is-a-really-long-email-with+FILTERS@weird-long-host.com.br'))

    def test_parse_email_service(self):
        self.assertEquals(('email:b.c/a', 'email'), identifiers.parse(' email:B.C/A '))
        self.assertEquals(('email:b.c/a%2Bb%2Bxyz', 'email'), identifiers.parse(' email:B.C/A%2Bb%2BXYZ '))

    @unittest.skip('not implemented yet')
    def test_parse_email_invalid(self):
        # FIXME - all bellow should be invalid emails
        # none of the special characters in this local part is allowed outside quotation marks
        self.assertRaises(errors.InvalidArgument, identifiers.parse, 'a"b(c)d,e:f;g<h>i[j\k]l@example.com')
        # quoted strings must be dot separated or the only element making up the local-part
        self.assertRaises(errors.InvalidArgument, identifiers.parse, 'just"not"right@example.com')
        # spaces, quotes, and backslashes may only exist when within quoted strings and preceded by a backslash
        self.assertRaises(errors.InvalidArgument, identifiers.parse, 'this is"not\allowed@example.com')
        # even if escaped (preceded by a backslash), spaces, quotes, and backslashes must still be contained by quotes
        self.assertRaises(errors.InvalidArgument, identifiers.parse, 'this\ still\"not\\allowed@example.com')
        # double dot before @
        self.assertRaises(errors.InvalidArgument, identifiers.parse, 'john..doe@example.com')
        # double dot after @
        self.assertRaises(errors.InvalidArgument, identifiers.parse, 'john.doe@example..com')

    def test_parse_phone(self):
        self.assertEquals(('+123456', 'phone'), identifiers.parse('+123456'))
        self.assertEquals(('+123456', 'phone'), identifiers.parse('+1 2 3 4 5 6'))
        self.assertEquals(('+1234567890', 'phone'), identifiers.parse('+1 (234) 56-7890'))
        self.assertEquals(('+123456789012345678901234567890', 'phone'), identifiers.parse('+123456789012345678901234567890'))
        self.assertEquals(('+123456789012345678901234567890', 'phone'), identifiers.parse('+1 (234) 56-789012345678901234567890'))
        self.assertEquals(('+123456', 'phone'), identifiers.parse('+123456abc'))
        self.assertEquals(('+123456', 'phone'), identifiers.parse('+123abc456'))
        self.assertEquals(('+1239415487', 'phone'), identifiers.parse('++1239415487'))
        self.assertEquals(('+1239415487', 'phone'), identifiers.parse('+#1239415487'))
        self.assertEquals(('+1239415487', 'phone'), identifiers.parse('+@1239415487'))
        self.assertEquals(('+123456', 'phone'), identifiers.parse('+e123mail@d4o5m6ain.com'))
        self.assertEquals(('+123456', 'phone'), identifiers.parse('+✂1✒2✏3 4 56 '))
        self.assertEquals(('+123456', 'phone'), identifiers.parse('+12@34!5-6k'))
        self.assertEquals(('+123456', 'phone'), identifiers.parse('+阿里123456巴巴集团'))
        self.assertEquals(('+123456', 'phone'), identifiers.parse('+❤1💔2💗3💓4💕5💖6💞💘💌💋💍💎👤👥💬👣💭'))

    def test_parse_services(self):
        self.assertEquals(('ikea:%22Stuff%22/Hello+There', 'service_id'), identifiers.parse(' Ikea :%22Stuff%22/Hello%20There '))
        self.assertEquals(('ikea:%22Stuff%22/Hello+There', 'service_id'), identifiers.parse('ikea:"Stuff"/Hello+There'))
        self.assertEquals(('spotify:BOB', 'service_id'), identifiers.parse('SPOTIFY:BOB'))

    def test_parse_username(self):
        self.assertEquals(('a', 'username'), identifiers.parse('a'))
        self.assertEquals(('ab', 'username'), identifiers.parse('ab'))
        self.assertEquals(('a1', 'username'), identifiers.parse('a1'))
        self.assertEquals(('a1b', 'username'), identifiers.parse('a1b'))
        self.assertEquals(('a1b0', 'username'), identifiers.parse('a1b0'))
        self.assertEquals(('a1b0', 'username'), identifiers.parse('a1B0'))
        self.assertEquals(('a_1b-0', 'username'), identifiers.parse('a_1B-0'))
        self.assertEquals(('a_1b-0', 'username'), identifiers.parse('a_1B-0 '))
        self.assertEquals(('z1234567890z1234567890z1234567890z1234567890z1234567890', 'username'), identifiers.parse('z1234567890z1234567890z1234567890z1234567890z1234567890'))
