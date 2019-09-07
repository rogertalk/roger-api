# -*- coding: utf-8 -*-

import logging

import webapp2

from google.appengine.ext.webapp.mail_handlers import InboundMailHandler

from roger import config


class EmailHandler(InboundMailHandler):
    def receive(self, message):
        logging.error('Received a message from %s to %s', message.sender, message.to)
        for content_type, body in message.bodies():
            logging.debug('%s: %r', content_type, body)
            try:
                logging.debug('%r', body.decode())
            except:
                logging.debug('Could not decode body')


app = webapp2.WSGIApplication([EmailHandler.mapping()], debug=config.DEVELOPMENT)
