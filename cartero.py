#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Import smtplib for the actual sending function
import smtplib

# Import the email modules we'll need
from email.mime.text import MIMEText
from  config import USR_CONTROL_MAIL

# Defining a class
class Cartero:


    def __init__(self):
        pass
        
    def sendMail(self,mensaje):
        # Send the message via our own SMTP server, but don't include the
        # envelope header.
        xme = "username@example.com"
        xyou = USR_CONTROL_MAIL
        msg = MIMEText(mensaje)
        msg['Subject'] = '[matomoSNRD]'
        msg['From'] = xme
        msg['To'] = xyou

        
        s = smtplib.SMTP('localhost')
        s.set_debuglevel(1)
        s.sendmail(xme, [xyou], msg.as_string())
        s.quit()