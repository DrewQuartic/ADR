#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      DDowling
#
# Created:     26/09/2013
# Copyright:   (c) DDowling 2013
# Licence:     <your licence>
#-------------------------------------------------------------------------------
import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email.MIMEText import MIMEText
from email.Utils import COMMASPACE, formatdate
from email import Encoders
def main(send_from, to, subject, text, IsError,  files=[]):
    """Email sending module"""
    recip=[]
    recip = ['drew@quarticsolutions.com'] #['steve@quarticsolutions.com', 'bill@quarticsolutions.com']#, 'timo@quarticsolutions.com'] #'drew@quarticsolutions.com', 'rob@quarticsolutions.com']
##    if (str.upper(IsError) == "TRUE"):
##        recip.append('drewdowling@gmail.com')
##        recip.append('lkebede@gmail.com')
##        recip.append('lkebede@beverlyhills.org')
##        recip.append('drew@quarticsolutions.com')
##    else:
##        recip.append('lkebede@beverlyhills.org')
##        recip.append('person1@beverlyhills.org')
##        recip.append('person2@beverlyhills.org')
##        recip.append('person3@beverlyhills.org')
##
##            smtpObj = smtplib.SMTP('smtp-out.sannet.gov')
##            smtpObj.sendmail(sender,receivers, message)

    #mailServer = smtplib.SMTP("smtp.gmail.com", 587)
    mailServer = smtplib.SMTP('smtp-out.sannet.gov')
    mailServer.ehlo()
    mailServer.starttls()
    mailServer.ehlo()
    msg = MIMEMultipart()
    msg['From'] = send_from
    #msg['To'] = to
    msg['To'] = ",".join(recip)
    msg['Subject'] = subject
    msg.attach( MIMEText(text) )

    for f in files:
        part = MIMEBase('application', "octet-stream")
        part.set_payload( open(f,"rb").read() )
        Encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment; filename="%s"' % os.path.basename(f))
        msg.attach(part)

    # login, send email, logout
    #mailServer.login("drew@QUARTICSOLUTIONS.COM", "M00nb3am")
    mailServer.sendmail(send_from, recip, msg.as_string())
    mailServer.close()

if __name__ == '__main__':
    main()
