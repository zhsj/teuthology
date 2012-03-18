import os
import smtplib
import socket
import tarfile

from StringIO import StringIO
from email import Encoders
from email.MIMEBase import MIMEBase
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from email.Utils import formatdate
from optparse import OptionParser

def send_email(FROM = 'teuthology@%s' % socket.getfqdn(),
               TO = '',
               SUBJECT = 'Teuthology Report - %s' % formatdate(localtime=True),
               SERVER = 'localhost',
               USER = None,
               PASSWORD = None,
               TEXT = None,
               HTML = None,
               ATTACHMENTS = {},
    ):
    msg = MIMEMultipart()
    msg['From'] = FROM
    msg['To'] = TO
    msg['Subject'] = SUBJECT
    msg['Date'] = formatdate(localtime=True)
    if TEXT is not None:
        msg.attach(MIMEText(TEXT, 'plain'))
    if HTML is not None:
        msg.attach(MIMEText(HTML, 'html'))

    #FIXME something is broken here and attachments are being made empty
    for name, fileobj in ATTACHMENTS.iteritems():
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(fileobj.read())
        Encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment; filename="%s"' % name)
        msg.attach(part)
    server = smtplib.SMTP(SERVER)
    server.ehlo()
    server.starttls()
    server.ehlo()

    try:
        if USER is not None and PASSWORD is not None:
            server.login(USER, PASSWORD)

        result = server.sendmail(FROM, TO, msg.as_string())
        server.close()
    except Exception, e:
        errorMsg = 'Failed to send message.  Error: %s' % str(e)

def create_tgz(inputs):
    fileobj = StringIO()
    tar = tarfile.open(mode='w', fileobj=fileobj)
    for name, data in inputs.iteritems():
        tarobj = StringIO(data)
        tarinfo = tarfile.TarInfo(name='name')
        tarinfo.size = len(data)
        tar.addfile(tarinfo, tarobj)
#    result = fileobj.getvalue()
    tar.close()
    print '-------- fileobj ---------'
    print fileobj.len
    print fileobj
    return fileobj

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option('--to', dest='TO', help='Email Recipient')
    parser.add_option('--server', dest='SERVER', help='SMTP Server')
    parser.add_option('--user', dest='USER', help='SMTP User')
    parser.add_option('--password', dest='PASSWORD', help='SMTP Password')
    parser.add_option('--text', dest='TEXT', help='Text message to send')
    parser.add_option('--html', dest='HTML', help='HTML message to send')

    (opts, args) = parser.parse_args()
    if opts.TO is None:
        print 'Please specify a recipient email address.'
    send_email(TO=opts.TO, 
               SERVER=opts.SERVER, 
               USER=opts.USER,
               PASSWORD=opts.PASSWORD,
               TEXT=opts.TEXT,
               HTML=opts.HTML,
        )

