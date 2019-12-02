from config import Config
import MySQLdb
import boto3
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import logging


class SqlConnection:

    def __init__(self):
        self.db_config = Config.MYSQL_CONFIG
        try:
            self.connection = MySQLdb.connect(**self.db_config)
            self.cursor = self.connection.cursor(MySQLdb.cursors.DictCursor)
        except Exception as e:
            self.connection.close()

    def reconnect_db(self):
        self.connection = MySQLdb.connect(**self.db_config)
        self.cursor = self.connection.cursor(MySQLdb.cursors.DictCursor)

    def query_db(self, query, params=None, commit=True):
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            if commit:
                self.connection.commit()
        except (AttributeError, MySQLdb.OperationalError):
            self.reconnect_db()
            self.cursor.execute(query)
            if commit:
                self.connection.commit()
        result = self.cursor.fetchall()
        return result

    def query_db_one(self, query, params=None, commit=True):
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            if commit:
                self.connection.commit()
        except (AttributeError, MySQLdb.OperationalError):
            self.reconnect_db()
            self.cursor.execute(query)
            if commit:
                self.connection.commit()
        result = self.cursor.fetchone()
        return dict()

    def write_db(self, query, params=None):
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            self.connection.commit()
        except (AttributeError, MySQLdb.OperationalError):
            self.reconnect_db()
            self.cursor.execute(query)
            self.connection.commit()
        return self.cursor.lastrowid

    def __del__(self):
        self.cursor.close()
        self.connection.close()


class SESEmailer:
    def __init__(self):
        self.msg = MIMEMultipart()
        self.connection = boto3.client('ses', region_name=Config.SES_MAILER_CONFIG.get('region'),
                                       aws_access_key_id=Config.SES_MAILER_CONFIG.get('aws_access_key_id'),
                                       aws_secret_access_key=Config.SES_MAILER_CONFIG.get(
                                           'aws_secret_access_key'))

    def prepare_header(self, receiver, subject):
        self.msg['Subject'] = subject
        self.msg['From'] = Config.SES_MAILER_CONFIG.get("sender")
        self.msg['To'] = ",".join(receiver)

    def add_message_body(self, message):
        part = MIMEText(message)
        self.msg.attach(part)

    def send_email(self, message, receiver, subject):
        try:
            self.prepare_header(receiver, subject)
            self.add_message_body(message)
            mail_status = self.connection.send_raw_email(RawMessage={"Data": self.msg.as_string()},
                                                         Source=self.msg['From'], Destinations=receiver)
            self.msg = MIMEMultipart()
        except Exception as e:
            self.logger.info(
                'Error :  \n occurred for the Email: {}\n'.format(receiver))
