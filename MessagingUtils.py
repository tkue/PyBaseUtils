import os

from sendgrid import Mail, SendGridAPIClient


class Alert(object):
    pass

class EmailAlert(Alert):
    pass

class SendGridEmailAlert(EmailAlert):
    import os
    from sendgrid import SendGridAPIClient
    from sendgrid.helpers.mail import Mail

    def __init__(self,
                 api_key: str,
                 from_email: str,
                 to_emails: str,
                 subject: str,
                 html_content: str):

        self._api_key = api_key
        self._from_email = from_email
        self._to_emails = to_emails
        self._subject = subject
        self._html_content = html_content

        self.__send_email(self._from_email,
                          self._to_emails,
                          self._subject,
                          self._html_content)

    def __send_email(self,
                     from_email: str,
                     to_email: str,
                     subject: str,
                     html_content: str):

        message = Mail(
            from_email=from_email,
            to_emails=to_email,
            subject=subject,
            html_content=html_content
        )
        try:
            sg = SendGridAPIClient(self._api_key)
            response = sg.send(message)
            print(response.status_code)
            print(response.body)
            print(response.headers)
        except Exception as e:
            print(e.message)

if __name__ == '__main__':
    api_key = ''
    msg = SendGridEmailAlert(api_key=api_key,
                             from_email='tomkuecken@gmail.com',
                             to_emails='tomkuecken@gmail.com',
                             subject='Test',
                             html_content='Testing')
    print(msg)