from xmrg_processing.smtp_utils import smtpClass
import logging



def email_results(email_settings, subject, message, attachments):

    try:
        logger = logging.getLogger()
        logger.info(f"Sending email, subject: {subject} to: {email_settings['to_addresses']}")
        # Now send the email.
        email_obj = smtpClass(host=email_settings['host'],
                         user=email_settings['username'],
                         password=email_settings['password'],
                         port=email_settings['port'],
                         use_tls=email_settings['use_tls'])
        email_obj.rcpt_to(email_settings['to_addresses'])
        email_obj.from_addr(email_settings['from_address'])
        for file_to_attach in attachments:
            email_obj.attach(file_to_attach)

        email_obj.subject(subject)
        email_obj.message(message)
        email_obj.send(charset="UTF-8")

        logger.info("Finished sending email")

    except Exception as e:
        logger.exception(e)

    return
