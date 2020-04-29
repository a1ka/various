import requests
from bs4 import BeautifulSoup
import smtplib, ssl
from config import pwd
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from random import randint
from time import sleep


smtp_server = "smtp.gmail.com"
sender_email = "sender@gmail.com"
receiver_email = "receiver@gmail.com"


def download_actual():
    #script runs every one minute hence random 1-9 seconds sleep interval at the beginning
    sleep(randint(1, 9))
    URL = 'https://eshop.scrapeme.xx/'
    page = requests.get(URL)
    soup = BeautifulSoup(page.content, 'html.parser')
    results = soup.find_all('a', class_='look_for_me')

    links = []
    parts = []
    name = []
    for link in results:
        links.append(link.get('href'))
        parts.append(link.get('href').split("/"))
        for part in parts:
            name.append(part[4])

    return links


def compare():
    actual = download_actual()

    f = open("/volume1/homes/user/text_file.txt", "r")
    archive = f.read()
    archive = archive.replace('[', '').replace(']', '').replace('\'', '')
    archive_list = list(archive.split(", "))

    diff = []
    for item in actual:
        if item not in archive_list:
            diff.append(item)

    with open("/volume1/homes/user/text_file.txt", "w") as output:
        output.write(str(actual))

    return diff


def download_first():
    URL = 'https://eshop.scrapeme.xx/'
    page = requests.get(URL)
    soup = BeautifulSoup(page.content, 'html.parser')
    results = soup.find_all('a', class_='look_for_me')

    links = []
    for link in results:
        links.append(link.get('href'))

    with open("/volume1/homes/user/text_file.txt", "w") as output:
        output.write(str(links))

    return links


def email():
    diff = compare()
    message = MIMEMultipart("alternative")
    message["Subject"] = "Eshop ALERT"
    message["From"] = sender_email
    message["To"] = receiver_email

    # Create the plain-text and HTML version of email message
    text = "test"
    if len(diff) > 0:
        for link in diff:
            #html = f"<html><body><p>Linky:<br><a href=\"{link}\">{diff}</a></p></body></html>" Python 3
            #Python 2 syntax used due to python interpreter running on Synology NAS
            html = "<html><body><p>Links:<br><a href=\"%s\">%s</a></p></body></html>" % (link, link)

        # Turn these into plain/html MIMEText objects
        part1 = MIMEText(text, "plain")
        part2 = MIMEText(html, "html")

        # Add HTML/plain-text parts to MIMEMultipart message
        message.attach(part1)
        message.attach(part2)

        # Create secure connection with server and send email
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            server.login(sender_email, pwd)
            server.sendmail(
                sender_email, receiver_email, message.as_string()
            )
    else:
        html = "Nothing new in the eshop."


email()






