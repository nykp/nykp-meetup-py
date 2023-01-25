import os

SECRETS_DIR = os.path.abspath(os.path.join(__file__, '../../../.secrets'))
MEETUP_AUTH_TOKEN_FILE = os.path.join(SECRETS_DIR, 'meetup_authorization_token')


def get_meetup_auth_token():
    with open(MEETUP_AUTH_TOKEN_FILE) as f:
        return f.readlines()[0].strip()
