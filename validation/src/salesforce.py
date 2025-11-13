import os
import logging
import time
import requests
import jwt
from simple_salesforce import Salesforce
from cryptography.hazmat.primitives import serialization
import base64

def load_private_key_from_env():
    """Load Salesforce private key from environment variable"""
    private_key_content = os.environ.get('SF_PRIVATE_KEY')
    if not private_key_content:
        raise Exception("SF_PRIVATE_KEY environment variable not set")

    if not private_key_content.startswith('-----BEGIN'):
        private_key_content = base64.b64decode(private_key_content).decode('utf-8')

    return serialization.load_pem_private_key(
        private_key_content.encode('utf-8'),
        password=None
    )

def create_jwt_assertion(client_id, username, login_url, private_key):
    """Create JWT assertion for Salesforce"""
    issued_at = int(time.time())
    expiration = issued_at + 300
    payload = {
        'iss': client_id,
        'sub': username,
        'aud': login_url,
        'exp': expiration,
        'iat': issued_at
    }
    return jwt.encode(payload, private_key, algorithm='RS256')

def get_salesforce_access_token(jwt_token, login_url):
    """Get Salesforce access token"""
    token_url = f'{login_url}/services/oauth2/token'
    response = requests.post(token_url, data={
        'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer',
        'assertion': jwt_token
    })
    if response.status_code != 200:
        raise Exception(f"Salesforce token request failed: {response.text}")
    return response.json()

def connect_salesforce():
    """Connect to Salesforce using JWT authentication"""
    logging.info("Connecting to Salesforce using JWT...")
    client_id = os.environ.get('SF_CLIENT_ID')
    username = os.environ.get('SF_USERNAME')
    login_url = os.environ.get('SF_LOGIN_URL', 'https://login.salesforce.com')

    if not client_id or not username:
        raise Exception("SF_CLIENT_ID and SF_USERNAME environment variables must be set")

    private_key = load_private_key_from_env()
    jwt_token = create_jwt_assertion(client_id, username, login_url, private_key)
    response_data = get_salesforce_access_token(jwt_token, login_url)

    if 'access_token' not in response_data or 'instance_url' not in response_data:
        raise Exception(f"Failed to retrieve Salesforce access token: {response_data}")

    logging.info("✅ Connected to Salesforce")
    return Salesforce(instance_url=response_data['instance_url'], session_id=response_data['access_token'])
