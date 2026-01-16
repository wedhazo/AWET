"""Patched auth.py for SuperAGI - fixes Request import and argument order issues."""
from starlette.requests import Request
from fastapi import Depends, HTTPException, Header, Security, status
from fastapi.security import APIKeyHeader
from fastapi_jwt_auth import AuthJWT
from fastapi_sqlalchemy import db
from fastapi.security.api_key import APIKeyHeader
from superagi.config.config import get_config
from superagi.models.organisation import Organisation
from superagi.models.user import User
from superagi.models.api_key import ApiKey
from typing import Optional
from sqlalchemy import or_


def check_auth(Authorize: AuthJWT = Depends()):
    """
    Function to check if the user is authenticated or not based on the environment.
    """
    env = get_config("ENV", "DEV")
    if env == "PROD":
        Authorize.jwt_required()
    return Authorize


def get_user_organisation(Authorize: AuthJWT = Depends(check_auth)):
    """
    Function to get the organisation of the authenticated user based on the environment.
    """
    user = get_current_user(Authorize)
    if user is None:
        raise HTTPException(status_code=401, detail="Unauthenticated")
    organisation = db.session.query(Organisation).filter(Organisation.id == user.organisation_id).first()
    return organisation


def get_current_user(Authorize: AuthJWT = Depends(check_auth), request: Request = None):
    """Get current user - fixed to handle Request properly."""
    env = get_config("ENV", "DEV")

    if env == "DEV":
        email = "super6@agi.com"
    else:
        # Check for HTTP basic auth headers
        if request:
            auth_header = request.headers.get('Authorization')
            if auth_header and auth_header.startswith('Basic '):
                import base64
                auth_decoded = base64.b64decode(auth_header.split(' ')[1]).decode('utf-8')
                username, password = auth_decoded.split(':')
                # Assuming username is the email
                email = username
            else:
                email = Authorize.get_jwt_subject()
        else:
            email = Authorize.get_jwt_subject()

    user = db.session.query(User).filter(User.email == email).first()
    return user


api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def validate_api_key(api_key: str = Security(api_key_header)):
    """Validate API key from header."""
    if api_key is None:
        return None
    # Handle is_expired being NULL or False (both are valid)
    api_key_object = db.session.query(ApiKey).filter(
        ApiKey.key == api_key,
        or_(ApiKey.is_expired == False, ApiKey.is_expired == None)
    ).first()
    if api_key_object is None:
        raise HTTPException(status_code=401, detail="Invalid or expired API key.")
    return api_key_object


def get_organisation_from_api_key(api_key: ApiKey = Depends(validate_api_key)):
    """Get organisation from API key."""
    if api_key is None:
        raise HTTPException(status_code=401, detail="API key required.")
    organisation = db.session.query(Organisation).filter(Organisation.id == api_key.org_id).first()
    return organisation
