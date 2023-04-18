import token_ap.config as config
import datetime
import json
import logging
import os

from token_ap.database import db
from flask import Blueprint, Flask, app
from globus_action_provider_tools import (
    ActionProviderDescription,
    ActionRequest,
    ActionStatus,
    ActionStatusValue,
    AuthState,
)
from globus_action_provider_tools.authorization import (
    authorize_action_access_or_404,
    authorize_action_management_or_404
)
from globus_action_provider_tools.flask import add_action_routes_to_blueprint
from globus_action_provider_tools.flask.exceptions import ActionConflict
from globus_action_provider_tools.flask.types import ActionCallbackReturn

app = Flask(__name__)


def load_schema():
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "schema.json")) as f:
        schema = json.load(f)
    return schema


def auth_to_userinfo(auth: AuthState):
    token_info = auth.introspect_token().data
    client = dict()
    attributes = list()
    userinfo = dict()

    client["id"] = token_info["iss"] + "/" + token_info["sub"]
    mapping = {"display_name": "username",
               "full_name": "name",
               "email": "email",
               "identities": "identity_set"}
 
    for k,v in mapping.items():
        client[k] = token_info[v]

    groups_client = auth._get_groups_client()
    groups = groups_client.list_groups()
    for group in groups:
        attributes.append({"id": token_info["iss"] + "/" + group['id'],
                           "display_name": group.get("name")})
    attributes.append(client)
    userinfo.update({'client': client})
    userinfo.update({'attributes': attributes})

    # return json.dumps(userinfo)
    return userinfo


def action_run(request: ActionRequest, auth: AuthState) -> ActionCallbackReturn:
    action_id = db.query(request.request_id)
    caller_id = auth.effective_identity

    if action_id is not None:
        return action_status(action_id, auth)

    userinfo = auth_to_userinfo(auth)
    result_details = {"userinfo": userinfo}

    status = ActionStatus(
        status=ActionStatusValue.SUCCEEDED,
        creator_id=caller_id or "UNKNOWN",
        monitor_by=request.monitor_by,
        manage_by=request.manage_by,
        start_time=str(datetime.datetime.now().isoformat()),
        completion_time=str(datetime.datetime.now().isoformat()),
        release_after=request.release_after or "P30D",
        display_status=ActionStatusValue.SUCCEEDED,
        details=result_details,
    )
    db.persist(request.request_id, status.action_id)
    db.persist(status.action_id, status)
    return status


def action_status(action_id: str, auth: AuthState) -> ActionCallbackReturn:
    status = db.query(action_id)
    authorize_action_access_or_404(status, auth)
    return status, 200


def action_cancel(action_id: str, auth: AuthState) -> ActionCallbackReturn:
    status = db.query(action_id)
    authorize_action_management_or_404(status, auth)

    # If action is already in complete state, return completion details
    if status.status in (ActionStatusValue.SUCCEEDED, ActionStatusValue.FAILED):
        return status

    # Process Action cancellation
    status.status = ActionStatusValue.FAILED
    status.display_status = "Canceled by user request"
    return status


def action_release(action_id: str, auth: AuthState) -> ActionCallbackReturn:
    status = db.query(action_id)
    authorize_action_management_or_404(status, auth)

    # Error if attempt to release an active Action
    if status.status not in (ActionStatusValue.SUCCEEDED, ActionStatusValue.FAILED):
        raise ActionConflict("Action is not complete")

    db.delete(action_id)
    return status, 200


def create_app():
    app.logger.setLevel(logging.DEBUG)
    app.url_map.strict_slashes = False

    # Create and define a blueprint onto which the routes will be added
    skeleton_blueprint = Blueprint("token", __name__, url_prefix="/token")

    # Create the ActionProviderDescription with the correct scope and schema
    provider_description = ActionProviderDescription(
        globus_auth_scope=config.scope,
        title="Token Passthru Action Provider",
        admin_contact=config.admin_contact,
        synchronous=config.synchronous,
        input_schema=load_schema(),
        log_supported=False,  # This provider doesn't implement the log callback
        visible_to=["public"],
        executable_by=["all_authenticated_users"],
        administered_by=["david@globus.org"],
    )

    # Use the flask helper function to register the endpoint callbacks onto the
    # blueprint
    add_action_routes_to_blueprint(
        blueprint=skeleton_blueprint,
        client_id=config.client_id,
        client_secret=config.client_secret,
        client_name=None,
        provider_description=provider_description,
        action_run_callback=action_run,
        action_status_callback=action_status,
        action_cancel_callback=action_cancel,
        action_release_callback=action_release,
        action_enumeration_callback=None,
        additional_scopes=config.additional_scopes,
    )

    # Register the blueprint with your flask app before returning it
    app.register_blueprint(skeleton_blueprint)
    return app


def main():
    app = create_app()
    app.run(debug=True, port=5001, threaded=False)


if __name__ == "__main__":
    main()
