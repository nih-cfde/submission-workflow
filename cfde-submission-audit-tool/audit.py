#!/usr/bin/env python3
import argparse
import configs
import json
import os
import globus_sdk
import pandas as pd
import requests
from cfde_deriva.registry import Registry
from globus_sdk import GlobusError
from globus_sdk.scopes import TransferScopes, GroupsScopes, AuthScopes
from globus_sdk.tokenstorage import SimpleJSONFileAdapter


def get_deriva_acls(deriva_server, deriva_token):
    credentials = {"bearer-token": deriva_token["access_token"]}
    registry = Registry('https', deriva_server, credentials=credentials)
    dccs = [dcc_info["id"] for dcc_info in registry.get_dcc()]
    roles = [role_info["id"] for role_info in registry.get_group_role()]
    acl_data = dict()

    for dcc in dccs:
        dcc_short = dcc.split(":")[1]
        acl_data[dcc_short] = dict()
        for role in roles:
            role_short = role.split(":")[1]
            role_id_list = registry.get_dcc_acl(dcc, role)
            acl_data[dcc_short][role_short] = role_id_list
    return acl_data


def get_deriva_groups(deriva_server):
    server = f'https://{deriva_server}/ermrest/catalog/registry/attributegroup/group/id;name'
    response = requests.get(server).text
    groups_list = json.loads(response)
    groups_dict = dict()
    for group in groups_list:
        groups_dict[group['id']] = group['name']
    return groups_dict


def get_globus_groups(groups_token, group_ids):
    groups_authorizer = globus_sdk.AccessTokenAuthorizer(groups_token["access_token"])
    groups_client = globus_sdk.GroupsClient(authorizer=groups_authorizer)
    groups_dict = dict()
    for group in group_ids:
        try:
            group_name = groups_client.get_group(group_id=group)["name"]
            groups_dict[group] = group_name
        except GlobusError:
            pass
    return groups_dict


def get_globus_users(auth_token, user_ids):
    auth_authorizer = globus_sdk.AccessTokenAuthorizer(auth_token["access_token"])
    auth_client = globus_sdk.AuthClient(authorizer=auth_authorizer)
    users_dict = dict()
    results = auth_client.get_identities(ids=user_ids)
    for result in results:
        try:
            if result['email']:
                users_dict[result["id"]] = f"{result['name']} ({result['email']})"
            else:
                users_dict[result["id"]] = result['name']
        except KeyError:
            continue
    return users_dict


def get_collection_acl(guest_collection, transfer_token):
    authorizer = globus_sdk.AccessTokenAuthorizer(transfer_token["access_token"])
    transfer_client = globus_sdk.TransferClient(authorizer=authorizer)
    acls = transfer_client.endpoint_acl_list(guest_collection)
    df = pd.DataFrame(acls)
    df["Principal Name"] = df["principal"]
    df = df[['path', 'permissions', 'principal_type', 'Principal Name', 'principal', 'role_type',
             'create_time', 'id']]
    df = df.rename(columns={'path': 'Path',
                            'permissions': 'Permissions',
                            'principal_type': 'Principal Type',
                            'principal': 'Principal ID',
                            'role_type': 'Role',
                            'create_time': 'Create Time',
                            'id': 'Rule ID'})
    return df


def login():
    client_id = "2621f896-1451-4e2f-a661-1f773ed47d94"
    deriva_scope = "https://auth.globus.org/scopes/app.nih-cfde.org/deriva_all"
    deriva_resource_server = "0bf44295-88e9-4362-9c80-a5ec72a85b74"
    scopes = [TransferScopes.all, GroupsScopes.all, AuthScopes.profile, AuthScopes.email,
              AuthScopes.openid, deriva_scope]
    token_file = SimpleJSONFileAdapter(os.path.expanduser("~/.cfde-submission-audit-tool.json"))

    if token_file.file_exists():
        transfer_token = token_file.get_token_data(TransferScopes.resource_server)
        deriva_token = token_file.get_token_data(deriva_resource_server)
        groups_token = token_file.get_token_data(GroupsScopes.resource_server)
        auth_token = token_file.get_token_data(AuthScopes.resource_server)
    else:
        client = globus_sdk.NativeAppAuthClient(client_id)
        client.oauth2_start_flow(requested_scopes=scopes)
        authorize_url = client.oauth2_get_authorize_url()
        print(f"Please go to this URL and login:\n\n{authorize_url}\n")
        auth_code = input("Please enter the code you get after login here: ").strip()
        token_response = client.oauth2_exchange_code_for_tokens(auth_code)
        token_file.store(token_response)
        by_rs = token_response.by_resource_server
        transfer_token = by_rs[TransferScopes.resource_server]
        groups_token = by_rs[GroupsScopes.resource_server]
        auth_token = by_rs[AuthScopes.resource_server]
        deriva_token = by_rs[deriva_resource_server]
    return transfer_token, groups_token, auth_token, deriva_token


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", help="Filename for xls output", required=True)
    args = parser.parse_args()
    transfer_token, groups_token, auth_token, deriva_token = login()
    sheets = dict()

    for environment in ["dev", "staging", "prod"]:
        config = configs.environments[environment]

        # Collection ACLs
        collection_acls = get_collection_acl(config["guest_collection"], transfer_token)

        # Populate users from globus auth
        globus_users = get_globus_users(auth_token, set(collection_acls["Principal Name"]))
        collection_acls["Principal Name"] = collection_acls["Principal Name"].replace(globus_users)

        # Populate groups from globus groups
        globus_groups = get_globus_groups(groups_token, set(collection_acls["Principal Name"]))
        collection_acls["Principal Name"] = collection_acls["Principal Name"].replace(globus_groups)

        # For any other groups we can't identify, get info from deriva
        deriva_groups = get_deriva_groups(config["deriva_server"])
        deriva_groups_with_auth = {f"https://auth.globus.org/{k}": v
                                   for k, v in deriva_groups.items()}
        collection_acls["Principal Name"] = collection_acls["Principal Name"].replace(deriva_groups)
        sheets[f"{environment} collection"] = collection_acls

        # Deriva ACLs
        deriva_acls = get_deriva_acls(config["deriva_server"], deriva_token)
        for dcc_name, dcc_acl in deriva_acls.items():
            for role, groups in dcc_acl.items():
                deriva_acls[dcc_name][role] = ", ".join([x if x not in deriva_groups_with_auth
                                                         else deriva_groups_with_auth[x] for x in
                                                         deriva_acls[dcc_name][role]])
        deriva_df = pd.DataFrame.from_dict(deriva_acls, orient='index').reset_index()
        deriva_df.index.names = ['DCC']
        sheets[f"{environment} deriva"] = deriva_df

        with pd.ExcelWriter(args.output, engine='xlsxwriter') as xls_writer:
            for sheet_name, sheet_data in sheets.items():
                sheet_data.to_excel(xls_writer, sheet_name=sheet_name, index=False)


if __name__ == "__main__":
    main()
