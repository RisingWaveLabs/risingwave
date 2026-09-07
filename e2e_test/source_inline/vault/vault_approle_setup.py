#!/usr/bin/env python3

import argparse
import json
import os
import sys
import requests
import psycopg2


def create_vault_approle(vault_addr, vault_token, role_name="test-role"):
    """Create or retrieve Vault AppRole credentials"""
    print(f"Setting up Vault AppRole: {role_name}")

    headers = {
        "X-Vault-Token": vault_token,
        "Content-Type": "application/json"
    }

    # Check if AppRole auth method is enabled
    try:
        auth_methods_resp = requests.get(f"{vault_addr}/v1/sys/auth", headers=headers)
        auth_methods_resp.raise_for_status()
        auth_methods = auth_methods_resp.json()

        if "approle/" not in auth_methods:
            print("Enabling AppRole authentication...")
            enable_resp = requests.post(
                f"{vault_addr}/v1/sys/auth/approle",
                headers=headers,
                json={"type": "approle"}
            )
            enable_resp.raise_for_status()
        else:
            print("AppRole authentication already enabled")
    except requests.exceptions.RequestException as e:
        print(f"Failed to check/enable AppRole auth: {e}")
        sys.exit(1)

    # Create a policy for secret access
    try:
        policy_data = {
            "policy": 'path "secret/data/*" {\n  capabilities = ["read"]\n}'
        }
        policy_resp = requests.put(
            f"{vault_addr}/v1/sys/policies/acl/test-policy",
            headers=headers,
            json=policy_data
        )
        policy_resp.raise_for_status()
        print("Created/updated test policy")
    except requests.exceptions.RequestException as e:
        print(f"Failed to create policy: {e}")
        sys.exit(1)

    # Create or update the AppRole
    try:
        role_data = {
            "token_policies": ["test-policy"],
            "token_ttl": "1h",
            "token_max_ttl": "4h"
        }
        role_resp = requests.post(
            f"{vault_addr}/v1/auth/approle/role/{role_name}",
            headers=headers,
            json=role_data
        )
        role_resp.raise_for_status()
        print(f"Created/updated AppRole: {role_name}")
    except requests.exceptions.RequestException as e:
        print(f"Failed to create AppRole: {e}")
        sys.exit(1)

    # Get role ID
    try:
        role_id_resp = requests.get(
            f"{vault_addr}/v1/auth/approle/role/{role_name}/role-id",
            headers=headers
        )
        role_id_resp.raise_for_status()
        role_id = role_id_resp.json()["data"]["role_id"]
        print(f"Retrieved role ID: {role_id}")
    except requests.exceptions.RequestException as e:
        print(f"Failed to get role ID: {e}")
        sys.exit(1)

    # Generate secret ID
    try:
        secret_id_resp = requests.post(
            f"{vault_addr}/v1/auth/approle/role/{role_name}/secret-id",
            headers=headers
        )
        secret_id_resp.raise_for_status()
        secret_id = secret_id_resp.json()["data"]["secret_id"]
        print("Generated secret ID successfully")
    except requests.exceptions.RequestException as e:
        print(f"Failed to generate secret ID: {e}")
        sys.exit(1)

    return role_id, secret_id


def create_risingwave_secret(db_name, vault_addr, role_id, secret_id):
    """Create the approle_kafka_user secret in RisingWave"""
    print("Creating approle_kafka_user secret in RisingWave...")

    # Connect to RisingWave
    try:
        conn = psycopg2.connect(
            host=os.environ.get("RISEDEV_RW_FRONTEND_LISTEN_ADDRESS", "localhost"),
            port=os.environ.get("RISEDEV_RW_FRONTEND_PORT", "4566"),
            user='root',
            password='',
            database=db_name
        )
        cur = conn.cursor()
        print(f"Connected to RisingWave database: {db_name}")
    except Exception as e:
        print(f"Failed to connect to RisingWave: {e}")
        sys.exit(1)

    try:
        # Drop existing secret if it exists
        drop_secret_sql = "DROP SECRET IF EXISTS approle_kafka_user;"
        cur.execute(drop_secret_sql)
        conn.commit()
        print("Dropped existing approle_kafka_user secret if it existed")

        # Create the secret using AppRole authentication
        create_secret_sql = f"""
        CREATE SECRET approle_kafka_user
        WITH (
          backend = 'hashicorp_vault',
          addr = '{vault_addr}',
          path = 'secret/data/myapp/kafka',
          field = 'username',
          auth_method = 'approle',
          auth_role_id = '{role_id}',
          auth_secret_id = '{secret_id}'
        ) AS NULL;
        """

        cur.execute(create_secret_sql)
        conn.commit()
        print("Created approle_kafka_user secret successfully")

        # Verify the secret was created
        verify_sql = "SELECT name FROM rw_secrets WHERE name = 'approle_kafka_user';"
        cur.execute(verify_sql)
        result = cur.fetchone()

        if result:
            print(f"✅ Secret verification successful: {result[0]}")
        else:
            print("❌ Secret not found after creation")
            sys.exit(1)

    except Exception as e:
        print(f"Failed to create secret: {e}")
        sys.exit(1)
    finally:
        cur.close()
        conn.close()


def main():
    parser = argparse.ArgumentParser(description='Setup Vault AppRole and create RisingWave secret')
    parser.add_argument('--db-name', required=True, help='RisingWave database name')
    parser.add_argument('--vault-addr', default='http://vault-server:8200', help='Vault server address')
    parser.add_argument('--vault-token', default='root-token', help='Vault root token')
    parser.add_argument('--role-name', default='test-role', help='AppRole name')

    args = parser.parse_args()

    # Override with environment variables if available
    vault_addr = os.environ.get("VAULT_ADDR", args.vault_addr)
    vault_token = os.environ.get("VAULT_TOKEN", args.vault_token)

    print(f"Using Vault address: {vault_addr}")
    print(f"Using database: {args.db_name}")

    # Step 1: Create/setup Vault AppRole
    role_id, secret_id = create_vault_approle(vault_addr, vault_token, args.role_name)

    # Step 2: Create RisingWave secret using AppRole credentials
    create_risingwave_secret(args.db_name, vault_addr, role_id, secret_id)

    print("✅ Setup completed successfully!")
    print(f"Role ID: {role_id}")

    # Export environment variables for potential use by other tests
    os.environ["VAULT_TEST_ROLE_ID"] = role_id
    os.environ["VAULT_TEST_SECRET_ID"] = secret_id


if __name__ == "__main__":
    main()
