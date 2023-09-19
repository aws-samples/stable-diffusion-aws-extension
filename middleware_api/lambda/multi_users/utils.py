import logging

import boto3
from botocore.exceptions import ClientError

from multi_users._types import PARTITION_KEYS, User


class KeyEncryptService:

    def __init__(self, logging_level=logging.INFO):
        self.kms_client = boto3.client('kms')
        self.logger = logging.getLogger('boto3')
        self.logger.setLevel(logging_level)

    def encrypt(self, key_id: str, text: str) -> bytes:
        """
        Encrypts text by using the specified key.

        :param key_id: The ARN or ID of the key to use for encryption.
        :param text: The text need to be encrypted
        :return: The encrypted version of the text.
        """
        try:
            cipher_text = self.kms_client.encrypt(
                KeyId=key_id, Plaintext=text.encode())['CiphertextBlob']
        except ClientError as err:
            self.logger.error(
                "Couldn't encrypt text. Here's why: %s", err.response['Error']['Message'])
        else:
            self.logger.debug(f"Your ciphertext is: {cipher_text}")
            return cipher_text

    def decrypt(self, key_id: str, cipher_text: bytes) -> bytes:
        """
        Decrypts text previously encrypted with a key.

        :param key_id: The ARN or ID of the key used to decrypt the data.
        :param cipher_text: The encrypted text to decrypt.
        """
        try:
            text = self.kms_client.decrypt(KeyId=key_id, CiphertextBlob=cipher_text)['Plaintext']
        except ClientError as err:
            self.logger.error("Couldn't decrypt your ciphertext. Here's why: %s",
                              err.response['Error']['Message'])

        else:
            self.logger.debug(f"Your plaintext is {text.decode()}")
            return text


def check_user_existence(ddb_service, user_table, username):
    creator = ddb_service.query_items(table=user_table, key_values={
        'kind': PARTITION_KEYS.user,
        'sort_key': username,
    })

    return not creator or len(creator) == 0


def get_user_roles(ddb_service, user_table_name, username):
    user = ddb_service.query_items(table=user_table_name, key_values={
        'kind': PARTITION_KEYS.user,
        'sort_key': username,
    })
    if not user or len(user) == 0:
        raise Exception(f'user: "{username}" not exist')

    user = User(**ddb_service.deserialize(user[0]))
    return user.roles


def check_user_permissions(checkpoint_owners: [str], user_roles: [str], user_name: str) -> bool:
    if not checkpoint_owners or user_name in checkpoint_owners or '*' in checkpoint_owners:
        return True

    for user_role in user_roles:
        if user_role in checkpoint_owners:
            return True

    return False
