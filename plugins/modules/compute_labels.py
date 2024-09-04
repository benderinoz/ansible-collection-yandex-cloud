from __future__ import absolute_import, division, print_function

__metaclass__ = type

DOCUMENTATION = """
  module: compute_snapshot
  version_added: 0.1.0
  author:
    - Maxim Zalysin <zalysin.m@gmail.com>
    - Pavel Demidov <aztecmmm@gmail.com>
  short_description: Manage Yandex Cloud compute labels.
  description:
    - Module for managing compute instance labels.
  requirements:
    - python >= 3.10
    - yandexcloud >= 0.250.0
  notes:
    - 'API Reference: U(https://cloud.yandex.ru/en/docs/compute/api-ref/grpc/instance_service)'.
    - The I(iam_token), I(service_account_key) and I(token) options are mutually exclusive.  
  options:
    state:
      type: str
      default: present
      choices:
        - present
        - absent
      description:
        - State of instance labels.
    folder_id:
      type: str
      required: true
      description:
        - Required. ID of the folder to create a snapshot in.
          The maximum string length in characters is 50.
    name:
      type: str
      required: true
      description:
        - Name of the instance.
          Value must match the regular expression C([a-z]([-a-z0-9]{0,61}[a-z0-9])?).
    labels:
      type: dict
      description:
        - Resource labels as key:value pairs. No more than 64 per resource. The maximum string length in characters for each value is 63.
        - The string length in characters for each key must be 1-63.
          Each key must match the regular expression C([a-z][-_./\\@0-9a-z]*). 
        - The maximum string length in characters for each value is 63.
          Each value must match the regular expression C([-_./\\@0-9a-z]*).
        - If 'state: absent' then specifying the value is not necessary
    iam_token:
      type: str
      description:
        - An IAM token is a unique sequence of characters issued to a user after authentication.
        - The following regular expression describes a token: C(t1\.[A-Z0-9a-z_-]+[=]{0,2}\.[A-Z0-9a-z_-]{86}[=]{0,2})
    service_account_key:
      type: dict
      description:
        - A Service Account Key.
      suboptions:
        id:
          type: str
          description:
            - Key object ID
            - "id" field from service account key JSON
        service_account_id:
          type: str
          description:
            - Service account ID
            - "service_account_id" field from service account key JSON
        private_key:
          type: str
          description:
            - Private key
            - "private_key" field from service account key JSON
    token:
      type: str
      description:
        - An OAuth Token.
"""

EXAMPLES = """
- yandex.cloud.compute_labels:
   iam_token: t1.abcdefghij-123456789...
   folder_id: abcdefghijk123456789
   name: Create labels
   labels:
     newlabel: newvalue
     anotherlabel: anothervalue
 register: compute_snapshot
"""

RETURN = """
instance:
  returned: success
  type: complex
  description:
    - Dictionary with instance labels.
  contains:
    instance_labels:
      returned: success
      type: dict
      description:
        - Resource labels as key:value pairs. Maximum of 64 per resource.
"""

from time import sleep

from google.protobuf.field_mask_pb2 import FieldMask

from yandex.cloud.operation.operation_service_pb2_grpc import OperationServiceStub
from yandex.cloud.operation.operation_service_pb2 import GetOperationRequest

from yandex.cloud.compute.v1.instance_service_pb2_grpc import InstanceServiceStub
from yandex.cloud.compute.v1.instance_service_pb2 import (
    GetInstanceRequest,
    UpdateInstanceRequest,
    ListInstancesRequest,
)

from ansible.module_utils.basic import AnsibleModule
from ansible.module_utils.common.text.converters import to_native
from ansible.module_utils.common.yaml import *

from ..module_utils.common import OperationError, message_to_dict
from ..module_utils.sdk import client


def _find(module, service):
    instance = dict()

    folder_id = module.params["folder_id"]
    filter = "name = %r" % module.params["name"]
    request = ListInstancesRequest(
        folder_id=folder_id, filter=filter, order_by="name asc"
    )

    try:
        response = message_to_dict(service.List(request))
        if len(response["instances"]) == 1:
            instance = response["instances"][0]

    except Exception as e:
        module.fail_json(
            msg="unknown error find instance %r by filter %r. Error was: %s"
            % (to_native(folder_id), to_native(filter), to_native(e))
        )

    return instance


def _get(module, service, instance_id):
    instance = dict()

    request = GetInstanceRequest(instance_id=instance_id, view="FULL")

    try:
        instance = message_to_dict(service.Get(request))

    except Exception as e:
        module.fail_json(
            msg="unknown error getting instance by id %r. Error was: %s"
            % (to_native(instance_id), to_native(e))
        )

    return instance


def _update(module, service, instance_id, instance_labels):

    for key in module.params["labels"]:
        if module.params["labels"][key] == None:
            module.fail_json(
                msg=f"Value for key '{to_native(key)}' cannot be 'None'",
            )

    for key, value in module.params["labels"].items():
        instance_labels[key] = value

    request = UpdateInstanceRequest(
        instance_id=instance_id,
        update_mask=FieldMask(paths=["labels"]),
        labels=instance_labels,
    )

    try:
        operation = message_to_dict(service.Update(request))

    except Exception as e:
        module.fail_json(
            msg=f"unknown error update labels %r. Error was: %s"
            % (to_native(module.params["name"]), to_native(e)),
        )

    return operation


def _delete(module, service, instance_id, instance_labels):

    for key in module.params["labels"]:
        if key in instance_labels:
            del instance_labels[key]

    request = UpdateInstanceRequest(
        instance_id=instance_id,
        update_mask=FieldMask(paths=["labels"]),
        labels=instance_labels,
    )

    try:
        operation = message_to_dict(service.Update(request))

    except Exception as e:
        module.fail_json(
            msg=f"unknown error update labels %r. Error was: %s"
            % (to_native(module.params["name"]), to_native(e)),
        )

    return operation


def _wait(module, service, operation_id):
    request = GetOperationRequest(operation_id=operation_id)

    try:
        while True:
            sleep(1)
            operation = message_to_dict(service.Get(request))
            if "done" in operation and operation["done"]:
                if "error" in operation:
                    raise OperationError(
                        "unknown operation error code %r. Error was: %s"
                        % (
                            to_native(operation["error"]["code"]),
                            to_native(operation["error"]["message"]),
                        )
                    )
                break
    except Exception as e:
        module.fail_json(
            msg="unknown error operation snapshot %r. Error was: %s"
            % (to_native(module.params["name"]), to_native(e)),
        )


def main():
    module = AnsibleModule(
        argument_spec=dict(
            state=dict(type="str", default="present", choices=["present", "absent"]),
            folder_id=dict(type="str", required=True),
            name=dict(type="str", required=True),
            labels=dict(type="dict", required=True),
            iam_token=dict(type="str", no_log=True),
            service_account_key=dict(type="dict", no_log=True),
            token=dict(type="str", no_log=True),
        ),
        required_one_of=[("iam_token", "service_account_key", "token")],
        mutually_exclusive=[("iam_token", "service_account_key", "token")],
        supports_check_mode=False,
    )

    instance_service = client(
        service=InstanceServiceStub,
        iam_token=module.params["iam_token"],
        token=module.params["token"],
        service_account_key=module.params["service_account_key"],
    )

    operation_service = client(
        service=OperationServiceStub,
        iam_token=module.params["iam_token"],
        token=module.params["token"],
        service_account_key=module.params["service_account_key"],
    )

    instance = _find(module, instance_service)

    match module.params["state"]:
        case "present":
            if instance == {}:
                module.exit_json(
                    changed=False,
                    msg="instance %r not exist" % (to_native(module.params["name"])),
                )

            elif all(
                key in instance["labels"] and instance["labels"][key] == value
                for key, value in module.params["labels"].items()
            ):
                module.exit_json(
                    changed=False,
                    msg="the specified labels for instance %r already exist"
                    % (to_native(module.params["name"])),
                    instance_labels=instance["labels"],
                )

            else:
                operation = _update(
                    module, instance_service, instance["id"], instance["labels"]
                )
                _wait(module, operation_service, operation["id"])
                get_instance = _get(
                    module, instance_service, operation["metadata"]["instance_id"]
                )

                module.exit_json(
                    changed=True,
                    msg="instance %r labels updated"
                    % (to_native(module.params["name"])),
                    instance_labels=get_instance["labels"],
                )

        case "absent":
            if instance == {}:
                module.exit_json(
                    changed=False,
                    msg="instance %r not exist" % (to_native(module.params["name"])),
                )

            elif any(key in instance["labels"] for key in module.params["labels"]):
                operation = _delete(
                    module, instance_service, instance["id"], instance["labels"]
                )
                _wait(module, operation_service, operation["id"])
                get_instance = _get(
                    module, instance_service, operation["metadata"]["instance_id"]
                )

                module.exit_json(
                    changed=True,
                    msg="instance %r labels updated"
                    % (to_native(module.params["name"])),
                    instance_labels=get_instance["labels"],
                )

            else:
                module.exit_json(
                    changed=False,
                    msg="the specified labels for instance %r already absent"
                    % (to_native(module.params["name"])),
                    instance_labels=instance["labels"],
                )


if __name__ == "__main__":
    main()
