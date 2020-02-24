import json


def message_to_dict(message):
    """
    This converts a given protobuf message to a dictionary.
    In contrast to googles internal function google.protobuf.json_format.MessageToDict this also
    converts nested protobuf messages.
    :param message: Protobuf message instance.
    :return: dict
    """
    msg_dict = {}

    for descriptor in message.DESCRIPTOR.fields:
        key = descriptor.name
        value = getattr(message, descriptor.name)

        if descriptor.label == descriptor.LABEL_REPEATED:
            message_list = []

            for sub_message in value:
                message_list.append(_handle_field(descriptor, sub_message))

            msg_dict[key] = message_list
        else:
            msg_dict[key] = _handle_field(descriptor, value)

    return msg_dict


def _handle_field(descriptor, value):
    if descriptor.type == descriptor.TYPE_MESSAGE:
        return message_to_dict(value)
    else:
        return value


def message_to_json(message, indent=4):
    """
    Converts a given protobuf message to json string. Internally uses message_to_dict.
    :param message: Protobuf message instance.
    :param indent: Int which determines how many spaces are used for indenting the "levels" in the string.
    :return: string
    """
    msg_dict = message_to_dict(message)

    return json.dumps(msg_dict, indent=indent)
