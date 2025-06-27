import json


def warning_log(message):
    data = {"severity": "warning", "message": message}
    print(json.dumps(data))


def info_log(message):
    data = {"severity": "info", "message": message}
    print(json.dumps(data))


def error_log(message):
    data = {"severity": "error", "message": message}
    print(json.dumps(data))


def print_log(severity, message):
    data = dict(
        severity=severity,
        message=message,
    )

    print(json.dumps(data))
