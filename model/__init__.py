def job(name, image, command):
    return {
        'apiVersion': 'batch/v1',
        'kind': 'job',
        'metadata': {
            'name': name
        },
        'spec': {
            "template": {
                "metadata": name,
                "spec": {
                    "containers": [
                        {
                            "name": name,
                            "image": image,
                            "command": command
                        }
                    ]
                }
            }
        }
    }
