import boto3
from abc import abstractmethod


class Alert:
    def __init__(self, config):
        alert_config = AlertConfig(config)
        if alert_config.mode() == 'cloudwatch':
            self.__alert_client = CloudWatchAlertClient()
        else:
            self.__alert_client = PrintAlertClient()

    def send_heartbeat(self, app_name):
        self.__alert_client.send_heartbeat(app_name)


class AlertClient:
    @abstractmethod
    def send_heartbeat(self, app_name):
        pass


class CloudWatchAlertClient(AlertClient):
    def __init__(self):
        self.__cloudwatch = boto3.client('cloudwatch')

    def send_heartbeat(self, app_name):
        self.__cloudwatch.put_metric_data(
            MetricData=[
                {
                    'MetricName': 'heartbeat',
                    'Dimensions': [
                        {
                            'Name': 'app_name',
                            'Value': app_name
                        },
                    ],
                    'Unit': 'None',
                    'Value': 1.0
                },
            ],
            Namespace='APP/HEARTBEAT'
        )


class PrintAlertClient(AlertClient):

    def send_heartbeat(self, app_name):
        print("heartbeat received for app: {}".format(app_name))


class AlertConfig:
    def __init__(self, config_dict):
        alert_config = config_dict['alert_config']
        self.__alert_mode = alert_config['mode']

    def mode(self):
        return self.__alert_mode
