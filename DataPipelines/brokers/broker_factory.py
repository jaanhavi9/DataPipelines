import logging
from DataPipelines.brokers.zerodha_broker import ZerodhaBroker
from DataPipelines.brokers.upstox_broker import UpstoxBroker
from DataPipelines.brokers.dhan_broker import DhanBroker
from config import SUPPORTED_BROKERS


class BrokerFactory:
    """A factory class to create broker instances based on the broker name."""

    @staticmethod
    def get_broker(broker_name: str, account_name: str, logger: logging.Logger):
        broker_name = broker_name.title()

        if broker_name not in SUPPORTED_BROKERS:
            logger.error(f"Broker '{broker_name}' is not supported.")
            raise ValueError(f"Broker '{broker_name}' is not supported.")

        if broker_name == 'Zerodha':

            return ZerodhaBroker(account_name, logger)
        elif broker_name == 'Upstox':
            return UpstoxBroker(account_name, logger)
        elif broker_name == 'Dhan':
            return DhanBroker(account_name, logger)
        else:
            logger.error(f"No implementation for broker '{broker_name}'.")
            raise NotImplementedError(f"No implementation for broker '{broker_name}'.")