from abc import ABC, abstractmethod


class BaseAuthenticator(ABC):

    @abstractmethod
    def authenticate():
        """Generate access_token for the any broker"""
        pass