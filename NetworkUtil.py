class NetworkUtil(object):

    @staticmethod
    def get_public_ip():

        import requests

        from Validator import Validator

        IP_URL = 'http://checkip.amazonaws.com/'

        try:
            r = requests.get(IP_URL).text
            if not Validator.is_valid_ip_address(r):
                return None

            return r.strip()
        except:
            return None
