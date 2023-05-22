from .utils_api import EXCHANGE_TO_BASE_URL, make_get_request


class DlClient:

    def __init__(self):
        self.base_url = EXCHANGE_TO_BASE_URL["DEFILLAMA"]

    def get_protocol_tvl(self, protocol):
        return make_get_request(self.base_url, f"protocol/{protocol.lower()}")

    def get_yields(self):
        return make_get_request('https://yields.llama.fi', 'pools')
