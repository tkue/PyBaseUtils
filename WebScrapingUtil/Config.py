
from WebScrapingUtil.WebScrapingTypes import WebDriverType
from ConfigUtil import BasicConfig
from StringUtil import StringUtil


class WebScrapingConfig(BasicConfig):

    def __init__(self, config_path: str):
        super(WebScrapingConfig, self).__init__(config_path=config_path)

    def get_url_by_name(self, name: str):
        if not name:
            return

        name = name.strip().lower()
        for url in self.config['sites']:
            if url['name'].strip().lower() == name:
                return url['url']

    def get_urls(self):
        urls = []

        for url in self.config['sites']:
            urls.append(url['url'])

    def get_driver_arguments(self, driver_type: WebDriverType):
        if driver_type.value == WebDriverType.CHROME.value:

            args = []
            for arg in self.config['selenium']['chrome_driver_arguments']:
                args.append(arg)
        else:
            raise NotImplementedError

        return args

    def get_driver_options(self, driver_type: WebDriverType):
        if driver_type.value == WebDriverType.CHROME.value:
            from selenium.webdriver.chrome.options import Options as ChromeOptions

            opts = ChromeOptions()

            for arg in self.get_driver_arguments(driver_type):
                opts.add_argument(arg.strip())

            return opts

        else:
            raise NotImplementedError

    def get_driver_path(self, driver_type: WebDriverType):
        if driver_type.value == WebDriverType.CHROME.value:
            path = self.config['selenium']['chrome_driver_path']
            return self.get_path(path)
        else:
            raise NotImplementedError

    def get_original_ip(self):
        """
        Original host IP address
        *** The IP address you want to mask ***
        :return:
        :rtype:
        """
        return self.config['connection']['original_public_ip']

    def get_is_need_mask_ip(self):
        is_need_mask_ip = self.config['connection']['is_need_mask_ip'].strip()
        return StringUtil.get_boolean_from_string(is_need_mask_ip)
