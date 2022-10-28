class UrlMaker:

    def make_urls(self, firm_name):
        url_joins = ('', '-')
        TLDs     = ('ch','com')

        possible_urls = []
        firm_name = firm_name.lower()
        name_as_list = firm_name.split(' ')
        if name_as_list[-1] in ('liquidazione','liquidation'):
            name_as_list = name_as_list[:-2] # remove 'in liquidazione', 'in liquidation', 'en liquidation'
        if name_as_list[-1] in ('sagl','sa','ag','gmbh','sàrl'):
            name_as_list = name_as_list[:-1]
        for join in url_joins:
            baseUrl = join.join(name_as_list)
            for tld in TLDs:
                url = f'http://{baseUrl}.{tld}'
                possible_urls.append(url)
            if len(name_as_list)==1: break
        return possible_urls
