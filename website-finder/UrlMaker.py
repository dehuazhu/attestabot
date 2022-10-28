class UrlMaker:

    def make_urls(self, firmName):
        urlJoins = ('', '-')
        TLDs     = ('ch','com')

        possibleUrls = []
        firmName = firmName.lower()
        nameList = firmName.split(' ')
        if nameList[-1] in ('liquidazione','liquidation'):
            nameList = nameList[:-2] # remove 'in liquidazione', 'in liquidation', 'en liquidation'
        if nameList[-1] in ('sagl','sa','ag','gmbh','sàrl'):
            nameList = nameList[:-1]
        for join in urlJoins:
            baseUrl = join.join(nameList)
            for tld in TLDs:
                url = f'http://{baseUrl}.{tld}'
                possibleUrls.append(url)
            if len(nameList)==1: break
        return possibleUrls
