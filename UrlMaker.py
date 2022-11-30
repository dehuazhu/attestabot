import itertools
from pdb import set_trace

class UrlMaker:

    def make_urls(self, firm_name):
        url_joins = ('', '-')
        TLDs     = ('ch','com')

        firm_name = firm_name.lower()
        name_as_list = firm_name.split(' ')
        if name_as_list[-1] in ('liquidazione','liquidation'):
            name_as_list = name_as_list[:-2] # remove 'in liquidazione', 'in liquidation', 'en liquidation'
        if name_as_list[-1] in ('sagl','sa','ag','gmbh','sàrl'):
            name_as_list = name_as_list[:-1]
        partial_names = (name_as_list[:number_of_words] for number_of_words in range(1, min(5,len(name_as_list)+1))) #partial firm names with up to 4 words
        if len(name_as_list)>4: #try also the full name if it has more then 4 words
            partial_names = itertools.chain(partial_names, (name_as_list,))
        for partial_name_list in partial_names:
            for join in url_joins:
                baseUrl = join.join(partial_name_list)
                for tld in TLDs:
                    url = f'http://{baseUrl}.{tld}'
                    yield url
                if len(partial_name_list)==1: break
