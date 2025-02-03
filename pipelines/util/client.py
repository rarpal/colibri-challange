import requests

def get_rest_api(url):
    resp = None
    nexturl = url
    restlist = []

    while nexturl:
        # Make API request, get response object back, create dataframe from above schema.
        try:
            resp = requests.get(nexturl)
        except Exception as e:
            return e

        if resp != None and resp.status_code == 200:
            #return json.loads(res.text)
            rest = resp.json()
            nexturl = rest['info']['next']
            restlist.extend(rest['results'])
        else:
            return None

    return restlist