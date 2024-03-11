from undonesting import UndoNest
import pandas as pd
import requests

class TheOddsAPIHelper:
    __API_KEY = "fc35e20b0a8d983f77fc2fa9257a35c7"
    __SPORTS_URL = f"https://api.the-odds-api.com/v4/sports?apiKey={__API_KEY}"
    __ODDS_URL = "https://api.the-odds-api.com/v4/sports/{key}/odds/?apiKey={api_key}&regions=us&markets=h2h,spreads&oddsFormat=american"
    __SPORTS_TO_TRY = [
        "American Football", 
        "Baseball", 
        "Basketball", 
        "Boxing", "Golf", 
        "Ice Hockey", 
        "Soccer", 
        "Mixed Martial Arts", 
        "UFC"
    ]
    __KEYS_TO_TRY = [
        "americanfootball_ncaaf",
        "baseball_mlb_preseason",
        "baseball_mlb_world_series_winner",
        "baseball_ncaa",
        "basketball_nba",
        "basketball_nba_championship_winner",
        "basketball_ncaab",
        "basketball_ncaab_championship_winner",
        "boxing_boxing",
        "mma_mixed_martial_arts",
        "golf_masters_tournament_winner",
        "golf_pga_championship_winner",
        "golf_the_open_championship_winner",
        "golf_us_open_winner",
        "icehockey_nhl",
        "icehockey_nhl_championship_winner",
    ]

    @staticmethod
    def _process_raw_json(raw_json):
        json_list = []
        for json in raw_json:
            try:
                json_list.append(UndoNest(json).get_finalized_table())
            except:
                continue
        return pd.concat(json_list)

    def __init__(self):
        self.requests_left = None
        self.requests_made = None
        self.odds_table = None
        

    def _get_raw_odds_table(self, keys: list[str]):
        for key in keys:
            url = TheOddsAPIHelper.__ODDS_URL.format(key=key, api_key=TheOddsAPIHelper.__API_KEY)
            response = requests.get(url)
            self.requests_left = response.headers["X-Requests-Remaining"]
            self.requests_made = response.headers["X-Requests-Used"]
            yield response.json()

    def get_odds_table(self, keys=__KEYS_TO_TRY):
        if self.odds_table:
            return self.odds_table
        self.odds_table = TheOddsAPIHelper._process_raw_json(self._get_raw_odds_table(keys))
        return self.odds_table