import os
import betfairlightweight
from betfairlightweight import filters
from dotenv import load_dotenv

load_dotenv()

password = os.getenv("PASSWORD")
app_key = os.getenv("APP_KEY")

def events():

    trading = betfairlightweight.APIClient("mortonprod", password, app_key=app_key, certs='/home/mortonprod/certs/')

    trading.login()

    comps = trading.betting.list_competitions(
        filter=filters.market_filter(text_query="Soccer")
    )

    print(f"Number of competitions {len(comps)}")
    for comp in comps:
        print(f"{comp.competition.name} --- {comp.competition.id}")

    # English Premier League --- 10932509
        
    eventResults = trading.betting.list_events(
        filter=filters.market_filter(competition_ids=["10932509"])
    )

    event_ids = []
    for eventResult in eventResults:
        print(f"{eventResult.event.name} --- {eventResult.event.id}")
        event_ids.append(eventResult.event.id)

    trading.logout()
    return event_ids