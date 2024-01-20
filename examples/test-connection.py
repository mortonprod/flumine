import os
import csv
import logging
from flumine.controls.loggingcontrols import LoggingControl
from flumine.order.ordertype import OrderTypes
from betfairlightweight.filters import streaming_market_filter
from flumine import BaseStrategy, Flumine, clients
from flumine.markets.market import Market
from betfairlightweight.resources import MarketBook, MarketCatalogue
import betfairlightweight
from dotenv import load_dotenv

load_dotenv()

password = os.getenv("PASSWORD")
app_key = os.getenv("APP_KEY")

logger = logging.getLogger(__name__)

FIELDNAMES = [
    "bet_id",
    "strategy_name",
    "market_id",
    "selection_id",
    "trade_id",
    "date_time_placed",
    "price",
    "price_matched",
    "size",
    "size_matched",
    "profit",
    "side",
    "elapsed_seconds_executable",
    "order_status",
    "market_note",
    "trade_notes",
    "order_notes",
]


class LiveLoggingControl(LoggingControl):
    NAME = "BACKTEST_LOGGING_CONTROL"

    def __init__(self, *args, **kwargs):
        super(LiveLoggingControl, self).__init__(*args, **kwargs)
        self._setup()

    # Changed file path and checks if the file order_real.csv already exists, if it doens't then create it
    def _setup(self):
        if os.path.exists("order_real.csv"):
            logging.info("Results file exists")
        else:
            with open("order_real.csv", "w") as m:
                csv_writer = csv.DictWriter(m, delimiter=",", fieldnames=FIELDNAMES)
                csv_writer.writeheader()

    def _process_cleared_orders_meta(self, event):
        orders = event.event
        with open("order_real.csv", "a") as m:
            for order in orders:
                if order.order_type.ORDER_TYPE == OrderTypes.LIMIT:
                    size = order.order_type.size
                else:
                    size = order.order_type.liability
                if order.order_type.ORDER_TYPE == OrderTypes.MARKET_ON_CLOSE:
                    price = None
                else:
                    price = order.order_type.price
                try:
                    order_data = {
                        "bet_id": order.bet_id,
                        "strategy_name": order.trade.strategy,
                        "market_id": order.market_id,
                        "selection_id": order.selection_id,
                        "trade_id": order.trade.id,
                        "date_time_placed": order.responses.date_time_placed,
                        "price": price,
                        "price_matched": order.average_price_matched,
                        "size": size,
                        "size_matched": order.size_matched,
                        "profit": 0 if not order.cleared_order else order.cleared_order.profit,
                        "side": order.side,
                        "elapsed_seconds_executable": order.elapsed_seconds_executable,
                        "order_status": order.status.value,
                        "market_note": order.trade.market_notes,
                        "trade_notes": order.trade.notes_str,
                        "order_notes": order.notes_str,
                    }
                    csv_writer = csv.DictWriter(m, delimiter=",", fieldnames=FIELDNAMES)
                    csv_writer.writerow(order_data)
                except Exception as e:
                    logger.error(
                        "_process_cleared_orders_meta: %s" % e,
                        extra={"order": order, "error": e},
                    )

        logger.info("Orders updated", extra={"order_count": len(orders)})

    def _process_cleared_markets(self, event):
        cleared_markets = event.event
        for cleared_market in cleared_markets.orders:
            logger.info(
                "Cleared market",
                extra={
                    "market_id": cleared_market.market_id,
                    "bet_count": cleared_market.bet_count,
                    "profit": cleared_market.profit,
                    "commission": cleared_market.commission,
                },
            )

class TestConnectionStrategy(BaseStrategy):
    def start(self) -> None:
        print("starting strategy 'TestConnectionStrategy'")

    def check_market_book(self, market: Market, market_book: MarketBook) -> bool:
        if market_book.status != "CLOSED":
            # if market_book.market_definition.market_type == "OVER_UNDER_25":
            return True
        
    def process_market_book(self, market: Market, market_book: MarketBook) -> None:
        print(market.event_name)
        print(vars(market))
        print(market.market_catalogue.competition.name)
        print(vars(market_book))
        runner_id_to_name = {}
        for runner in market.market_book.market_definition.runners:
            print(vars(runner))
            print(f"{runner.selection_id} --- {runner.name}")
            runner_id_to_name[runner.selection_id] = runner.name
        for runner in market_book.runners:
            print(vars(runner))
            print(runner.sp)
            print(runner.ex)
            print(f"{runner_id_to_name[runner.selection_id]} --- {runner.last_price_traded}")

    def process_market_catalogue(self, market: Market, market_catalogue: MarketCatalogue) -> None:
        print("process market catalogue")
    def process_raw_data(self, clk: str, publish_time: int, datum: dict) -> None:
        print("process raw data")

strategy = TestConnectionStrategy(
    market_filter=streaming_market_filter(
        # market_ids=["32938374"],
        # event_ids=["32902034"],
        country_codes=["GB"],
        market_types=["MATCH_ODDS", 'OVER_UNDER_25'],
    ),
    max_order_exposure=1, # Max exposure per order
    max_selection_exposure=1, # Max exposure per selection
    max_live_trade_count=1, # Max live (with executable orders) trades per runner
    max_trade_count=1, # Max total number of trades per runner
)

trading = betfairlightweight.APIClient("mortonprod", password, app_key=app_key, certs='/home/mortonprod/certs/')
client = clients.BetfairClient(trading, paper_trade=True)
framework = Flumine(client=client)

framework.add_logging_control(
    LiveLoggingControl()
)   

# Run our strategy on the simulated market
framework.add_strategy(strategy)
framework.run()