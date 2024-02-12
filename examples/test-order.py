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
from flumine.order.trade import Trade
from flumine.order.order import LimitOrder, OrderStatus
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

    # Changed file path and checks if the file order_paper.csv already exists, if it doens't then create it
    def _setup(self):
        if os.path.exists("order_paper.csv"):
            logging.info("Results file exists")
        else:
            with open("order_paper.csv", "w") as m:
                csv_writer = csv.DictWriter(m, delimiter=",", fieldnames=FIELDNAMES)
                csv_writer.writeheader()

    def _process_cleared_orders_meta(self, event):
        orders = event.event
        with open("order_paper.csv", "a") as m:
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
    is_second_half = {}
    def start(self) -> None:
        print("starting strategy 'TestConnectionStrategy'")

    def check_market_book(self, market: Market, market_book: MarketBook) -> bool:
        print(f"Check market book {market.event_name} {market.market_type}")
        if market.event_name is not None:
            self.is_second_half[market.event_name] = False
            if market.market_type == "FIRST_HALF_GOALS_65" and market_book.status == "CLOSED":
                self.is_second_half[market.event_name] = True
        if market_book.status != "CLOSED":
            return True
        
    def process_market_book(self, market: Market, market_book: MarketBook) -> None:
        print(self.is_second_half)
        print(f"Process {market.event_name} {market.market_type}")
        print(f"Start {market.market_start_datetime.day}/{market.market_start_datetime.month}/{market.market_start_datetime.year} {market.market_start_datetime.hour}:{market.market_start_datetime.minute}")
        print(f"Now {market_book.publish_time.day}/{market_book.publish_time.month}/{market_book.publish_time.year} {market_book.publish_time.hour}:{market_book.publish_time.minute}")
        time_diff = market_book.publish_time - market.market_start_datetime
        if market_book.publish_time > market.market_start_datetime:
            print(f"Minutes of Match completed including the half time break {time_diff.seconds/60.0}")
        else: 
            print("Game has not started yet")
        runner_id_to_name = {}
        if market.market_catalogue is not None:
            for runner in market.market_catalogue.runners:
                print(f"Set {runner.selection_id} => {runner.runner_name}")
                runner_id_to_name[runner.selection_id] = runner.runner_name
            for runner in market_book.runners:
                print(f"{runner_id_to_name[runner.selection_id]} --- {runner.last_price_traded}")
                if runner.status == "ACTIVE" and market.market_type == "MATCH_ODDS" and runner.last_price_traded < 1.30:
                    print("Create order.")
                    trade = Trade(
                        market_id=market_book.market_id,
                        selection_id=runner.selection_id,
                        handicap=runner.handicap,
                        strategy=self
                    )
                    order = trade.create_order(
                        side="BACK",
                        order_type=LimitOrder(price=(runner.last_price_traded - 0.01), size=1.00)
                    )
                    market.place_order(order)
                

    def process_orders(self, market: Market, orders: list) -> None:
        print("Process orders")
        for order in orders:
            print(f"Order id {order.id} status {order.status} size remaining {order.size_remaining} persistence type {order.order_type.persistence_type}")


    def process_market_catalogue(self, market: Market, market_catalogue: MarketCatalogue) -> None:
        print("process market catalogue")
    def process_raw_data(self, clk: str, publish_time: int, datum: dict) -> None:
        print("process raw data")

strategy = TestConnectionStrategy(
    market_filter=streaming_market_filter(
        # market_ids=["32938374"],
        event_ids=["32885271"],
        # country_codes=["GB"],
        # Use the halftime market to determine when half time is
        market_types=["MATCH_ODDS", 'OVER_UNDER_65', 'FIRST_HALF_GOALS_65'],
    ),
    max_order_exposure=1, # Max exposure per order
    max_selection_exposure=1, # Max exposure per selection. A selection would be like loss in (win,draw,loss), i.e the MATCH_ODDS market. Selection is the same as a runner.
    max_live_trade_count=1, # Max live (with executable orders) trades per runner
    max_trade_count=1, # Max total number of trades per runner
)

trading = betfairlightweight.APIClient("mortonprod", password, app_key=app_key, certs='/home/mortonprod/certs/')
client = clients.BetfairClient(trading, paper_trade=False)
framework = Flumine(client=client)

framework.add_logging_control(
    LiveLoggingControl()
)   

# Run our strategy on the simulated market
framework.add_strategy(strategy)
framework.run()