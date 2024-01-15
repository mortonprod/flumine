# Check paper trade: order_package.client.paper_trade


import os
from flumine import FlumineSimulation
from flumine import clients
from flumine import BaseStrategy
from flumine.markets.market import Market
from flumine.order.trade import Trade
from flumine.order.order import LimitOrder
from flumine.order.ordertype import OrderTypes
from flumine.controls.loggingcontrols import LoggingControl
from betfairlightweight.resources import MarketBook
import csv
from pprint import pprint
import logging

logging.basicConfig(filename = 'sim.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

logger = logging.getLogger(__name__)

# Fields we want to log in our simulations
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
    "side"
]

# Log results from simulation into csv file named sim_hta_2.csv
# If the csv file doesn't exist then it is created, otherwise we append results to the csv file
class BacktestLoggingControl(LoggingControl):
    NAME = "BACKTEST_LOGGING_CONTROL"

    def __init__(self, *args, **kwargs):
        super(BacktestLoggingControl, self).__init__(*args, **kwargs)
        self._setup()

    def _setup(self):
        if os.path.exists("sim_result.csv"):
            logging.info("Results file exists")
        else:
            with open("sim_result.csv", "w") as m:
                csv_writer = csv.DictWriter(m, delimiter=",", fieldnames=FIELDNAMES)
                csv_writer.writeheader()

    def _process_cleared_orders_meta(self, event):
        orders = event.event
        with open("sim_result.csv", "a") as m:
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
                    # pprint(vars(order))
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
                        "profit": order.simulated.profit,
                        "side": order.side
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

class BackEvensStrategy(BaseStrategy):
    def start(self) -> None:
        print("starting strategy 'BackEvensStrategy'")

    def check_market_book(self, market: Market, market_book: MarketBook) -> bool:
        # process_market_book only executed if this returns True
        if market_book.status != "CLOSED":
            return True
        
    def process_market_book(self, market: Market, market_book: MarketBook) -> None:
        # pprint(vars(market.market_book))
        # pprint(vars(market.market_book.market_definition))
        runner_id_to_name = {}
        for runner in market.market_book.market_definition.runners:
            # pprint(vars(runner))
            # print(f"{runner.selection_id} --- {runner.name}")
            runner_id_to_name[runner.selection_id] = runner.name
        # pprint(runner_id_to_name)
        for runner in market_book.runners:
            if runner_id_to_name[runner.selection_id] == "Over 2.5 Goals":
                # print(runner.last_price_traded)
                if runner.last_price_traded >= 2.0:
                    # print("TRADE")
                    trade = Trade(
                        market_id=market_book.market_id,
                        selection_id=runner.selection_id,
                        handicap=runner.handicap,
                        strategy=self,
                    )
                    order = trade.create_order(
                        side="BACK", order_type=LimitOrder(price=runner.last_price_traded, size=1)
                    )
                    market.place_order(order)

    # def process_closed_market(self, market: Market, market_book: MarketBook):
        # print("Process closed")

# Searches for all betfair data files within the folder sample_monthly_data_output
data_folder = "../../../data/betfair/soccer/BASIC/2022/Jan/1/31144803"
# data_folder = '../../../data/betfair/soccer/BASIC/2022/'
data_files = os.listdir(data_folder,)
data_files = [f'{data_folder}/{path}' for path in data_files]

client = clients.SimulatedClient(simulated_full_match=True)
framework = FlumineSimulation(client=client)

strategy = BackEvensStrategy(
    # market_filter selects what portion of the historic data we simulate our strategy on
    # markets selects the list of betfair historic data files
    # market_types specifies the type of markets
    # listener_kwargs specifies the time period we simulate for each market
    market_filter= {
        "markets": data_files,  
        'market_types':['OVER_UNDER_25'],
        "listener_kwargs": {"inplay": True},  
    },
    max_order_exposure=1,
    max_selection_exposure=1,
    max_live_trade_count=1,
    max_trade_count=1,
)

framework.add_logging_control(
    BacktestLoggingControl()
)

# Run our strategy on the simulated market
framework.add_strategy(strategy)
framework.run()