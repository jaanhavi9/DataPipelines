import DataPipelines.brokers.dhan_broker as dhan_broker
from fastapi import FastAPI, Query, HTTPException
from typing import Optional, List, Dict, Any
from DataPipelines.utils.logger import setup_logger


app = FastAPI()

logger = setup_logger("dhanlogger", "dhan_logger.log")

broker = dhan_broker.DhanBroker(account_name="ACC1", logger=logger)


@app.on_event("startup")
async def startup_event():
    """Initialize the broker when the app starts."""
    await broker.initialize()


@app.get("/data/live/master-scrip")
async def get_master_scrip(mode: str = Query("compact", description="Mode: 'compact' or 'detailed'")):
    """Get master scrip data as a dataframe."""
    try:
        data = await broker.master_scrip(mode=mode)
        return {"status": "success", "data": data}
    except Exception as e:
        logger.error(f"Error fetching master scrip: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    

@app.get("/data/live/trade-book")
async def get_trade_book():
    """Get trade book data."""
    try:
        data = await broker.get_trade_book()
        return {"status": "success", "data": data}
    except Exception as e:
        logger.error(f"Error fetching trade book: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/data/live/ohlc-quote/{exchange_token}")
async def get_ohlc_quote(exchange_token: str):
    """Get OHLC data for a given exchange token."""
    try:
        data = await broker.ohlc_quote(exchange_token=exchange_token)
        return {"status": "success", "data": data}
    except Exception as e:
        logger.error(f"Error fetching OHLC quote: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/data/live/ltp-quote/{exchange_token}")
async def get_ltp_quote(exchange_token: str):
    """Get the last traded price (LTP) for a given exchange token."""
    try:
        data = await broker.ltp_quote(exchange_token=exchange_token)
        return {"status": "success", "data": data}
    except Exception as e:
        logger.error(f"Error fetching LTP quote: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/data/live/funds-and-margin")
async def get_funds_and_margin(segment: Optional[str] = None):
    """Get funds and margin details."""
    try:
        data = await broker.get_funds_and_margin(segment=segment)
        return {"status": "success", "data": data}
    except Exception as e:
        logger.error(f"Error fetching funds and margin: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/data/live/full-market-quote/{exchange_token}")
async def get_full_market_quote(exchange_token: str):
    """Get full market depth for a given exchange token."""
    try:
        data = await broker.full_market_quote(exchange_token=exchange_token)
        return {"status": "success", "data": data}
    except Exception as e:
        logger.error(f"Error fetching full market quote: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    

@app.get("/data/live/orderbook")
async def get_orderbook():
    """Get the order book."""
    try:
        data = await broker.get_orderbook()
        return {"status": "success", "data": data}
    except Exception as e:
        logger.error(f"Error fetching order book: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/data/live/order-details/{order_id}")
async def get_order_details(order_id: str):
    """Get details of a specific order."""
    try:
        data = await broker.get_order_details(order_id=order_id)
        return {"status": "success", "data": data}
    except Exception as e:
        logger.error(f"Error fetching order details: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/data/live/calculate-margin/{exchange_token}/{transaction_type}/{quantity}/{product_type}/{price}/{trigger_price}")
async def calculate_margin(
    exchange_token: str,
    transaction_type: str,
    quantity: int,
    product_type: str,
    price: float,
    trigger_price: Optional[float] = None
):
    """Calculate margin for a given exchange token and instrument details."""
    try:

        instrument_dict = {}
        instrument_dict['transaction_type'] = transaction_type
        instrument_dict['quantity'] = quantity
        instrument_dict['product_type'] = product_type
        instrument_dict['price'] = price
        instrument_dict['trigger_price'] = trigger_price if trigger_price else None
  

        data = await broker.calculate_margin(
            exchange_token=exchange_token,
            instrument_dict=instrument_dict,
        )
        return {"status": "success", "data": data}
    except Exception as e:
        logger.error(f"Error calculating margin: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/data/live/option-chain/{exchange_token}")
async def get_option_chain(
    exchange_token: str,
    expiry_date: Optional[str] = None,
):
    """Get option chain data for a given exchange token."""
    try:
        data = await broker.option_chain(
            exchange_token=exchange_token,
            expiry_date=expiry_date,
        )
        return {"status": "success", "data": data}
    except Exception as e:
        logger.error(f"Error fetching option chain: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/data/live/expiry-dates/{exchange_token}")
async def get_expiry_dates(exchange_token: str):
    """Get expiry dates for a given exchange token."""
    try:
        data = await broker.expiry_dates(exchange_token=exchange_token)
        return {"status": "success", "data": data}
    except Exception as e:
        logger.error(f"Error fetching expiry dates: {e}")
        raise HTTPException(status_code=500, detail=str(e))

    

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)




